# sbe-collector

Records exchange market data over SBE websockets: Binance spot, OKX, and
Gate spot and USDT perpetual futures. Same process shape as the JSON
`collector` crate: one exchange per process and directory, a symbol list,
daily files, optional extra sockets, rolling replace.

This is not a drop-in for the collector in
[hftbacktest](https://github.com/nkaz001/hftbacktest/tree/master/collector).
That crate (and our JSON fork of it) stores `<recv_ns> <json>` text. SBE frames
are binary, so hftbacktest's Python convert path will not read these files.
Use `reader` or `gap_detector` from this repo, or write your own decoder.

## Exchanges

| exchange | symbols | credentials | channels |
|---|---|---|---|
| `binancesbespot` | `btcusdt` | `BINANCE_API_KEY` | trade, bestBidAsk, depth diffs, depth20; hourly REST depth snapshots |
| `okxsbe` | instIds: `BTC-USDT`, `BTC-USDT-SWAP`, `BTC-USD-261225` | `OKX_API_KEY`, `OKX_SECRET_KEY`, `OKX_PASSPHRASE` | `bbo-tbt`; hourly REST book snapshots (400 levels) |
| `gatesbespot` | `BTC_USDT` | none | `book_ticker`, `trades`, `obu` (50 levels, 20 ms) |
| `gatesbefutures` | `BTC_USDT` (USDT-settled) | none | `book_ticker`, `trades`, `obu` (50 levels, 20 ms) |

**OKX.** The login rides on the websocket upgrade, signed per attempt, so the
host clock must be within 30 s. Each instId is resolved at startup to the
`instIdCode` OKX's frames carry; an unknown one stops the process. OKX gives
`books-l2-tbt` and `trades` only to trading fee tier VIP4 and above (`64003`
otherwise), so only `bbo-tbt` is recorded. Each connection receives its own
sample of `bbo-tbt`, so `-c 2` records the union of two samples.

**Gate.** Public market data, no login. Pairs are checked against Gate's
listings at startup. `obu` sends a full snapshot when subscribed and deltas
after it. A connection whose own book stream skips an id replaces itself
make-before-break, starting from a fresh snapshot; every connection is also
replaced hourly, so each daily file holds a snapshot within the hour. Spot
and futures share template ids over different layouts: decoding needs to
know which.

```sh
cargo build --release
export BINANCE_API_KEY=...
./target/release/sbe-collector -c 2 /data/sbe/binance/spot binancesbespot btcusdt ethusdt
export OKX_API_KEY=... OKX_SECRET_KEY=... OKX_PASSPHRASE=...
./target/release/sbe-collector -c 2 /data/sbe/okx okxsbe BTC-USDT BTC-USDT-SWAP
./target/release/sbe-collector -c 2 /data/sbe/gate/spot gatesbespot BTC_USDT ETH_USDT
./target/release/sbe-collector -c 2 /data/sbe/gate/futures gatesbefutures BTC_USDT ETH_USDT
```

Missing credentials, an unknown symbol, or credentials the venue refuses (401
before any connection went live) end the process instead of retrying.

## vs hftbacktest / the JSON collector

| | hftbacktest collector | this crate |
|---|---|---|
| venue | Binance JSON, Bybit, Hyperliquid | Binance spot, OKX, Gate over SBE |
| file | `<symbol>_YYYYMMDD.gz` | `<symbol>_YYYYMMDD.zst` |
| payload | JSON lines | length-prefixed SBE (and REST snapshots) |
| sockets | one | `-c N`, duplicates dropped |
| replace | stop then start | Unix socket handover, see [ROLLING_UPDATE.md](ROLLING_UPDATE.md) |

The JSON `collector` crate is the closer sibling. Layout, `flock`, sidecars,
quality logs, and the handover protocol match. Only the wire format and the
exchanges differ.

`scripts/run_sbe.sh` starts one collector per exchange in a tmux session and
will kill an existing one. Use it for a cold start, not for an upgrade. `tmux
kill-session` delivers SIGHUP, which drains and exits like SIGTERM, unless
SIGHUP was already ignored when the collector started (`nohup`): then it stays
ignored.

## Redundant connections

With `-c N` every connection subscribes to the same channels and a frame is
kept once. Two connections' copies of one event differ only in a send time the
venue stamps per connection (Binance `eventTime`, OKX `outTime`, Gate `time`),
which the duplicate check leaves out. The duplicate rate it logs every five
minutes settles near (N - 1) / N where every connection gets every event: about
50% at `-c 2` for Binance and Gate futures. Where each connection gets its own
sample of the best bid/offer, as on OKX and Gate spot, it is lower (about 39%
on OKX, 45-50% on Gate spot), and the samples' merge shows as reordered ids in
`gap_detector`.

With one connection the filter is off for Binance and OKX: their timer-pushed
snapshots repeat unchanged while the book sits still, and dropping those would
be silent, while a session handover's overlap records a few frames twice.
Gate filters a single connection too: replacing its session every hour
overlaps two sockets each time, and none of its frames repeat unchanged.

## File format

One zstd stream (sometimes several concatenated frames after a handover) of
records:

```
[i64 recv_ns LE][u8 tag][u32 payload_len LE][payload]
```

Tag `S` is a raw SBE frame from the stream. Tag `R` is a REST book snapshot as
the venue returned it: JSON from Binance, SBE template 1006 from OKX. A crash
mid-day can leave today's file without a zstd footer; that is the tradeoff for
keeping one file per symbol per UTC day. Nothing is appended after such a
tail, since a decoder cannot step past it: the next writer moves the file
aside to `<symbol>_<date>_unterminated-<ns>.zst` and starts the day again in a
fresh daily file.

A record does not say which exchange it is from, and Gate's two markets send
identical headers over different layouts, so each output directory names its
exchange in an `_exchange` file. The collector writes it on first start and
refuses a directory marked for another exchange. A directory without one was
recorded before markers existed, by `binancesbespot`.

A cleanly sealed file ends with a clean-close trailer: a 24-byte zstd
skippable frame holding `COLLECTR` and the offset it starts at, so the next
writer can trust the file's tail without walking every block. `zstd -dc`,
libzstd readers, `reader` and `gap_detector` skip it.

During a rolling replace the new process may write
`<symbol>_<date>_<run-id>.zst` until it gets the daily-file lock, then it
appends that sidecar and deletes it.

### Replaying a book

A snapshot can land in a file after deltas newer than itself: an OKX REST
snapshot is written when its reply arrives, and a fresh Gate connection's
`full` snapshot can trail deltas another connection already delivered. Apply a
snapshot only when it is newer than the book, or to recover from a gap, and
find the delta that follows it by sequence id, not by its position in the
file.

## Checking files

```sh
cargo build --release --bin reader --bin gap_detector
./target/release/reader /data/sbe/binance/spot/btcusdt_20260821.zst
./target/release/gap_detector /data/sbe --min-gap 5
```

`reader` prints decoded records; `gap_detector` checks receive-time gaps and
each venue's id chains: depth and trade ids, best bid/offer update ids, Gate's
`obu` deltas. Both decode a file as the exchange its directory names;
`--exchange` overrides. On a file that is still open `reader` exits with
`incomplete frame` after the last complete record; that is expected.
`gap_detector` says the same thing as an unterminated zstd stream.
