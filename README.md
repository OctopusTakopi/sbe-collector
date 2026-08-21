# sbe-collector

Records Binance spot over the SBE websocket (`stream-sbe.binance.com`). Same
process shape as the JSON `collector` crate: one exchange, a symbol list,
daily files, optional extra sockets, rolling replace.

This is not a drop-in for the collector in
[hftbacktest](https://github.com/nkaz001/hftbacktest/tree/master/collector).
That crate (and our JSON fork of it) stores `<recv_ns> <json>` text. SBE frames
are binary, so hftbacktest's Python convert path will not read these files.
Use `reader` or `gap_detector` from this repo, or write your own decoder.

You need `BINANCE_API_KEY` in the environment. The SBE endpoint requires it.

## vs hftbacktest / the JSON collector

| | hftbacktest collector | this crate |
|---|---|---|
| venue | Binance JSON, Bybit, Hyperliquid | Binance spot SBE only (`binancesbespot`) |
| file | `<symbol>_YYYYMMDD.gz` | `<symbol>_YYYYMMDD.zst` |
| payload | JSON lines | length-prefixed SBE (and REST snapshots) |
| sockets | one | `-c N`, duplicates dropped |
| replace | stop then start | Unix socket handover, see [ROLLING_UPDATE.md](ROLLING_UPDATE.md) |

The JSON `collector` crate is the closer sibling. Layout, `flock`, sidecars,
quality logs, and the handover protocol match. Only the wire format and the
exchange name differ.

`scripts/run_sbe.sh` starts a tmux session and will kill an existing one. Use
it for a cold start, not for an upgrade.

## Build and run

```sh
cargo build --release
export BINANCE_API_KEY=...
./target/release/sbe-collector -c 2 /data/sbe/binance/spot binancesbespot btcusdt ethusdt
```

Streams: trade, bestBidAsk, depth diffs, and depth20 snapshots, plus periodic
REST book snapshots mixed into the same file.

## File format

One zstd stream (sometimes several concatenated frames after a handover) of
records:

```
[i64 recv_ns LE][u8 tag][u32 payload_len LE][payload]
```

Tag `S` is a raw SBE frame from the stream. Tag `R` is a REST depth snapshot
as JSON. A crash mid-day can leave today's file without a zstd footer; that is
the tradeoff for keeping one file per symbol per UTC day.

During a rolling replace the new process may write
`<symbol>_<date>_<run-id>.zst` until it gets the daily-file lock, then it
appends that sidecar and deletes it.

## Checking files

```sh
cargo build --release --bin reader --bin gap_detector
./target/release/reader /data/sbe/binance/spot/btcusdt_20260821.zst
./target/release/gap_detector /data/sbe/binance/spot --min-gap 5
```

`reader` prints decoded trades / BBO / depth. On a file that is still open it
exits with `incomplete frame` after the last complete record; that is expected.
`gap_detector` says the same thing as an unterminated zstd stream.
