#!/usr/bin/env python3
"""Check sbe-collector recordings against the venues' SBE XML schemas.

An independent decoder: every wire layout comes from the XML in `schemas/`,
nothing from the collector's Rust structs, so it catches a decoder whose
offsets drifted from what the venue publishes. For each recorded file it
checks:

* the framing of every record;
* every SBE payload against its schema: schema id and version, a template the
  schema defines, root and group block lengths, var data, a decode that ends
  exactly at the frame's end, enum values;
* venue semantics: the frame's instrument against its file, the venue's
  timestamps against receive time, best bid below best ask, sorted and
  uncrossed snapshot books, trade ids in order;
* with --reader, record by record, that the numbers the collector's own
  `reader` prints are the schema's.

Needs only Python 3 and the `zstd` command.

Usage: scripts/sbe_verify.py <recording dir>... [--reader target/release/reader]

Exits 1 if any check fails.
"""

import collections
import json
import re
import statistics
import struct
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

SCHEMAS = Path(__file__).resolve().parent.parent / "schemas"
SCHEMA_FILES = {
    "binancesbespot": "binance_stream_1_0.xml",
    "okxsbe": "okx_sbe_1_0.xml",
    "gatesbespot": "gate_spot_ws_v1.0.0.xml",
    "gatesbefutures": "gate_fex_ws_v1.0.0.xml",
}
PRIMITIVES = {
    "char": ("c", 1), "int8": ("b", 1), "uint8": ("B", 1), "int16": ("h", 2),
    "uint16": ("H", 2), "int32": ("i", 4), "uint32": ("I", 4), "int64": ("q", 8),
    "uint64": ("Q", 8), "float": ("f", 4), "double": ("d", 8),
}


def local(tag):
    return tag.rsplit("}", 1)[-1]


class Schema:
    """One messageSchema: its types and messages, sized from the XML alone."""

    def __init__(self, path):
        root = ET.parse(path).getroot()
        self.path = path
        self.id = int(root.get("id"))
        self.version = int(root.get("version", "0"))
        self.types = {}
        for types in (child for child in root if local(child.tag) == "types"):
            for node in types:
                self.types[node.get("name")] = node
        self.messages = {
            int(node.get("id")): node for node in root if local(node.tag) == "message"
        }

    def kind(self, type_name):
        """('prim', fmt, size, count) | ('enum', fmt, size, values) | ('composite', members, size)."""
        if type_name in PRIMITIVES:
            fmt, size = PRIMITIVES[type_name]
            return ("prim", fmt, size, 1)
        node = self.types[type_name]
        tag = local(node.tag)
        if tag == "type":
            fmt, size = PRIMITIVES[node.get("primitiveType")]
            if node.get("presence") == "constant":
                return ("prim", fmt, 0, 0)
            return ("prim", fmt, size, int(node.get("length", "1")))
        if tag in ("enum", "set"):
            _, fmt, size, _ = self.kind(node.get("encodingType"))
            values = {}
            for value in (v for v in node if local(v.tag) in ("validValue", "choice")):
                text = value.text.strip()
                values[int(text) if text.lstrip("-").isdigit() else text] = value.get("name")
            return ("enum", fmt, size, values)
        if tag == "composite":
            members, size = [], 0
            for member in node:
                if local(member.tag) == "type":
                    fmt, unit = PRIMITIVES[member.get("primitiveType")]
                    length = int(member.get("length", "1"))
                    members.append((member.get("name"), ("prim", fmt, unit, length)))
                    size += unit * length
                elif local(member.tag) == "ref":
                    sub = self.kind(member.get("type"))
                    members.append((member.get("name"), sub))
                    size += kind_size(sub)
            return ("composite", members, size)
        raise ValueError(f"unknown type node {tag}")

    def block_size(self, node):
        """Bytes of the fixed fields directly in a message or group."""
        return sum(self.field_size(f) for f in node if local(f.tag) == "field")

    def field_size(self, field):
        if field.get("presence") == "constant":
            return 0
        return kind_size(self.kind(field.get("type")))


def kind_size(kind):
    if kind[0] == "prim":
        return kind[2] * kind[3]
    return kind[2]


class Malformed(Exception):
    pass


def decode_kind(kind, buf, off):
    if kind[0] in ("prim", "enum"):
        fmt, size = kind[1], kind[2]
        count = kind[3] if kind[0] == "prim" else 1
        if count == 0:
            return None
        if off + size * count > len(buf):
            raise Malformed(f"field runs past the frame at {off}")
        if count > 1:
            return bytes(buf[off:off + size * count])
        return struct.unpack_from("<" + fmt, buf, off)[0]
    value, cursor = {}, off
    for name, sub in kind[1]:
        value[name] = decode_kind(sub, buf, cursor)
        cursor += kind_size(sub)
    return value


class Decoder:
    def __init__(self, schema, issues):
        self.s = schema
        self.issues = issues

    def fields(self, node, buf, off, block_len, where):
        values, cursor = {}, off
        for field in (f for f in node if local(f.tag) == "field"):
            name = field.get("name")
            if field.get("presence") == "constant":
                values[name] = field.get("valueRef")
                continue
            size = self.s.field_size(field)
            if cursor + size > off + block_len:
                raise Malformed(f"{where}.{name} lies past the block length {block_len}")
            kind = self.s.kind(field.get("type"))
            value = decode_kind(kind, buf, cursor)
            if kind[0] == "enum" and value not in kind[3]:
                self.issues[f"{where}.{name}: value {value} not a validValue"] += 1
            values[name] = value
            cursor += size
        return values

    def body(self, node, buf, off, block_len, where):
        """Fixed fields, then groups, then var data, as SBE lays them out."""
        values = self.fields(node, buf, off, block_len, where)
        expected = self.s.block_size(node)
        if block_len != expected:
            self.issues[f"{where}: blockLength {block_len}, schema says {expected}"] += 1
        cursor = off + block_len
        for child in node:
            tag = local(child.tag)
            if tag == "group":
                dim_kind = self.s.kind(child.get("dimensionType", "groupSizeEncoding"))
                dim = decode_kind(dim_kind, buf, cursor)
                cursor += kind_size(dim_kind)
                entries = []
                for _ in range(dim["numInGroup"]):
                    entry, cursor = self.body(
                        child, buf, cursor, dim["blockLength"], f"{where}.{child.get('name')}"
                    )
                    entries.append(entry)
                values[child.get("name")] = entries
            elif tag == "data":
                _, length_kind = self.s.kind(child.get("type"))[1][0]
                length = decode_kind(length_kind, buf, cursor)
                cursor += kind_size(length_kind)
                if cursor + length > len(buf):
                    raise Malformed(f"{where}.{child.get('name')} runs past the frame")
                raw = bytes(buf[cursor:cursor + length])
                try:
                    values[child.get("name")] = raw.decode("utf-8")
                except UnicodeDecodeError:
                    self.issues[f"{where}.{child.get('name')}: not UTF-8"] += 1
                    values[child.get("name")] = ""
                cursor += length
        return values, cursor

    def message(self, buf):
        if len(buf) < 8:
            raise Malformed(f"{len(buf)}-byte frame shorter than the header")
        block_len, template, schema_id, version = struct.unpack_from("<HHHH", buf, 0)
        if schema_id != self.s.id:
            self.issues[f"schemaId {schema_id}, {self.s.path.name} says {self.s.id}"] += 1
        if version != self.s.version:
            self.issues[f"template {template}: version {version}, {self.s.path.name} says {self.s.version}"] += 1
        node = self.s.messages.get(template)
        if node is None:
            raise Malformed(f"template {template} is not in {self.s.path.name}")
        name = node.get("name")
        values, end = self.body(node, buf, 8, block_len, name)
        if end != len(buf):
            self.issues[f"{name}: decoded {end} bytes of a {len(buf)}-byte frame"] += 1
        return template, name, values


def records(path):
    """`(recv_ns, tag, payload)` per record; tag None marks broken framing."""
    data = subprocess.run(["zstd", "-dcq", str(path)], capture_output=True, check=False).stdout
    off = 0
    while off + 13 <= len(data):
        recv_ns, tag, length = struct.unpack_from("<qBI", data, off)
        payload = data[off + 13:off + 13 + length]
        if len(payload) != length:
            yield recv_ns, None, payload
            return
        yield recv_ns, chr(tag), payload
        off += 13 + length
    if off != len(data):
        yield None, None, data[off:]


def percentile(values, q):
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))]


def describe(latencies_ms):
    return (f"median {statistics.median(latencies_ms):.1f} ms, p1 {percentile(latencies_ms, 0.01):.1f}, "
            f"p99 {percentile(latencies_ms, 0.99):.1f}, min {min(latencies_ms):.1f}, "
            f"max {max(latencies_ms):.1f}")


def ordered(levels, price, descending):
    prices = [level[price] for level in levels]
    return prices == sorted(prices, reverse=descending)


def crossed(bids, asks, price):
    return bool(bids) and bool(asks) and bids[0][price] >= asks[0][price]


def scale(mantissa, exponent):
    return mantissa * 10.0 ** exponent


def check_semantics(exchange, symbol, recv_ns, tag, name, v, latencies, bad, codes):
    recv_us = recv_ns / 1000
    if exchange == "binancesbespot":
        if v.get("symbol", "").lower() != symbol:
            bad[f"{name}: symbol {v.get('symbol')!r} in {symbol}'s file"] += 1
        latencies[f"{name}.eventTime"].append((recv_us - v["eventTime"]) / 1000)
        if name == "BestBidAskStreamEvent" and not v["bidPrice"] < v["askPrice"]:
            bad["best bid not below best ask"] += 1
        if name == "DepthSnapshotStreamEvent":
            if not ordered(v["bids"], "price", True) or not ordered(v["asks"], "price", False):
                bad["depth snapshot levels unsorted"] += 1
            if crossed(v["bids"], v["asks"], "price"):
                bad["depth snapshot crossed"] += 1
        if name == "DepthDiffStreamEvent" and v["firstBookUpdateId"] > v["lastBookUpdateId"]:
            bad["depth diff first id above last"] += 1
        if name == "TradesStreamEvent":
            ids = [trade["id"] for trade in v["trades"]]
            if ids != sorted(ids) or any(t["price"] <= 0 or t["qty"] <= 0 for t in v["trades"]):
                bad["trades out of order or not positive"] += 1
    elif exchange == "okxsbe":
        # The stream names only a code; one file holds one instrument's.
        codes.add(v["instIdCode"])
        latencies[f"{name}.tsUs"].append((recv_us - v["tsUs"]) / 1000)
        if "outTime" in v:
            latencies[f"{name}.outTime"].append((recv_us - v["outTime"]) / 1000)
        if name == "BboTbtChannelEvent":
            if not v["bidPxMantissa"] < v["askPxMantissa"]:
                bad["best bid not below best ask"] += 1
            if min(v["askOrdCount"], v["bidOrdCount"]) < 0 or min(v["askSzMantissa"], v["bidSzMantissa"]) <= 0:
                bad["negative order count or empty size"] += 1
        if name == "SnapshotDepthResponseEvent":
            if tag != "R":
                bad["REST snapshot outside an R record"] += 1
            if not ordered(v["asks"], "pxMantissa", False) or not ordered(v["bids"], "pxMantissa", True):
                bad["REST snapshot levels unsorted"] += 1
            if crossed(v["bids"], v["asks"], "pxMantissa"):
                bad["REST snapshot crossed"] += 1
    else:
        market = "spot" if exchange == "gatesbespot" else "futures"
        if not v.get("channel", "").startswith(market + "."):
            bad[f"{name}: channel {v.get('channel')!r} in a {market} recording"] += 1
        instrument = v.get("s") or v.get("currencyPair") or v.get("contract") or v.get("symbol") or ""
        if instrument.startswith("ob."):
            instrument = instrument[3:].rsplit(".", 1)[0]
        if instrument.lower() != symbol:
            bad[f"{name}: instrument {instrument!r} in {symbol}'s file"] += 1
        latencies[f"{name}.time"].append((recv_us - v["time"]) / 1000)
        if name == "bbo" and not v["bidMantissaPrice"] < v["askMantissaPrice"]:
            bad["best bid not below best ask"] += 1
        if name == "obu":
            # A delta lists the levels that changed in Gate's own order; only
            # a full snapshot is a sorted book.
            if v["full"]:
                if not ordered(v["bids"], "pxMantissa", True) or not ordered(v["asks"], "pxMantissa", False):
                    bad["obu full snapshot unsorted"] += 1
                if crossed(v["bids"], v["asks"], "pxMantissa"):
                    bad["obu full snapshot crossed"] += 1
                if v["firstID"] != 0:
                    bad["obu full snapshot with a first id"] += 1
            else:
                for side in ("bids", "asks"):
                    if len({level["pxMantissa"] for level in v[side]}) != len(v[side]):
                        bad["obu delta repeats a price level"] += 1
                if v["firstID"] > v["lastID"]:
                    bad["obu delta first id above last"] += 1
        if name == "publicTrade":
            if market == "spot":
                if v["priceMantissa"] <= 0 or v["amountMantissa"] <= 0:
                    bad["trade not positive"] += 1
            else:
                ids = [trade["id"] for trade in v["trades"]]
                if ids != sorted(ids) or any(t["size"] == 0 or t["price"] <= 0 for t in v["trades"]):
                    bad["futures trades out of order or empty"] += 1


def check_rest_json(payload, bad):
    """Binance's REST depth snapshot is JSON, not SBE."""
    snapshot = json.loads(payload)
    bids = [float(price) for price, _ in snapshot["bids"]]
    asks = [float(price) for price, _ in snapshot["asks"]]
    if not isinstance(snapshot.get("lastUpdateId"), int):
        bad["REST snapshot without a lastUpdateId"] += 1
    if bids != sorted(bids, reverse=True) or asks != sorted(asks) or (bids and asks and bids[0] >= asks[0]):
        bad["REST snapshot unsorted or crossed"] += 1
    return snapshot


# The numbers `reader` prints on each record's first line, from the schema
# decode, and the pattern that reads them back from `reader`'s output.
NUM = r"(-?[0-9.]+)"
READER_LINES = {
    "R": re.compile(r"R SNAPSHOT lastUpdateId=(\d+) bids=(\d+) asks=(\d+)"),
    "TRADES": re.compile(r"S TRADES \S+ trades=(\d+) t=(\d+)"),
    "BEST_BID_ASK": re.compile(rf"S BEST_BID_ASK \S+ bid={NUM} ask={NUM} t=(\d+)"),
    "DEPTH_SNAPSHOT": re.compile(r"S DEPTH_SNAPSHOT \S+ t=(\d+) uid=(\d+)"),
    "DEPTH_DIFF": re.compile(r"S DEPTH_DIFF \S+ t=(\d+) uid=(\d+)-(\d+)"),
    "BBO_TBT": re.compile(rf"S BBO_TBT code=(\d+) bid={NUM}x{NUM} ask={NUM}x{NUM} orders=(\d+)/(\d+) seq=(\d+) t=(\d+)"),
    "SNAPSHOT_DEPTH": re.compile(r"R SNAPSHOT_DEPTH code=(\d+) seq=(\d+) t=(\d+)"),
    "BBO": re.compile(rf"S BBO \S+ bid={NUM}x{NUM} ask={NUM}x{NUM} u=(\d+) t=(\d+)"),
    "TRADE": re.compile(rf"S TRADE \S+ id=(\d+) side=\w+ px={NUM} amount={NUM} t=(\d+)"),
    "OBU": re.compile(r"S OBU \S+ full=(\d) U=(\d+) u=(\d+) t=(\d+)"),
}


def expected_line(exchange, template, v):
    if exchange == "binancesbespot":
        if template is None:
            return "R", [v["lastUpdateId"], len(v["bids"]), len(v["asks"])]
        if template == 10000:
            return "TRADES", [len(v["trades"]), v["eventTime"]]
        if template == 10001:
            e = v["priceExponent"]
            return "BEST_BID_ASK", [scale(v["bidPrice"], e), scale(v["askPrice"], e), v["eventTime"]]
        if template == 10002:
            return "DEPTH_SNAPSHOT", [v["eventTime"], v["bookUpdateId"]]
        if template == 10003:
            return "DEPTH_DIFF", [v["eventTime"], v["firstBookUpdateId"], v["lastBookUpdateId"]]
    elif exchange == "okxsbe":
        if template == 1000:
            p, s = v["pxExponent"], v["szExponent"]
            return "BBO_TBT", [v["instIdCode"], scale(v["bidPxMantissa"], p), scale(v["bidSzMantissa"], s),
                               scale(v["askPxMantissa"], p), scale(v["askSzMantissa"], s),
                               v["bidOrdCount"], v["askOrdCount"], v["seqId"], v["tsUs"]]
        if template == 1006:
            return "SNAPSHOT_DEPTH", [v["instIdCode"], v["seqId"], v["tsUs"]]
    else:
        if template == 1:
            p, s = v["pxExponent"], v["szExponent"]
            return "BBO", [scale(v["bidMantissaPrice"], p), scale(v["bidMantissaSize"], s),
                           scale(v["askMantissaPrice"], p), scale(v["askMantissaSize"], s), v["u"], v["t"]]
        if template == 2 and exchange == "gatesbespot":
            return "TRADE", [v["idMarket"], scale(v["priceMantissa"], v["pxExponent"]),
                             scale(v["amountMantissa"], v["szExponent"]), v["createTimeUs"]]
        if template == 2:
            return "TRADES", [len(v["trades"]), v["time"]]
        if template == 3:
            return "OBU", [v["full"], v["firstID"], v["lastID"], v["t"]]
    return None


def same(printed, expected):
    """Ids, times and counts must match exactly; prices and sizes are printed
    to 6 decimals."""
    if isinstance(expected, int) and "." not in printed:
        return int(printed) == expected
    return abs(float(printed) - expected) <= 5e-7 + 1e-12 * abs(expected)


def compare_reader(reader, exchange, path, decoded):
    output = subprocess.run([reader, str(path)], capture_output=True, text=True).stdout
    heads = [line for line in output.splitlines() if re.match(r"^\[-?\d+\] [SR] ", line)]
    mismatches = collections.Counter()
    compared = 0
    if len(heads) != len(decoded):
        mismatches[f"reader printed {len(heads)} records, the schema decode found {len(decoded)}"] += 1
    for line, (recv_ns, tag, template, values) in zip(heads, decoded):
        if not line.startswith(f"[{recv_ns}] {tag} "):
            mismatches["record order or receive time differs"] += 1
            continue
        expected = None if values is None else expected_line(exchange, template, values)
        if expected is None:
            continue
        kind, numbers = expected
        match = READER_LINES[kind].search(line)
        if match is None:
            mismatches[f"{kind}: reader's line is not in the expected form"] += 1
            continue
        if len(match.groups()) != len(numbers) or not all(map(same, match.groups(), numbers)):
            key = f"{kind}: reader's numbers differ from the schema's"
            mismatches[key] += 1
            if mismatches[key] <= 2:
                print(f"      reader: {line}\n      schema: {numbers}")
        compared += 1
    summary = ", ".join(f"{k}: {n}" for k, n in mismatches.items()) or "all identical"
    print(f"    reader cross-check: {compared} records compared, {summary}")
    return not mismatches


def verify(directory, reader):
    marker = directory / "_exchange"
    # A directory without a marker was recorded by binancesbespot.
    exchange = marker.read_text().strip() if marker.exists() else "binancesbespot"
    schema = Schema(SCHEMAS / SCHEMA_FILES[exchange])
    print(f"\n######## {directory} ({exchange}) against {schema.path.name}")
    issues = collections.Counter()
    decoder = Decoder(schema, issues)
    ok = True
    files = sorted(directory.glob("*.zst"))
    if not files:
        print("  no .zst files")
    for path in files:
        symbol = path.name.rsplit("_", 1)[0]
        counts, latencies, bad = collections.Counter(), collections.defaultdict(list), collections.Counter()
        codes, decoded, framing = set(), [], 0
        for recv_ns, tag, payload in records(path):
            if tag is None:
                framing += 1
                continue
            if tag == "R" and exchange == "binancesbespot":
                snapshot = check_rest_json(payload, bad)
                counts[f"R JSON snapshot ({len(snapshot['bids'])}x{len(snapshot['asks'])} levels)"] += 1
                decoded.append((recv_ns, tag, None, snapshot))
                continue
            try:
                template, name, values = decoder.message(payload)
            except (Malformed, struct.error, KeyError) as error:
                issues[f"undecodable: {error}"] += 1
                decoded.append((recv_ns, tag, None, None))
                continue
            counts[f"{tag} {name}"] += 1
            decoded.append((recv_ns, tag, template, values))
            check_semantics(exchange, symbol, recv_ns, tag, name, values, latencies, bad, codes)
        print(f"  {path.name}: " + ", ".join(f"{n} {k}" for k, n in sorted(counts.items())))
        if len(codes) > 1:
            bad[f"instIdCodes {sorted(codes)} in one file"] += 1
        if framing:
            print(f"    FRAMING: {framing} broken record(s)")
            ok = False
        for key, values in sorted(latencies.items()):
            print(f"    receive minus {key}: {describe(values)}")
        for key, n in sorted(bad.items()):
            print(f"    SEMANTIC: {key}: {n}")
            ok = False
        if reader and not compare_reader(reader, exchange, path, decoded):
            ok = False
    for key, n in sorted(issues.items()):
        print(f"  SCHEMA: {key}: {n}")
        ok = False
    print(f"  RESULT: {'every record matches the schema' if ok else 'ISSUES FOUND'}")
    return ok


def main():
    args = sys.argv[1:]
    reader = None
    if "--reader" in args:
        index = args.index("--reader")
        reader = args[index + 1]
        del args[index:index + 2]
    if not args or any(arg.startswith("-") for arg in args):
        sys.exit(__doc__)
    results = [verify(Path(directory), reader) for directory in args]
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
