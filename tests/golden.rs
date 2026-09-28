//! Pins the exact output of the `reader` and `gap_detector` binaries.
//!
//! Regenerate with `BLESS=1 cargo test --test golden` only when an output
//! change is intended.

use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::{Duration, SystemTime},
};

const TRADES: u16 = 10_000;
const BEST_BID_ASK: u16 = 10_001;
const DEPTH_SNAPSHOT: u16 = 10_002;
const DEPTH_DIFF: u16 = 10_003;

fn record(recv_ns: i64, tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(13 + payload.len());
    out.extend_from_slice(&recv_ns.to_le_bytes());
    out.push(tag);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

fn sbe(recv_ns: i64, payload: &[u8]) -> Vec<u8> {
    record(recv_ns, b'S', payload)
}

fn rest(recv_ns: i64, json: &str) -> Vec<u8> {
    record(recv_ns, b'R', json.as_bytes())
}

fn header(block_length: u16, template_id: u16) -> Vec<u8> {
    [block_length, template_id, 1, 0]
        .iter()
        .flat_map(|field| field.to_le_bytes())
        .collect()
}

fn symbol(frame: &mut Vec<u8>, symbol: &str) {
    frame.push(symbol.len() as u8);
    frame.extend_from_slice(symbol.as_bytes());
}

fn trades_with_stride(event_time: i64, ids: &[i64], stride: u16, sym: &str) -> Vec<u8> {
    let mut frame = header(18, TRADES);
    frame.extend_from_slice(&event_time.to_le_bytes());
    frame.extend_from_slice(&(event_time + 1).to_le_bytes());
    frame.extend_from_slice(&[(-2i8) as u8, (-5i8) as u8]);
    frame.extend_from_slice(&stride.to_le_bytes());
    frame.extend_from_slice(&(ids.len() as u32).to_le_bytes());
    for &id in ids {
        let start = frame.len();
        frame.extend_from_slice(&id.to_le_bytes());
        frame.extend_from_slice(&(6_543_210 + id).to_le_bytes());
        frame.extend_from_slice(&(150_000 + id).to_le_bytes());
        frame.push((id % 2) as u8);
        frame.resize(start + usize::from(stride), 0xEE);
    }
    symbol(&mut frame, sym);
    frame
}

fn trades(event_time: i64, ids: &[i64], sym: &str) -> Vec<u8> {
    trades_with_stride(event_time, ids, 25, sym)
}

fn best_bid_ask(event_time: i64, update_id: i64, sym: &str) -> Vec<u8> {
    let mut frame = header(50, BEST_BID_ASK);
    frame.extend_from_slice(&event_time.to_le_bytes());
    frame.extend_from_slice(&update_id.to_le_bytes());
    frame.extend_from_slice(&[(-2i8) as u8, (-8i8) as u8]);
    for value in [6_543_210i64, 12_345_678, 6_543_211, 87_654_321] {
        frame.extend_from_slice(&value.to_le_bytes());
    }
    symbol(&mut frame, sym);
    frame
}

fn levels(frame: &mut Vec<u8>, count: u16, base: i64) {
    frame.extend_from_slice(&16u16.to_le_bytes());
    frame.extend_from_slice(&count.to_le_bytes());
    for level in 0..i64::from(count) {
        frame.extend_from_slice(&(base + level).to_le_bytes());
        frame.extend_from_slice(&(1_000 * (level + 1)).to_le_bytes());
    }
}

fn depth_diff(event_time: i64, first: i64, last: i64, bids: u16, asks: u16, sym: &str) -> Vec<u8> {
    let mut frame = header(26, DEPTH_DIFF);
    frame.extend_from_slice(&event_time.to_le_bytes());
    frame.extend_from_slice(&first.to_le_bytes());
    frame.extend_from_slice(&last.to_le_bytes());
    frame.extend_from_slice(&[(-2i8) as u8, (-4i8) as u8]);
    levels(&mut frame, bids, 6_543_200);
    levels(&mut frame, asks, 6_543_300);
    symbol(&mut frame, sym);
    frame
}

fn depth_snapshot(event_time: i64, update_id: i64, bids: u16, asks: u16, sym: &str) -> Vec<u8> {
    let mut frame = header(18, DEPTH_SNAPSHOT);
    frame.extend_from_slice(&event_time.to_le_bytes());
    frame.extend_from_slice(&update_id.to_le_bytes());
    frame.extend_from_slice(&[(-1i8) as u8, (-3i8) as u8]);
    levels(&mut frame, bids, 654_320);
    levels(&mut frame, asks, 654_330);
    symbol(&mut frame, sym);
    frame
}

fn zstd_file(path: &Path, records: &[Vec<u8>]) {
    let mut encoder = zstd::stream::write::Encoder::new(File::create(path).unwrap(), 1).unwrap();
    for record in records {
        encoder.write_all(record).unwrap();
    }
    encoder.finish().unwrap();
}

/// One whole frame holding all but the last record, then a frame holding the
/// last one cut short, as a crashed or still-running writer leaves a file.
fn truncated_zstd_file(path: &Path, records: &[Vec<u8>], cut: usize) {
    let (last, whole) = records.split_last().unwrap();
    let mut bytes = zstd::encode_all(whole.concat().as_slice(), 1).unwrap();
    let tail = zstd::encode_all(last.as_slice(), 1).unwrap();
    bytes.extend_from_slice(&tail[..tail.len() - cut]);
    fs::write(path, bytes).unwrap();
}

/// The collector's clean-close trailer: a zstd skippable frame recording
/// the offset it starts at.
fn trailer(offset: usize) -> Vec<u8> {
    [
        &0x184D_2A5Cu32.to_le_bytes()[..],
        &16u32.to_le_bytes(),
        b"COLLECTR",
        &(offset as u64).to_le_bytes(),
    ]
    .concat()
}

/// As the collector leaves a file after a merge: each frame followed by a
/// trailer.
fn zstd_file_with_trailers(path: &Path, frames: &[&[Vec<u8>]]) {
    let mut bytes = Vec::new();
    for records in frames {
        bytes.extend(zstd::encode_all(records.concat().as_slice(), 1).unwrap());
        bytes.extend(trailer(bytes.len()));
    }
    fs::write(path, bytes).unwrap();
}

fn age(path: &Path) {
    let old = SystemTime::now() - Duration::from_secs(24 * 60 * 60);
    File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(old)
        .unwrap();
}

fn fixture_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sbe-golden-{name}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn run(bin: &str, cwd: &Path, args: &[&str]) -> Output {
    Command::new(bin)
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap()
}

fn assert_golden(name: &str, actual: &[u8]) {
    let actual = String::from_utf8(actual.to_vec()).unwrap();
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden")
        .join(name);
    if std::env::var_os("BLESS").is_some() {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, &actual).unwrap();
        return;
    }
    let expected =
        fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    assert!(
        actual == expected,
        "{name} drifted from {}:\n--- actual ---\n{actual}",
        path.display()
    );
}

const T0: i64 = 1_785_542_400_000_000_000; // 2026-08-01T00:00:00Z
const SECOND: i64 = 1_000_000_000;

#[test]
fn reader_output_is_unchanged() {
    let dir = fixture_dir("reader");
    let ids: Vec<i64> = (100..107).collect();
    let mut short_trades = trades(1, &[1], "BTCUSDT");
    short_trades.truncate(20);
    zstd_file(
        &dir.join("btcusdt_20260801.zst"),
        &[
            sbe(T0, &trades(1_785_542_400_000_001, &ids, "BTCUSDT")),
            sbe(
                T0 + 1,
                &trades(1_785_542_400_000_002, &[107, 108], "BTCUSDT"),
            ),
            sbe(
                T0 + 2,
                &trades_with_stride(1_785_542_400_000_003, &[109, 110], 30, "BTCUSDT"),
            ),
            sbe(T0 + 3, &best_bid_ask(1_785_542_400_000_004, 77, "BTCUSDT")),
            sbe(
                T0 + 4,
                &depth_diff(1_785_542_400_000_005, 10, 20, 2, 6, "BTCUSDT"),
            ),
            sbe(
                T0 + 5,
                &depth_snapshot(1_785_542_400_000_006, 21, 7, 3, "BTCUSDT"),
            ),
            sbe(T0 + 6, &{
                let mut frame = header(4, 12_345);
                frame.extend_from_slice(&[1, 2, 3, 4]);
                symbol(&mut frame, "BTCUSDT");
                frame
            }),
            sbe(T0 + 7, &[1, 2, 3, 4, 5]),
            sbe(T0 + 8, &short_trades),
            sbe(T0 + 9, &best_bid_ask(1_785_542_400_000_010, 78, "$$")),
            sbe(T0 + 10, &{
                let mut frame = depth_diff(1_785_542_400_000_011, 21, 22, 4, 0, "BTCUSDT");
                frame.truncate(8 + 26 + 4 + 16);
                frame
            }),
            rest(
                T0 + 11,
                r#"{"lastUpdateId":21,"bids":[["65432.10","0.5"],["65432.09","1.0"],["65432.08","1.5"],["65432.07","2.0"],["65432.06","2.5"],["65432.05","3.0"],["65432.04","3.5"]],"asks":[["65432.11","0.1"],["65432.12",7]]}"#,
            ),
            rest(T0 + 12, "not json"),
            rest(T0 + 13, r#"{"lastUpdateId":"x"}"#),
            record(T0 + 14, b'X', b"what"),
            // Fits its declared 4-byte block but not the real one: the reader
            // prints nothing for it.
            sbe(T0 + 15, &{
                let mut frame = header(4, TRADES);
                frame.extend_from_slice(&[0; 4]);
                frame
            }),
        ],
    );
    zstd_file(
        &dir.join("ethusdt_20260801.zst"),
        &[sbe(T0 + 15, &best_bid_ask(1, 2, "ETHUSDT"))],
    );
    truncated_zstd_file(
        &dir.join("xrpusdt_20260801.zst"),
        &[
            sbe(T0 + 16, &best_bid_ask(3, 4, "XRPUSDT")),
            sbe(T0 + 17, &trades(5, &[1, 2], "XRPUSDT")),
        ],
        7,
    );

    let output = run(
        env!("CARGO_BIN_EXE_reader"),
        &dir,
        &[
            "btcusdt_20260801.zst",
            "ethusdt_20260801.zst",
            "xrpusdt_20260801.zst",
            "missing.zst",
        ],
    );
    assert!(output.status.success());
    assert_golden("reader.stdout", &output.stdout);
    assert_golden("reader.stderr", &output.stderr);
    fs::remove_dir_all(&dir).unwrap();
}

/// A directory exercising every section of the gap report.
fn gap_fixtures(dir: &Path) {
    let day = 24 * 60 * 60 * SECOND;
    zstd_file(
        &dir.join("btcusdt_20260801.zst"),
        &[
            sbe(T0, &depth_diff(100, 1, 10, 1, 1, "BTCUSDT")),
            sbe(T0 + SECOND, &depth_diff(110, 11, 20, 1, 1, "BTCUSDT")),
            // 7 s of silence, and depth ids 21..30 never arrive.
            sbe(T0 + 8 * SECOND, &depth_diff(120, 31, 40, 1, 1, "BTCUSDT")),
            sbe(
                T0 + 8 * SECOND + 1,
                &depth_diff(115, 25, 40, 1, 1, "BTCUSDT"),
            ),
            sbe(
                T0 + 9 * SECOND,
                &trades(130, &[1_000, 1_001, 1_002], "BTCUSDT"),
            ),
            sbe(T0 + 9 * SECOND + 1, &trades(131, &[1_005], "BTCUSDT")),
            sbe(
                T0 + 9 * SECOND + 2,
                &trades(132, &[1_003, 1_003], "BTCUSDT"),
            ),
            sbe(T0 + 10 * SECOND, &best_bid_ask(140, 500, "BTCUSDT")),
            sbe(T0 + 10 * SECOND + 1, &best_bid_ask(141, 400, "BTCUSDT")),
            sbe(
                T0 + 10 * SECOND + 2,
                &depth_snapshot(150, 41, 1, 1, "BTCUSDT"),
            ),
            rest(T0 + 11 * SECOND, "{}"),
            // Receive-time regression.
            sbe(T0 + 10 * SECOND, &best_bid_ask(142, 501, "BTCUSDT")),
            sbe(T0 + 11 * SECOND + 1, &[0, 0, 0x39, 0x30, 0, 0, 0, 0]),
        ],
    );
    // 2026-08-02 is missing; the series resumes a day later, and a 20 s hole
    // straddles nothing but this file.
    zstd_file(
        &dir.join("btcusdt_20260803.zst"),
        &[
            sbe(T0 + 2 * day, &trades(200, &[1_006, 1_007], "BTCUSDT")),
            sbe(
                T0 + 2 * day + 20 * SECOND,
                &trades(201, &[1_008], "BTCUSDT"),
            ),
            sbe(
                T0 + 2 * day + 50 * SECOND,
                &trades(202, &[1_009], "BTCUSDT"),
            ),
        ],
    );

    // A handover: the incumbent's daily file and the challenger's sidecar
    // overlap, and the same events differ only in their event time.
    let eth = |recv, event_time, ids: &[i64]| sbe(recv, &trades(event_time, ids, "ETHUSDT"));
    zstd_file(
        &dir.join("ethusdt_20260801.zst"),
        &[
            eth(T0, 10, &[1]),
            eth(T0 + SECOND, 11, &[2]),
            eth(T0 + 2 * SECOND, 12, &[3]),
        ],
    );
    zstd_file(
        &dir.join("ethusdt_20260801_1785542400000000001-42.zst"),
        &[
            eth(T0 + SECOND + 5, 21, &[2]),
            eth(T0 + 2 * SECOND + 5, 22, &[3]),
            eth(T0 + 3 * SECOND, 23, &[5]),
        ],
    );

    // Legacy segmented files of one run.
    zstd_file(
        &dir.join("adausdt_20260801_1785542400000000000_run-1.zst"),
        &[sbe(T0, &trades(1, &[1, 2, 3], "ADAUSDT"))],
    );
    zstd_file(
        &dir.join("adausdt_20260801_1785549600000000000_run-1.zst"),
        &[sbe(T0 + 2 * 3_600 * SECOND, &trades(2, &[4, 5], "ADAUSDT"))],
    );

    // The JSON sister collector's format.
    zstd_file(
        &dir.join("solusdt_20260801.zst"),
        &[b"1785542400000000000 {\"e\":\"trade\"}\n".to_vec()],
    );

    // One crashed long ago, one still being written.
    let xrp = [
        sbe(T0, &best_bid_ask(1, 1, "XRPUSDT")),
        sbe(T0 + SECOND, &best_bid_ask(2, 2, "XRPUSDT")),
        sbe(T0 + 2 * SECOND, &best_bid_ask(3, 3, "XRPUSDT")),
    ];
    let crashed = dir.join("xrpusdt_20260801.zst");
    truncated_zstd_file(&crashed, &xrp, 5);
    age(&crashed);
    truncated_zstd_file(&dir.join("xrpusdt_20260802.zst"), &xrp, 5);

    fs::write(dir.join("notes.zst"), b"not a recording").unwrap();
    fs::write(dir.join("readme.txt"), b"ignored").unwrap();
    fs::create_dir_all(dir.join("empty")).unwrap();
    zstd_file(&dir.join("empty").join("dogeusdt_20260801.zst"), &[]);
}

#[test]
fn gap_detector_output_is_unchanged() {
    let dir = fixture_dir("gaps");
    gap_fixtures(&dir);
    let bin = env!("CARGO_BIN_EXE_gap_detector");

    let default = run(bin, &dir, &["."]);
    assert!(default.status.success());
    assert_golden("gap_detector.stdout", &default.stdout);

    let exact = run(
        bin,
        &dir,
        &[".", "--exact", "--min-gap", "1.5", "--max-reported", "1"],
    );
    assert!(exact.status.success());
    assert_golden("gap_detector_exact.stdout", &exact.stdout);

    let filtered = run(bin, &dir, &[".", "--filter", "eth", "--fail-on-gaps"]);
    assert_golden("gap_detector_filtered.stdout", &filtered.stdout);
    assert_eq!(filtered.status.code(), Some(1));

    let nothing = run(bin, &dir, &[".", "--filter", "nothing-matches"]);
    assert!(nothing.status.success());
    assert_golden("gap_detector_none.stdout", &nothing.stdout);

    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn trailers_change_neither_reader_nor_gap_detector_output() {
    let records = [
        sbe(T0, &depth_diff(100, 1, 10, 1, 1, "BTCUSDT")),
        sbe(T0 + SECOND, &trades(110, &[1_000, 1_001], "BTCUSDT")),
        sbe(T0 + 8 * SECOND, &best_bid_ask(120, 500, "BTCUSDT")),
        rest(T0 + 9 * SECOND, "{}"),
    ];
    let plain = fixture_dir("plain");
    let trailed = fixture_dir("trailed");
    zstd_file(&plain.join("btcusdt_20260801.zst"), &records);
    let (first, second) = records.split_at(2);
    zstd_file_with_trailers(&trailed.join("btcusdt_20260801.zst"), &[first, second]);

    let reader = env!("CARGO_BIN_EXE_reader");
    let expected = run(reader, &plain, &["btcusdt_20260801.zst"]);
    let actual = run(reader, &trailed, &["btcusdt_20260801.zst"]);
    assert!(String::from_utf8_lossy(&expected.stdout).contains("total: 4 records"));
    assert_eq!(actual.stdout, expected.stdout);
    assert_eq!(actual.stderr, expected.stderr);

    let gap_detector = env!("CARGO_BIN_EXE_gap_detector");
    let expected = run(gap_detector, &plain, &["."]);
    let actual = run(gap_detector, &trailed, &["."]);
    assert!(expected.status.success() && actual.status.success());
    assert_eq!(
        String::from_utf8(actual.stdout).unwrap(),
        String::from_utf8(expected.stdout).unwrap()
    );

    fs::remove_dir_all(&plain).unwrap();
    fs::remove_dir_all(&trailed).unwrap();
}
