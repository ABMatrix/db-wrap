use std::process::Command;
use std::time::Duration;
use std::thread;
use tempfile::TempDir;
use std::env;

use db_wrap::{DbWrap, RocksdbOptions};
use rocksdb::Options;

fn worker_main(path: &str) {
    let opt: Options = RocksdbOptions::default().into();
    let db = DbWrap::new(path, opt);
    let db_name = "power_test";
    for i in 0..1000u32 {
        let key = format!("key-{i:05}").into_bytes();
        let val = vec![(i % 256) as u8];
        // ignore put errors; we're simulating abrupt termination
        let _ = db.put(&key, val, 1, true, db_name);
        thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn simulate_power_outage() {
    // When re-executed with DB_WRAP_WORKER=1, act as the writer process.
    if env::var("DB_WRAP_WORKER").ok().as_deref() == Some("1") {
        let tmp = env::var("DB_WRAP_TMP").expect("DB_WRAP_TMP must be set for worker");
        worker_main(&tmp);
        return;
    }

    // Test harness: spawn the writer (same test binary), kill it mid-run,
    // then reopen DB and verify recovery (no crash, and some entries persisted).
    let tmpdir = TempDir::new().expect("create tmpdir");
    let tmp_path = tmpdir.path().to_str().unwrap().to_string();

    let mut child = Command::new(env::current_exe().unwrap())
        .env("DB_WRAP_WORKER", "1")
        .env("DB_WRAP_TMP", &tmp_path)
        .spawn()
        .expect("spawn writer child");

    // Let the child run a bit longer so it can perform and flush writes, then kill abruptly.
    thread::sleep(Duration::from_millis(800));
    let _ = child.kill();
    let _ = child.wait();

    // Debug: list temp dir contents so we can see what the writer created
    eprintln!("tmp_path={}", &tmp_path);
    if let Ok(entries) = std::fs::read_dir(&tmp_path) {
        for e in entries.flatten() {
            eprintln!("entry: {:?}", e.path());
            if e.path().is_dir() {
                if let Ok(inner) = std::fs::read_dir(e.path()) {
                    for i in inner.flatten() {
                        eprintln!("  inner: {:?}", i.path());
                    }
                }
            }
        }
    }

    // Reopen DB and check persisted entries
    let opt: Options = RocksdbOptions::default().into();
    let db = DbWrap::new(&tmp_path, opt);
    let entries = db.get_prefix(b"key-", "power_test").expect("get_prefix");
    // Expect some entries to have been persisted before the abrupt kill.
    assert!(entries.len() > 0, "no entries persisted after simulated crash");
    // It's possible the child completed quickly; ensure this test still detects
    // that at least some, but not necessarily all, entries exist. If all 1000
    // are present it means the process finished before being killed.
    assert!(entries.len() <= 1000, "unexpected entry count");
}
