//! Compiled as tests in each Worker, where option_env! sees that Worker's build.
#[test]
fn build_object_shape() {
    let b = bsv_worker_build_info::build_json!();
    println!("{} build: {b}", env!("CARGO_PKG_NAME"));
    let keys: Vec<_> = b.as_object().unwrap().keys().map(String::as_str).collect();
    assert_eq!(
        keys,
        ["built_at", "crates", "dirty", "engine_rev", "lib_rev"]
    );
    for key in ["engine_rev", "lib_rev"] {
        let rev = b[key].as_str().unwrap();
        assert!(
            rev == "unknown" || (rev.len() == 40 && rev.bytes().all(|c| c.is_ascii_hexdigit())),
            "{b}"
        );
    }
    assert!(b["dirty"].is_boolean(), "{b}");
    let t = b["built_at"].as_str().unwrap();
    assert_eq!(t.len(), 20, "{b}");
    for (i, c) in t.bytes().enumerate() {
        match i {
            4 | 7 => assert_eq!(c, b'-'),
            10 => assert_eq!(c, b'T'),
            13 | 16 => assert_eq!(c, b':'),
            19 => assert_eq!(c, b'Z'),
            _ => assert!(c.is_ascii_digit()),
        }
    }
    assert!((1..=12).contains(&t[5..7].parse::<u8>().unwrap()));
    assert!((1..=31).contains(&t[8..10].parse::<u8>().unwrap()));
    assert!(t[11..13].parse::<u8>().unwrap() < 24);
    assert!(t[14..16].parse::<u8>().unwrap() < 60);
    assert!(t[17..19].parse::<u8>().unwrap() < 60);
    // Amended for bsv-rs 0.6.0 (lane L2-migrate-engine): the lock's versions
    // moved with it, the middleware to its release on 0.6.0.
    assert_eq!(
        b["crates"],
        serde_json::json!({
            "bsv-rs": "0.6.0",
            "bsv-middleware-cloudflare": "0.6.2",
            "bsv-middleware-core": "0.3.2",
        })
    );
}
