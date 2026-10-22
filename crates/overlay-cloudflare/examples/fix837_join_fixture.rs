//! bsv-low #837 (the FIX-837 lens's MEDIUM-1): the budget tier's JOIN population for the missing-spend discovery
//! pass (`tools/lane-837/join_hop_pass_ci.mjs`, `make ci-d1-budget`). Prints SQL for the overlay D1: `wide`
//! stranger JOINs of `width` carried P2PKH inputs each and `honest` two-hop JOINs, each a held covenant pot row
//! (old enough to be a discovery candidate, older than any other row so the pass reads these first), a hop row per
//! input (`lockKind = 'p2pkh'`, unspent) and the JOIN's stored atomic BEEF in `pot_beefs`. One comment line per
//! JOIN names it: `-- JOIN <wide|honest> <join txid> <funding txid> <inputs>`.
//!
//! ```text
//! cargo run -q --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --example fix837_join_fixture -- \
//!     <wide> <width> <honest>
//! ```

use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{Beef, Transaction, TransactionInput, TransactionOutput};
use overlay_discovery::pot::covenant::{p2pkh_lock, CovenantParams};
use overlay_discovery::pot::covenant_lock_of;

/// The candidates' age: 100 hours, older than every row the tier's other cells write.
const AGE_SECS: u64 = 360_000;

fn params(seed: u8) -> CovenantParams {
    CovenantParams {
        pub_a: [0x02; 33],
        pub_b: [0x03; 33],
        pub_tower: [0x04; 33],
        pay_pkh_a: [seed; 20],
        pay_pkh_b: [0xBB; 20],
        rake_pkh: [0xCC; 20],
        stake_a: 1250,
        stake_b: 1250,
        fee_sats: 100,
        recovery_height: 900_000,
    }
}

fn join_sql(kind: &str, seed: u8, width: u32) -> String {
    let mut funding = Transaction::new();
    funding
        .add_input(TransactionInput::new(format!("{seed:02x}").repeat(32), 0))
        .unwrap();
    for i in 0..width {
        funding
            .add_output(TransactionOutput {
                satoshis: Some(1_000),
                locking_script: LockingScript::from_binary(&p2pkh_lock(&[(i % 250) as u8; 20]))
                    .unwrap(),
                change: false,
            })
            .unwrap();
    }
    let mut join = Transaction::new();
    for i in 0..width {
        let mut input = TransactionInput::new(funding.id(), i);
        input.source_transaction = Some(Box::new(funding.clone()));
        join.inputs.push(input);
    }
    join.outputs.push(TransactionOutput {
        satoshis: Some(u64::from(width) * 900),
        locking_script: LockingScript::from_binary(&covenant_lock_of(&params(seed))).unwrap(),
        change: false,
    });
    let beef = Beef::from_binary(&join.to_beef(true).unwrap())
        .unwrap()
        .to_binary_atomic(&join.id())
        .unwrap();
    let (j, f) = (join.id(), funding.id());
    let mut out = format!("-- JOIN {kind} {j} {f} {width}\n");
    out.push_str(&format!(
        "INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES ('{j}', 0, 0, unixepoch() - {AGE_SECS}, 'covenant');\n"
    ));
    let rows: Vec<String> = (0..width)
        .map(|i| format!("('{f}', {i}, 0, unixepoch() - {AGE_SECS} - 60, 'p2pkh')"))
        .collect();
    out.push_str(&format!(
        "INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES {};\n",
        rows.join(", ")
    ));
    out.push_str(&format!(
        "INSERT INTO pot_beefs (txid, beef, createdAt, has_proof) VALUES ('{j}', X'{}', unixepoch() - {AGE_SECS}, 0);\n",
        hex::encode(beef)
    ));
    out
}

fn main() {
    let args: Vec<u32> = std::env::args()
        .skip(1)
        .map(|a| {
            a.parse()
                .expect("usage: fix837_join_fixture <wide> <width> <honest>")
        })
        .collect();
    let [wide, width, honest] = args[..] else {
        panic!("usage: fix837_join_fixture <wide> <width> <honest>");
    };
    for k in 0..wide {
        print!("{}", join_sql("wide", 0x10 + k as u8, width));
    }
    for k in 0..honest {
        print!("{}", join_sql("honest", 0x80 + k as u8, 2));
    }
}
