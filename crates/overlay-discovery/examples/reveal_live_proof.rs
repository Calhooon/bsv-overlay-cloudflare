//! Generate a live-proof BEEF for the `tm_reveal` / `ls_reveal` deployment.
//!
//! Emits a BEEF built from a REAL mainnet LOW break-glass reveal tx
//! (txid `a0e644db698f510db0d1e50b9fec7a2d72ce328a8a1b51dfea90e6ce6cbf4c24`,
//! the golden vector the tower's `break_glass.rs` locks) plus the
//! `(gameId, seat)` you query it back with.
//!
//! Usage (from the repo root):
//! ```sh
//! cargo run -p bsv-overlay-discovery --example reveal_live_proof
//! # then POST REVEAL_BEEF to /submit with x-topics: ["tm_reveal"] and
//! # x-submit-mode: historical-tx-no-spv, and query /lookup ls_reveal:
//! #   {"service":"ls_reveal","query":{"type":"byGameSeat","gameId":"<GAME_ID>","seat":<SEAT>}}
//! ```

use bsv_rs::transaction::Transaction;

/// The captured REAL mainnet break-glass reveal tx (same golden vector as
/// the tower's `break_glass.rs` and this crate's topic-manager tests).
const GOLDEN_REVEAL_RAW: &str = "010000000143ee8ac505e1a71b5ed7352bc2700bf361e1fd776da5578b159f67c4f433a0c1020000006a47304402205c5d9ef2e31742172c3ea9e5eedf144941601532fb3ff5a2dd1990fbc79456f302202ac923630b21685f1466c910ccc41e73e16c7434894074f62b4903199f83e7714121023a4122cb1b8fb58c8ee35b1230c72cd482c5097d1273f7d4b889bed70a3116e4ffffffff030000000000000000fd7d01006a0d4c4f572f72657665616c2f76322066a950e5e22cb232210497896a73a65b7d95be6e5a55c0baf05cc8b69e4ffd1001010500020406084ca059abae85d00fcd811b2b71cdd8450f60be08059ba8b60028d7cbcaa5b0fd527843ec19d5d91cac341fa72f314be4da1a1de92f4c583872ed9fd9572c6e82f7691b38ab993898aaa3db185f2cfb52942edd1eaf6bba10605e49817df5faec1189773d537079785d77fac4806063e8944957ba7bb01d6c13ec5b85362c4272392ddf21303b88019f18eda04478e05ab3ecce0a51a4b75e0bd80a4726e9c5cafdd04ca03491ec22bc6e6c3560b162d922935883e910373a4bb0c555c62193a5f344a2a3692ed91dc189e53afa050292d9c7bd725d57a884843264ba0274184d0ff98a3c7a9003e825cc1ecdfd7f731412a59c2fe005200886b58b5f47715270a9853abd0336af544cf0b568301b237ed85fec0f3ca1a5b15bb81daf8b5696a78462d4edb35d645d096a6b56c0a048a1c9f2bcbfd8d62966d928aeca962bc24401985832e8030000000000001976a914ec48fcae21b11476443fc695aa3b2bc574121ac088ac05140000000000001976a914ec48fcae21b11476443fc695aa3b2bc574121ac088ac00000000";

/// gameId encoded in the golden artifact.
const GAME_ID: &str = "66a950e5e22cb232210497896a73a65b7d95be6e5a55c0baf05cc8b69e4ffd10";
/// seat encoded in the golden artifact (B).
const SEAT: u8 = 1;

fn main() {
    let tx = Transaction::from_hex(GOLDEN_REVEAL_RAW).expect("golden reveal parses");
    // allow_partial: this tx has no attached source-tx / merkle proof, so
    // it's a "no-SPV" BEEF — submit it with x-submit-mode:historical-tx-no-spv.
    let beef = tx.to_beef(true).expect("BEEF serialization");

    println!("REVEAL_TXID={}", tx.id());
    println!("GAME_ID={GAME_ID}");
    println!("SEAT={SEAT}");
    println!("REVEAL_BEEF={}", hex::encode(&beef));
}
