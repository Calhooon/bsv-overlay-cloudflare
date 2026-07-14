# tm_pot — LOW Pot-Spend Landing-Proof Topic Manager

Indexes the LOW pot covenant output so its spend (the settle / refund /
sweep) becomes a queryable on-chain landing proof, replacing a browser →
WhatsOnChain `/spent` read. A LOW client credits a payout only once the
pot outpoint is spent *by* the settle txid; `tm_pot` + `ls_pot` serve that
fact from the overlay's own spend bookkeeping.

## What it admits

The live pot lock is the `Poc5TemplatePot` COVENANT (bsv-low #103): a
2-of-3 SETTLE-key multisig PLUS an in-script mandate that any spend pay one
of four templates (winner-A / winner-B / tie / height-gated refund). Its
compiled locking script is:

- a fixed **122-byte HEAD**, then
- 10 per-game param pushes
  (`<pubA><pubB><pubTower><payPkhA><payPkhB><rakePkh><stakeA><stakeB>
  <feeSats><recoveryHeight>`, ~45 bytes, variable), then
- a fixed **2850-byte contract-code TAIL**.

This topic admits an output **IFF its locking script IS that covenant** —
it begins with the HEAD and ends with the TAIL (the variable param middle
is not examined; any committed params are admitted, exactly as the on-chain
contract accepts any params). The 2850-byte tail makes an accidental
collision with a non-covenant script not a practical concern.

The settle transaction's OWN outputs are ordinary P2PKH (winner payout +
rake, or tie splits, or a refund) — those are **not** covenant outputs and
are skipped silently. Submitting the settle to `tm_pot` still fires the
spend notification for the pot INPUT, which is how `ls_pot` records the
spend even though the settle admits no new covenant output.

The recognizer template is copied byte-for-byte from the canonical LOW
source `crates/low-spend/src/poc5_template.hex` and pinned by sha256 (drift
guard) so an upstream template change can't silently mis-index.
