//! What a read of a BEEF through the streaming reader will hold, estimated
//! from the frame's LENGTHS AND COUNTS before anything is allocated (bsv-low
//! #585, the doors lens E585-D12-L3).
//!
//! bsv-rs 0.4.0's `BeefStream` hands out one element at a time, and an
//! element is not its wire bytes: a BUMP leaf of 2 to 38 bytes is a 56 byte
//! `Leaf` and, while the root is computed, an entry in four tables; a raw
//! transaction is its bytes and a record per input and per output. A reader
//! that keeps a table per element adds an entry per element. So the memory of
//! a read follows the element COUNT and a BUMP's LEAVES, not the body's
//! bytes: measured by the lens, natively, 77 MB for a 9.9 MB body of 900,000
//! minimal transactions and 108 MB for a 9.8 MB body holding one BUMP of 2^18
//! leaves, each beside the body in a 128 MB isolate.
//!
//! [`estimate`] walks the frame with no allocation and no hash (it reads the
//! varints and steps over everything else), adds what the caller says each
//! thing costs ([`StreamCharges`]), and stops at the first element that takes
//! the estimate past the caller's limit. The caller then does not open the
//! stream. It is a BUDGET, never a refusal: what a caller does with a breach
//! is its own (the script door answers "the network judges").
//!
//! The estimate is an UPPER bound by construction, not a measurement: every
//! growing buffer is charged three times its contents (a `Vec` that doubles
//! holds the old and the new buffer while it moves), every hash table three
//! times its buckets at the fullest load (7/8), and the sizes are the native
//! ones (a `usize` is 8 bytes; wasm32's are smaller). The pins hold the
//! measured peak under it for each shape.

use bsv_rs::transaction::{ATOMIC_BEEF, BEEF_V1, BEEF_V2};

use crate::script_door::Fields;

/// What a reader holds per thing of the stream, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamCharges {
    /// Kept for the whole read, per raw transaction.
    pub kept_tx: u64,
    /// Kept for the whole read, per input of a raw transaction.
    pub kept_input: u64,
    /// Kept for the whole read, per output of a raw transaction.
    pub kept_output: u64,
    /// Kept for the whole read, per txid a BUMP carries at level 0.
    pub kept_proven: u64,
    /// In hand, one element at a time: per byte of a raw transaction.
    pub tx_byte: u64,
    /// In hand: per input of the raw transaction.
    pub tx_input: u64,
    /// In hand: per output of the raw transaction.
    pub tx_output: u64,
    /// In hand: per leaf of the BUMP, at any level.
    pub bump_leaf: u64,
}

impl StreamCharges {
    /// The stream's own element and nothing kept: what `BeefStream` holds
    /// while it builds and hands out one element.
    ///
    /// A raw transaction: its bytes in a growing buffer (3x), an `InputRef`
    /// of 64 bytes an input and an `OutputRef` of 24 an output, each in a
    /// growing buffer (192, 72). A BUMP leaf: the `Leaf` of 56 bytes in a
    /// growing buffer (168) and, while `bump_root` runs, a path node of 48
    /// twice and an entry in three tables of 49, 41 and 41 bytes a bucket at
    /// up to 16/7 buckets an entry (96 + 112 + 94 + 94): 564, taken as 576.
    pub const STREAM: StreamCharges = StreamCharges {
        kept_tx: 0,
        kept_input: 0,
        kept_output: 0,
        kept_proven: 0,
        tx_byte: 3,
        tx_input: 192,
        tx_output: 72,
        bump_leaf: 576,
    };
}

/// The estimate of one read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamEstimate {
    /// What is kept and the heaviest element in hand, of everything read up
    /// to where the estimate stopped.
    pub bytes: u64,
    /// The estimate passed the limit: the offset of the element that took it
    /// there. Nothing after it was read.
    pub over_at: Option<usize>,
    /// The frame was followed to its end. `false` with no `over_at`: the
    /// bytes are not a BEEF frame this reader follows (the stream refuses
    /// them too), and `bytes` is of what came before.
    pub read: bool,
}

/// What a reader of the stream does with the estimate, before it opens the
/// stream. ONE rule for every reader (the script door and the census; the
/// delta lens E585-D12-DELTA-N1, Rule 10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// The frame was followed to its end within the limit: open the stream.
    Open,
    /// The estimate passed the limit: do NOT open the stream.
    Over,
    /// The frame was not followed to its end, under the limit. The stream
    /// refuses every such frame today (the pin
    /// `e585f_l3_the_sizing_follows_every_frame_the_stream_reads`, re-run at
    /// every bsv-rs bump), so the reader opens it and takes its refusal; a
    /// read the stream DOES make of such bytes was not estimated, and the
    /// reader answers it as over the limit, after the fact.
    Unfollowed,
}

impl StreamEstimate {
    /// The rule of [`Admission`].
    #[must_use]
    pub fn admission(&self) -> Admission {
        if self.over_at.is_some() {
            Admission::Over
        } else if self.read {
            Admission::Open
        } else {
            Admission::Unfollowed
        }
    }
}

/// Estimates what a read of `body` holds under `charges`, stopping at the
/// first element that takes it past `limit`.
#[must_use]
pub fn estimate(body: &[u8], charges: &StreamCharges, limit: u64) -> StreamEstimate {
    let mut sizer = Sizer {
        f: Fields { raw: body, at: 0 },
        charges,
        limit,
        kept: 0,
        largest: 0,
        over_at: None,
    };
    let followed = sizer.frame().is_some();
    StreamEstimate {
        bytes: sizer.kept.saturating_add(sizer.largest),
        over_at: sizer.over_at,
        read: followed,
    }
}

struct Sizer<'a> {
    f: Fields<'a>,
    charges: &'a StreamCharges,
    limit: u64,
    kept: u64,
    /// The heaviest element in hand so far.
    largest: u64,
    over_at: Option<usize>,
}

impl Sizer<'_> {
    /// An element at `at` holds `bytes` in hand: `None` once that is past
    /// the limit.
    fn in_hand(&mut self, at: usize, bytes: u64) -> Option<()> {
        self.largest = self.largest.max(bytes);
        if self.kept.saturating_add(self.largest) > self.limit {
            self.over_at = Some(at);
            return None;
        }
        Some(())
    }

    fn frame(&mut self) -> Option<()> {
        let mut word = self.f.u32()?;
        if word == ATOMIC_BEEF {
            self.f.take(32)?;
            word = self.f.u32()?;
        }
        let v2 = match word {
            BEEF_V1 => false,
            BEEF_V2 => true,
            _ => return None,
        };
        for _ in 0..self.f.varint()? {
            self.bump()?;
        }
        for _ in 0..self.f.varint()? {
            if v2 {
                match *self.f.take(1)?.first()? {
                    0 => self.tx()?,
                    1 => {
                        self.f.varint()?;
                        self.tx()?;
                    }
                    2 => {
                        self.f.take(32)?;
                    }
                    _ => return None,
                }
            } else {
                self.tx()?;
                match *self.f.take(1)?.first()? {
                    0 => {}
                    1 => {
                        self.f.varint()?;
                    }
                    _ => return None,
                }
            }
        }
        Some(())
    }

    /// One BUMP, charged leaf by leaf: a BUMP cut short is stopped where a
    /// whole one would be.
    fn bump(&mut self) -> Option<()> {
        let at = self.f.at;
        self.f.varint()?;
        let levels = *self.f.take(1)?.first()?;
        let mut leaves = 0u64;
        for level in 0..levels {
            for _ in 0..self.f.varint()? {
                self.f.varint()?;
                let hashed = match *self.f.take(1)?.first()? {
                    1 => false,
                    0 | 2 => {
                        self.f.take(32)?;
                        true
                    }
                    _ => return None,
                };
                leaves += 1;
                if level == 0 && hashed {
                    self.kept = self.kept.saturating_add(self.charges.kept_proven);
                }
                self.in_hand(at, leaves.saturating_mul(self.charges.bump_leaf))?;
            }
        }
        Some(())
    }

    /// One raw transaction, charged input by input and output by output.
    fn tx(&mut self) -> Option<()> {
        let at = self.f.at;
        self.f.take(4)?;
        let (mut inputs, mut outputs) = (0u64, 0u64);
        for _ in 0..self.f.varint()? {
            self.f.take(36)?;
            self.f.script()?;
            self.f.take(4)?;
            inputs += 1;
            self.tx_in_hand(at, inputs, outputs)?;
        }
        for _ in 0..self.f.varint()? {
            self.f.take(8)?;
            self.f.script()?;
            outputs += 1;
            self.tx_in_hand(at, inputs, outputs)?;
        }
        self.f.take(4)?;
        self.tx_in_hand(at, inputs, outputs)?;
        self.kept = self
            .kept
            .saturating_add(self.charges.kept_tx)
            .saturating_add(inputs.saturating_mul(self.charges.kept_input))
            .saturating_add(outputs.saturating_mul(self.charges.kept_output));
        self.in_hand(at, 0)
    }

    fn tx_in_hand(&mut self, at: usize, inputs: u64, outputs: u64) -> Option<()> {
        let c = self.charges;
        let bytes = ((self.f.at - at) as u64)
            .saturating_mul(c.tx_byte)
            .saturating_add(inputs.saturating_mul(c.tx_input))
            .saturating_add(outputs.saturating_mul(c.tx_output));
        self.in_hand(at, bytes)
    }
}
