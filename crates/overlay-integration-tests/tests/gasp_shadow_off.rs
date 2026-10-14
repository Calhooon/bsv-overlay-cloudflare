//! `off` is today's GASP finalize byte for byte, timing included (bsv-low
//! #657, R7.7 item 5; the design's R7.1). The recording is every storage call
//! the engine makes, in order and each awaited once, every peer call, every
//! sync result and the rows left; the polls count the engine's awaits. Both
//! were frozen on A1's tip `9d5a75a`, before the GASP shadow existed.

#[path = "support/gasp_shadow_world.rs"]
mod world;

#[test]
fn off_recording_is_a1s_byte_for_byte_timing_included() {
    let (transcript, polls) = world::recording(&|_, _, _| {});
    let (again, polls_again) = world::recording(&|_, _, _| {});
    assert_eq!(
        (&transcript, &polls),
        (&again, &polls_again),
        "deterministic"
    );
    println!(
        "OFF RECORDING: {} lines, digest {}, polls {polls:?}",
        transcript.len(),
        world::digest(&transcript)
    );
    assert_eq!(world::digest(&transcript), world::OFF_A1_DIGEST);
    assert_eq!(polls, world::OFF_A1_POLLS);
}
