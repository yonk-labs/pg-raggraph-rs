//! SC-010: bounded empty-queue backoff. 1s → 5s → grow → cap 30s; `reset()`
//! (called on a successful claim) returns to 1s. Pure policy, no DB.

use pg_raggraph_sidecar::jobloop::Backoff;

#[test]
fn backoff_grows_then_caps_and_resets() {
    let mut b = Backoff::new();
    assert_eq!(b.next_delay().as_secs(), 1);
    assert_eq!(b.next_delay().as_secs(), 5);
    // keep advancing; must monotonically grow then cap at 30
    let mut last = b.next_delay().as_secs();
    for _ in 0..12 {
        let d = b.next_delay().as_secs();
        assert!(
            d >= last,
            "must not shrink while empty (was {last}, now {d})"
        );
        last = d;
    }
    assert_eq!(last, 30, "must cap at 30s");
    b.reset(); // a claimed job resets the backoff
    assert_eq!(b.next_delay().as_secs(), 1);
}

#[test]
fn backoff_first_delay_is_one_second() {
    let mut b = Backoff::new();
    assert_eq!(b.next_delay(), std::time::Duration::from_secs(1));
}
