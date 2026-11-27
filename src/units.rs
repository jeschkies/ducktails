//! Parsing for the units LogQL writes in a query and reads out of a log line.
//!
//! Shared deliberately: Loki parses a duration *literal* with Prometheus' parser
//! falling back to Go's, but a duration *label value* with Go's alone — so
//! `| latency>7d` is accepted while a line carrying `latency=7d` is an error.
//! That asymmetry looks incidental rather than intended, so both sides go
//! through the same parser here.

use std::time::Duration;

use bytesize::ByteSize;

/// One duration unit and its length in nanoseconds.
///
/// Ordered so that a two-character unit is tried before the one-character unit
/// it starts with: `ms` must not read as `m` with a stray `s` behind it.
const DURATION_UNITS: [(&str, u128); 10] = [
    ("ns", 1),
    ("us", 1_000),
    ("µs", 1_000),
    ("ms", 1_000_000),
    ("s", 1_000_000_000),
    ("m", 60 * 1_000_000_000),
    ("h", 60 * 60 * 1_000_000_000),
    // Prometheus' calendar-free definitions, which is what Loki inherits.
    ("d", 24 * 60 * 60 * 1_000_000_000),
    ("w", 7 * 24 * 60 * 60 * 1_000_000_000),
    ("y", 365 * 24 * 60 * 60 * 1_000_000_000),
];

/// Parses a LogQL duration: `2h`, `1h30m`, `1.5s`, `7d`.
///
/// Loki tries Prometheus' `model.ParseDuration` first and falls back to Go's
/// `time.ParseDuration`, so the accepted set is the union of the two: Go's
/// `ns`/`us`/`µs`/`ms`/`s`/`m`/`h` with fractions, plus Prometheus' `d`, `w` and
/// `y`. Unlike Prometheus the units need not descend — Go does not require it
/// either, and rejecting `30m1h` buys the grammar nothing.
///
/// `None` rather than an error: a caller may still have byte sizes to try, and
/// only it knows that everything has failed.
pub fn parse_duration(literal: &str) -> Option<Duration> {
    let mut rest = literal;
    let mut nanos: u128 = 0;

    while !rest.is_empty() {
        // `None` means the magnitude runs to the end of the literal, i.e. a
        // trailing number with no unit — `1h30`.
        let magnitude_end = rest.find(|c: char| !(c.is_ascii_digit() || c == '.'))?;
        let (magnitude, tail) = rest.split_at(magnitude_end);
        let magnitude: f64 = magnitude.parse().ok()?;

        let (unit, tail) = DURATION_UNITS
            .iter()
            .find(|(name, _)| tail.starts_with(name))
            .map(|(name, unit)| (unit, &tail[name.len()..]))?;

        // Rounds to the nearest nanosecond, as Go's parser does. `as u128`
        // saturates rather than wrapping, so an absurd magnitude fails below
        // instead of silently becoming small.
        let unit_nanos = magnitude * *unit as f64;
        if !unit_nanos.is_finite() {
            return None;
        }
        nanos = nanos.checked_add(unit_nanos.round() as u128)?;
        rest = tail;
    }

    Some(Duration::from_nanos(u64::try_from(nanos).ok()?))
}

/// Parses a byte size: `10GB`, `3GiB`, `0.5B`.
///
/// Thin wrapper over `ByteSize`, so the scanner and the label filters agree on
/// SI vs. binary units by construction. Note it is case-insensitive and reads a
/// bare number as bytes — callers that need a unit must check for one.
pub fn parse_bytes(literal: &str) -> Option<u64> {
    literal.parse::<ByteSize>().ok().map(|size| size.as_u64())
}
