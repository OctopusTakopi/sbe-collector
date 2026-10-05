//! What the collector and the `reader` and `gap_detector` tools share: the
//! exchanges, the on-disk record framing and the SBE stream schemas.

pub mod exchange;
pub mod record;
pub mod sbe;
