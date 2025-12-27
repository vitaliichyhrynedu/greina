use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::block::BlockAddr;

/// Represents a contiguous span of blocks.
#[repr(C)]
#[derive(Default, Clone, Copy)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Extent {
    pub start: BlockAddr,
    pub end: BlockAddr,
}

impl Extent {
    /// Checks whether the extent does not point to any blocks.
    pub fn is_null(&self) -> bool {
        self.start == 0 && self.end == 0
    }

    /// Checks whether the extent represents a hole (sparse region).
    pub fn is_hole(&self) -> bool {
        self.start == 0 && self.end > 0
    }

    /// Zeroes out the extent.
    pub fn nullify(&mut self) {
        self.start = 0;
        self.end = 0;
    }

    /// Returns the number of blocks this extent covers.
    pub fn len(&self) -> u64 {
        self.end - self.start
    }

    /// Represesnts itself as a (start, end) span.
    pub fn span(&self) -> (u64, u64) {
        (self.start, self.end)
    }
}
