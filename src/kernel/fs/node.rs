use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::hardware::storage::block::BLOCK_SIZE;

/// [Node] size.
pub const NODE_SIZE: usize = size_of::<Node>();

/// How many nodes fit in a [Block].
pub const NODES_PER_BLOCK: usize = BLOCK_SIZE / NODE_SIZE;

/// How many extents a [Node] can have.
const MAX_EXTENTS: usize = 15;

/// Represents a file system object.
#[repr(C)]
#[derive(Default, Clone, Copy)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Node {
    pub size: u64,
    pub link_count: u64,
    pub extents: [Extent; MAX_EXTENTS],
}

/// Represents a contiguous span of blocks.
#[repr(C)]
#[derive(Default, Clone, Copy)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Extent {
    pub start_block: usize,
    pub length: usize,
}
