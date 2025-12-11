use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::hardware::storage::block::BLOCK_SIZE;

/// [Node] size.
pub const NODE_SIZE: usize = size_of::<Node>();

/// How many nodes fit in a [Block].
pub const NODES_PER_BLOCK: usize = BLOCK_SIZE / NODE_SIZE;

/// How many extents a [Node] can have.
const EXTENTS_PER_NODE: usize = 15;

/// Represents a file system object.
#[repr(C)]
#[derive(Default, Clone, Copy)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Node {
    pub size: usize,
    pub link_count: u64,
    pub extents: [Extent; EXTENTS_PER_NODE],
}

impl Node {
    /// Resolves the logical block index into a physical block index.
    pub fn get_physical_block(&self, logical_block: usize) -> Option<usize> {
        let mut offset = logical_block;
        for extent in self.extents.iter().take_while(|e| !e.is_null()) {
            let blocks_in_extent = extent.block_count();
            if blocks_in_extent > offset {
                return Some(extent.start + offset);
            }
            offset -= blocks_in_extent;
        }
        None
    }

    /// Resolves the byte offset into a physical block index.
    pub fn get_physical_block_from_offset(&self, byte_offset: usize) -> Option<usize> {
        let logical_block = Self::get_logical_block_from_offset(byte_offset);
        self.get_physical_block(logical_block)
    }

    /// Converts a byte offset into a logical block index
    pub const fn get_logical_block_from_offset(byte_offset: usize) -> usize {
        byte_offset / BLOCK_SIZE
    }

    /// Returns the number of logical blocks that belong to the node.
    pub fn block_count(&self) -> usize {
        self.extents
            .iter()
            .filter(|e| !e.is_null())
            .map(|e| e.end - e.start)
            .sum()
    }

    /// Adds the physical block to node's extents.
    pub fn add_block(&mut self, block_index: usize) -> Result<(), Error> {
        for i in 0..self.extents.len() {
            if self.extents[i].is_null() {
                // Check if we can merge with the previous extent
                if i > 0 {
                    let prev_idx = i - 1;
                    if self.extents[prev_idx].end == block_index {
                        self.extents[prev_idx].end += 1;
                        return Ok(());
                    }
                }
                // Cannot merge (or first extent)
                self.extents[i].start = block_index;
                self.extents[i].end = block_index + 1;
                return Ok(());
            }
        }
        Err(Error::OutOfExtents)
    }
}

/// Represents a contiguous span of physical blocks.
#[repr(C)]
#[derive(Default, Clone, Copy)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Extent {
    start: usize,
    end: usize,
}

impl Extent {
    /// Checks whether the extent does not point to any physical blocks.
    pub fn is_null(&self) -> bool {
        self.start == 0
    }

    /// Returns the number of blocks in this extent.
    pub fn block_count(&self) -> usize {
        self.end - self.start
    }
}

#[derive(Debug)]
pub enum Error {
    OutOfExtents,
}
