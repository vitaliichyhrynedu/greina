use super::{alloc_map::AllocFlag, node::Node};
use crate::hardware::storage::block::{BLOCK_SIZE, Block};
use zerocopy::{FromBytes, Immutable, IntoBytes};

/// A magic number to identify the filesystem.
pub const MAGIC: usize = 0xF5F5_F5F5;

/// Superblock index.
pub const SUPER_INDEX: usize = 0;

/// Represents metadata about the file system.
#[repr(C)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Superblock {
    pub magic: usize,
    pub block_count: usize,
    pub node_count: usize,
    // Region offsets (specified in blocks)
    pub block_map_offset: usize,
    pub node_map_offset: usize,
    pub node_table_offset: usize,
    pub data_offset: usize,
}

impl Superblock {
    /// Constructs a superblock with specified block and node count.
    pub fn new(block_count: usize, node_count: usize) -> Self {
        let block_map_bytes = block_count * (size_of::<AllocFlag>());
        let block_map_blocks = block_map_bytes.div_ceil(BLOCK_SIZE);

        let node_map_bytes = node_count * (size_of::<AllocFlag>());
        let node_map_blocks = node_map_bytes.div_ceil(BLOCK_SIZE);

        let node_table_bytes = node_count * (size_of::<Node>());
        let node_table_blocks = node_table_bytes.div_ceil(BLOCK_SIZE);

        // Superblock lives in the 0th block
        let block_map_offset = 1;
        let node_map_offset = block_map_offset + block_map_blocks;
        let node_table_offset = node_map_offset + node_map_blocks;
        let data_offset = node_table_offset + node_table_blocks;

        Self {
            magic: MAGIC,
            block_count,
            node_count,
            block_map_offset,
            node_map_offset,
            node_table_offset,
            data_offset,
        }
    }
}

impl From<&Superblock> for Block {
    fn from(value: &Superblock) -> Self {
        let bytes = value.as_bytes();
        Block::new(bytes)
    }
}
