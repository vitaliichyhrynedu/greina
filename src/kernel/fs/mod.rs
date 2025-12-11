use crate::kernel::fs::superblock::Superblock;

pub mod alloc_map;
pub mod directory;
pub mod node;
pub mod superblock;
pub mod transaction;

pub struct FileSystem {
    superblock: Superblock,
}

impl FileSystem {}
