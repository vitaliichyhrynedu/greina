use zerocopy::{FromBytes, IntoBytes, TryFromBytes};

use crate::{
    hardware::storage::Storage,
    kernel::fs::{
        alloc_map::{AllocFlag, AllocMap},
        directory::Dir,
        node::FileType,
        superblock::Superblock,
        transaction::Transaction,
    },
};

pub mod alloc_map;
pub mod directory;
pub mod node;
pub mod superblock;
pub mod transaction;

/// Root directory's node index.
pub const ROOT_INDEX: usize = 1;

/// An in-memory view of the filesystem.
pub struct Filesystem {
    superblock: Superblock,
    block_map: AllocMap,
    node_map: AllocMap,
}

impl Filesystem {
    /// Formats the persistent storage with a filesystem.
    ///
    /// # Panics
    /// ...
    pub fn format(storage: &mut Storage, block_count: usize, node_count: usize) -> Self {
        // Superblock
        let superblock = Superblock::new(block_count, node_count);

        // Allocation maps
        let mut block_map = AllocMap::new(block_count);
        let mut node_map = AllocMap::new(node_count);

        // Allocate metadata regions
        block_map
            .allocate_span((0, superblock.data_offset))
            .expect("'0'..'superblock.data_offset' blocks must not be allocated");

        // Allocate the null node
        node_map
            .allocate_at(0)
            .expect("'0'th node must not be allocated");

        // Create filesystem
        let mut fs = Filesystem {
            superblock,
            block_map,
            node_map,
        };

        // Initialize the root directory
        {
            let mut tx = Transaction::new(&mut fs, storage);
            let (_, root_index) = tx
                .create_node(FileType::Dir)
                .expect("Must be able to create the root node");
            assert!(root_index == ROOT_INDEX);
            let root = Dir::new(root_index, root_index);
            tx.write_directory(root_index, &root)
                .expect("Must be able to write the root directory");
            tx.commit();
        }

        fs
    }

    /// Mounts the filesystem from the persistent storage.
    ///
    /// # Panics
    /// ...
    pub fn mount(storage: &Storage) -> Self {
        // Read the superblock
        // TODO: Check whether the superblock is a valid superblock (with a magic number).
        let blocks = storage
            .read_block(0)
            .expect("Must be able to read the superblock");
        let bytes = blocks.as_bytes();
        let superblock = Superblock::read_from_bytes(&bytes[0..size_of::<Superblock>()])
            .expect("'bytes' must be a valid 'Superblock'");

        // Read the block allocation map
        let block_map = Self::read_map(
            storage,
            superblock.block_map_offset,
            superblock.node_map_offset,
            superblock.block_count,
        );

        // Read the node allocation map
        let node_map = Self::read_map(
            storage,
            superblock.node_map_offset,
            superblock.node_table_offset,
            superblock.node_count,
        );

        Self {
            superblock,
            block_map,
            node_map,
        }
    }

    fn read_map(storage: &Storage, map_start: usize, map_end: usize, count: usize) -> AllocMap {
        let block_indices: Vec<usize> = (map_start..map_end).collect();
        let blocks = storage
            .read_blocks(&block_indices)
            .expect("Must be able to read the allocation map");
        let bytes = blocks.as_bytes();
        let flags = <[AllocFlag]>::try_ref_from_bytes(bytes)
            .expect("'bytes' must be a valid '<[AllocFlag]>'");
        AllocMap::from_slice(&flags[..count])
    }
}
