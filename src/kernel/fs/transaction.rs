use std::collections::BTreeMap;

use zerocopy::{FromBytes, IntoBytes, TryFromBytes};

use crate::{
    hardware::storage::{
        Storage,
        block::{BLOCK_SIZE, Block},
    },
    kernel::fs::{
        FileSystem,
        alloc_map::{self, AllocMap},
        directory::{self, Directory, DirectoryEntry, Name},
        node::{self, FileType, NODE_SIZE, NODES_PER_BLOCK, Node},
    },
};

/// A cache to buffer changes.
type Changes = BTreeMap<usize, Block>;

/// A filesystem operation that buffers changes in memory before commiting them to persistent storage.
pub struct Transaction<'a> {
    fs: &'a mut FileSystem,
    storage: &'a mut Storage,
    changes: Changes,
}

impl<'a> Transaction<'a> {
    /// Constructs a [Transaction] for the given filesystem and storage.
    pub fn new(fs: &'a mut FileSystem, storage: &'a mut Storage) -> Self {
        Self {
            fs,
            storage,
            changes: Changes::new(),
        }
    }

    /// Commits the transaction to persistent storage, consuming the transaction.
    pub fn commit(mut self) {
        self.sync_maps();
        for (&block_index, block) in self.changes.iter() {
            self.storage
                .write_block(block_index, block)
                .expect("'block_index' must be a valid block index")
        }
    }

    /// Queues a synchronization of allocation maps.
    fn sync_maps(&mut self) {
        let fs = &self.fs;
        let changes = &mut self.changes;
        Self::buffer_write_map(changes, &fs.block_map, fs.superblock.block_map_offset);
        Self::buffer_write_map(changes, &fs.node_map, fs.superblock.node_map_offset);
    }

    /// Buffers a write to the allocation map.
    fn buffer_write_map(changes: &mut Changes, map: &AllocMap, map_offset: usize) {
        let bytes = map.as_slice().as_bytes();
        for (i, chunk) in bytes.chunks(BLOCK_SIZE).enumerate() {
            let block = Block::read_from_bytes(chunk).unwrap_or_else(|_| {
                let mut block = Block::new();
                block.data[..chunk.len()].copy_from_slice(chunk);
                block
            });
            Self::buffer_write_block(changes, map_offset + i, &block);
        }
    }

    /// Reads the node from the node table.
    pub fn read_node(&self, node_index: usize) -> Result<Node, Error> {
        let block_index = self
            .get_node_block_index(node_index)
            .ok_or(Error::NodeIndexOutOfBounds)?;
        let block = self.read_block(block_index)?;
        let byte_offset = self
            .get_node_byte_offset(node_index)
            .ok_or(Error::NodeIndexOutOfBounds)?;
        Ok(
            Node::try_read_from_bytes(&block.data[byte_offset..(byte_offset + NODE_SIZE)])
                .expect("'bytes' must be a valid 'Node'"),
        )
    }

    // Queues a write of the node to the node table.
    pub fn write_node(&mut self, node_index: usize, node: Node) -> Result<(), Error> {
        let block_index = self
            .get_node_block_index(node_index)
            .ok_or(Error::NodeIndexOutOfBounds)?;
        let mut block = self.read_block(block_index)?;
        let byte_offset = self
            .get_node_byte_offset(node_index)
            .ok_or(Error::NodeIndexOutOfBounds)?;
        block.data[byte_offset..(byte_offset + NODE_SIZE)].copy_from_slice(node.as_bytes());
        self.write_block(block_index, &block);
        Ok(())
    }

    /// Allocates a [Node], returning it and its index.
    pub fn create_node(&mut self, filetype: FileType) -> Result<(Node, usize), Error> {
        let node = Node::new(filetype);
        let (node_index, _) = self.fs.node_map.allocate(1).map_err(|e| Error::Alloc(e))?;
        self.write_node(node_index, node)?;
        Ok((node, node_index))
    }

    /// Allocates a physical block for the node and adds it to node's extents.
    /// Returns the index of the allocated physical block.
    fn grow_node(&mut self, node: &mut Node) -> Result<usize, Error> {
        //
        let (block_index, _) = self.fs.block_map.allocate(1).map_err(|e| Error::Alloc(e))?;
        node.add_block(block_index).map_err(|e| Error::Node(e))?;
        Ok(block_index)
    }

    /// Reads a number of bytes from the file starting from a given offset into the buffer.
    /// Returns the number of bytes read.
    pub fn read_file_at(
        &self,
        node_index: usize,
        offset: usize,
        buf: &mut [u8],
    ) -> Result<usize, Error> {
        let node = self.read_node(node_index)?;

        if offset >= node.size {
            return Ok(0);
        };

        let bytes_available = node.size - offset;
        let bytes_to_read = bytes_available.min(buf.len());
        let mut bytes_read = 0;

        while bytes_read != bytes_to_read {
            let curr_pos = offset + bytes_read;
            let offset_in_block = curr_pos % BLOCK_SIZE;
            let chunk_size = (BLOCK_SIZE - offset_in_block).min(bytes_to_read - bytes_read);
            match node.get_physical_block_from_offset(curr_pos) {
                Some(block_index) => {
                    let data = self.read_block(block_index)?.data;
                    buf[bytes_read..(bytes_read + chunk_size)]
                        .copy_from_slice(&data[offset_in_block..(offset_in_block + chunk_size)]);
                }
                // Handle a sparse file
                None => {
                    buf[bytes_read..(bytes_read + chunk_size)].fill(0u8);
                }
            };
            bytes_read += chunk_size;
        }

        Ok(bytes_read)
    }

    /// Writes a byte slice to the file starting from a given offset.
    /// Returns the number of byttes written.
    pub fn write_file_at(
        &mut self,
        node_index: usize,
        offset: usize,
        data: &[u8],
    ) -> Result<usize, Error> {
        let mut node = self.read_node(node_index)?;

        if offset > node.size {
            return Ok(0);
        };

        let bytes_to_write = data.len();
        let mut bytes_written = 0;

        while bytes_written != bytes_to_write {
            let curr_pos = offset + bytes_written;
            let offset_in_block = curr_pos % BLOCK_SIZE;
            let (block_index, has_grown) = match node.get_physical_block_from_offset(curr_pos) {
                Some(index) => (index, false),
                None => (self.grow_node(&mut node)?, true),
            };
            let chunk_size = (BLOCK_SIZE - offset_in_block).min(bytes_to_write - bytes_written);
            let mut block = if chunk_size == BLOCK_SIZE || has_grown {
                Block::new()
            } else {
                self.read_block(block_index)?
            };
            block.data[offset_in_block..(offset_in_block + chunk_size)]
                .copy_from_slice(&data[bytes_written..(bytes_written + chunk_size)]);
            self.write_block(block_index, &block);
            bytes_written += chunk_size;
        }

        let end_pos = offset + bytes_written;
        if end_pos > node.size {
            node.size = end_pos;
            self.write_node(node_index, node)?;
        }

        Ok(bytes_written)
    }

    /// Creates a file with the given name and type inside the specified parent directory, returning its node index.
    pub fn create_file(
        &mut self,
        parent_index: usize,
        name: &str,
        filetype: FileType,
    ) -> Result<usize, Error> {
        let name = Name::new(name).map_err(|e| Error::Dir(e))?;

        let (mut node, node_index) = self.create_node(FileType::File)?;
        node.link_count += 1;

        let entry = DirectoryEntry::new(node_index, filetype, name);
        let mut parent = self.read_directory(parent_index)?;
        parent.add_entry(entry);

        self.write_directory(parent_index, &parent)?;
        self.write_node(node_index, node)?;

        Ok(node_index)
    }

    /// Reads the directory.
    pub fn read_directory(&self, node_index: usize) -> Result<Directory, Error> {
        let node = self.read_node(node_index)?;
        let mut buf = vec![0u8; node.size];
        self.read_file_at(node_index, 0, &mut buf)?;
        let dir_ents = <[DirectoryEntry]>::try_ref_from_bytes(&buf)
            .expect("'buf' must contain a valid '[DirectoryEntry]'");
        Ok(Directory::from_slice(dir_ents))
    }

    /// Writes the directory.
    pub fn write_directory(&mut self, node_index: usize, dir: &Directory) -> Result<(), Error> {
        let bytes = dir.as_slice().as_bytes();
        self.write_file_at(node_index, 0, bytes)?;
        Ok(())
    }

    /// Creates a directory with the given name inside the specified parent directory, returning its node index.
    pub fn create_directory(&mut self, parent_index: usize, name: &str) -> Result<usize, Error> {
        let node_index = self.create_file(parent_index, name, FileType::Directory)?;
        let dir = Directory::new(node_index, parent_index);
        self.write_directory(node_index, &dir)?;
        Ok(node_index)
    }

    /// Creates a hard link to the file with a given name.
    pub fn link_file(
        &mut self,
        parent_index: usize,
        node_index: usize,
        name: &str,
    ) -> Result<(), Error> {
        let name = Name::new(name).map_err(|e| Error::Dir(e))?;

        let mut node = self.read_node(node_index)?;
        if node.filetype() != FileType::File {
            return Err(Error::FileTypeNotLinkable);
        }
        node.link_count += 1;
        self.write_node(node_index, node)?;

        let mut dir = self.read_directory(parent_index)?;
        let entry = DirectoryEntry::new(node_index, node.filetype(), name);
        dir.add_entry(entry);
        self.write_directory(parent_index, &dir)?;

        Ok(())
    }

    /// Removes a hard link to the file with a given name.
    pub fn unlink_file(&mut self, parent_index: usize, name: &str) -> Result<(), Error> {
        let name = Name::new(name).map_err(|e| Error::Dir(e))?;

        let mut dir = self.read_directory(parent_index)?;
        let entry = dir.get_entry(name).ok_or(Error::FileNotFound)?;
        if entry.filetype() != FileType::File {
            return Err(Error::FileTypeNotLinkable);
        }
        let node_index = dir.remove_entry(name).map_err(|e| Error::Dir(e))?;
        self.write_directory(parent_index, &dir)?;

        let mut node = self.read_node(node_index)?;
        node.link_count -= 1;
        if node.link_count == 0 {
            // Deallocate the file
            for extent in node.extents() {
                self.fs
                    .block_map
                    .free(extent.span())
                    .map_err(|e| Error::Alloc(e))?;
            }
            self.fs
                .node_map
                .free((node_index, node_index + 1))
                .map_err(|e| Error::Alloc(e))?;
            node = Node::default();
        }
        self.write_node(node_index, node)?;

        Ok(())
    }

    /// Reads the physical block.
    pub fn read_block(&self, block_index: usize) -> Result<Block, Error> {
        // Check cached changes
        match self.changes.get(&block_index) {
            Some(block) => Ok(*block),
            None => self
                .storage
                .read_block(block_index)
                .map_err(|_| Error::BlockIndexOutOfBounds),
        }
    }

    /// Buffers a write to the physical block.
    fn buffer_write_block(changes: &mut Changes, block_index: usize, block: &Block) {
        changes.insert(block_index, *block);
    }

    /// Queues a write of the physical block.
    pub fn write_block(&mut self, block_index: usize, block: &Block) {
        Self::buffer_write_block(&mut self.changes, block_index, block);
    }

    /// Returns the index of the block in which the node resides.
    fn get_node_block_index(&self, node_index: usize) -> Option<usize> {
        if node_index < self.fs.superblock.node_count {
            Some(self.fs.superblock.node_table_offset + (node_index * NODE_SIZE / BLOCK_SIZE))
        } else {
            None
        }
    }

    /// Returns the byte offset of the node within the block.
    fn get_node_byte_offset(&self, node_index: usize) -> Option<usize> {
        if node_index < self.fs.superblock.node_count {
            Some(node_index % NODES_PER_BLOCK * NODE_SIZE)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub enum Error {
    BlockIndexOutOfBounds,
    NodeIndexOutOfBounds,
    LogicalIndexOutOfBounds,
    Alloc(alloc_map::Error),
    Dir(directory::Error),
    Node(node::Error),
    FileNotFound,
    FileTypeNotLinkable,
}
