use crate::kernel::{
    Kernel,
    file::{FileDescription, FileDescriptor, FileStats},
    fs::{
        Filesystem, ROOT_INDEX,
        directory::DirEntryName,
        node::FileType,
        transaction::{self, Transaction},
    },
};

impl Kernel {
    /// Creates a file with a given name, if it doesn't exist.
    pub fn create(&mut self, name: &str) -> Result<()> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let mut tx = Transaction::new(fs, &mut self.storage);
        let dir = tx.read_directory(ROOT_INDEX)?;
        let entry_name = DirEntryName::try_from(name).map_err(transaction::Error::from)?;
        if dir.get_entry(entry_name).is_some() {
            tx.commit();
            return Err(Error::FileAlreadyExists);
        }
        tx.create_file(ROOT_INDEX, name, FileType::File)?;
        tx.commit();
        Ok(())
    }

    /// Opens the file specified with `name`, returning a corresponding file descriptor.
    pub fn open(&mut self, name: &str) -> Result<FileDescriptor> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let tx = Transaction::new(fs, &mut self.storage);
        let node_index = tx.lookup(name)?;
        tx.commit();
        let fd = FileDescription::new(node_index);
        Ok(self.open_file(fd))
    }

    /// Close the file descriptor referenced by `fd`.
    pub fn close(&mut self, fd: FileDescriptor) -> Result<()> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let desc = self
            .open_files
            .remove(&fd)
            .ok_or(Error::InvalidFileDescriptor)?;
        let is_opened = self
            .open_files
            .values()
            .any(|d| d.node_index() == desc.node_index());
        if !is_opened {
            let mut tx = Transaction::new(fs, &mut self.storage);
            let node = tx.read_node(desc.node_index())?;
            if node.link_count == 0 {
                tx.delete_node(desc.node_index())?;
            };
            tx.commit();
        }
        Ok(())
    }

    /// Reposition the offset of the file descriptor referenced by `fd`.
    pub fn seek(&mut self, fd: FileDescriptor, offset: usize) -> Result<()> {
        let desc = self
            .open_files
            .get_mut(&fd)
            .ok_or(Error::InvalidFileDescriptor)?;
        desc.offset = offset;
        Ok(())
    }

    /// Reads up to `buf.len()` bytes into `buf` from the file referenced by `fd`.
    /// Returns the number of bytes read.
    pub fn read(&mut self, fd: FileDescriptor, buf: &mut [u8]) -> Result<usize> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let desc = self
            .open_files
            .get_mut(&fd)
            .ok_or(Error::InvalidFileDescriptor)?;
        let tx = Transaction::new(fs, &mut self.storage);
        let bytes_read = tx.read_file_at(desc.node_index(), desc.offset, buf)?;
        tx.commit();
        desc.offset += bytes_read;
        Ok(bytes_read)
    }

    /// Writes up to `buf.len()` bytes from `buf` to the file referenced by `fd`.
    /// Returns the number of bytes written.
    pub fn write(&mut self, fd: FileDescriptor, buf: &[u8]) -> Result<usize> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let desc = self
            .open_files
            .get_mut(&fd)
            .ok_or(Error::InvalidFileDescriptor)?;
        let mut tx = Transaction::new(fs, &mut self.storage);
        let bytes_written = tx.write_file_at(desc.node_index(), desc.offset, buf)?;
        tx.commit();
        desc.offset += bytes_written;
        Ok(bytes_written)
    }

    /// Creates a hard link `new_name` to an existing file referenced by `old_name`.
    pub fn link(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let mut tx = Transaction::new(fs, &mut self.storage);
        let dir = tx.read_directory(ROOT_INDEX)?;
        let entry_name = DirEntryName::try_from(new_name).map_err(transaction::Error::from)?;
        if dir.get_entry(entry_name).is_some() {
            tx.commit();
            return Err(Error::FileAlreadyExists);
        }
        let node_index = tx.lookup(old_name)?;
        tx.link_file(ROOT_INDEX, node_index, new_name)?;
        tx.commit();
        Ok(())
    }

    /// Removes a hard link referenced by `name` from the filesystem.
    /// If `name` was the last link to reference the file, it is deleted.
    /// If `name` is currently opened, it is deleted after it's closed.
    pub fn unlink(&mut self, name: &str) -> Result<()> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let mut tx = Transaction::new(fs, &mut self.storage);
        let node_index = tx.lookup(name)?;
        let is_opened = self
            .open_files
            .values()
            .any(|desc| desc.node_index() == node_index);
        tx.unlink_file(ROOT_INDEX, name, !is_opened)?;
        tx.commit();
        Ok(())
    }

    /// Truncates the file referenced by `name` to be truncated to a size of `size` bytes.
    pub fn truncate(&mut self, name: &str, size: usize) -> Result<()> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let mut tx = Transaction::new(fs, &mut self.storage);
        let node_index = tx.lookup(name)?;
        tx.truncate_file(node_index, size)?;
        tx.commit();
        Ok(())
    }

    /// Returns statistics about a file referenced by `name`.
    pub fn stat(&mut self, name: &str) -> Result<FileStats> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let tx = Transaction::new(fs, &mut self.storage);
        let node_index = tx.lookup(name)?;
        let node = tx.read_node(node_index)?;
        tx.commit();
        Ok(FileStats::new(node_index, node))
    }

    /// Returns the list of hard links and corresponding node indeces in the root directory.
    pub fn ls(&mut self) -> Result<Vec<(String, usize)>> {
        let fs = self.fs.as_mut().ok_or(Error::FilesystemNotMounted)?;
        let tx = Transaction::new(fs, &mut self.storage);
        let dir = tx.read_directory(ROOT_INDEX)?;
        dir.as_slice()
            .iter()
            .filter(|e| !e.is_null())
            .map(|e| {
                let name = e.name().map_err(transaction::Error::from)?.to_string();
                Ok((name, e.node_index()))
            })
            .collect()
    }

    /// Formats the whole storage device with a filesystem capable of handling `node_count` nodes.
    pub fn mkfs(&mut self, node_count: usize) -> Result<()> {
        let block_count = self.storage.block_count();
        self.fs = Some(Filesystem::format(
            &mut self.storage,
            block_count,
            node_count,
        ));
        self.open_files.clear();
        Ok(())
    }

    /// Mounts the filesystem.
    pub fn mount(&mut self) -> Result<()> {
        self.fs = Some(Filesystem::mount(&mut self.storage));
        self.open_files.clear();
        Ok(())
    }

    /// Opens the file by inserting the file description into the open files table.
    /// Returns the corresponding file descriptor.
    fn open_file(&mut self, desc: FileDescription) -> FileDescriptor {
        let fd = self.find_free_fd();
        self.open_files.insert(fd, desc);
        fd
    }

    /// Returns a file descriptor that can be used to open a file.
    fn find_free_fd(&self) -> FileDescriptor {
        let mut fd = 0;
        for &occupied_fd in self.open_files.keys() {
            if fd < occupied_fd {
                return fd;
            }
            fd = occupied_fd + 1;
        }
        fd
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Filesystem(transaction::Error),
    InvalidFileDescriptor,
    FilesystemNotMounted,
    FileAlreadyExists,
}

impl From<transaction::Error> for Error {
    fn from(value: transaction::Error) -> Self {
        Self::Filesystem(value)
    }
}
