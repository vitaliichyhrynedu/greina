use crate::{
    hardware::storage::Storage,
    kernel::{file::OpenFileTable, fs::Filesystem},
};

pub mod file;
pub mod fs;
pub mod syscall;

/// A model for the kernel.
pub struct Kernel {
    storage: Storage,
    fs: Option<Filesystem>,
    open_files: OpenFileTable,
}

impl Kernel {
    /// Constructs a [Kernel].
    pub fn new(storage: Storage) -> Self {
        Self {
            storage,
            fs: None,
            open_files: OpenFileTable::new(),
        }
    }
}
