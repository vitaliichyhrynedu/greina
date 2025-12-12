use zerocopy::{FromBytes, Immutable, IntoBytes, TryFromBytes};

use crate::kernel::fs::node::FileType;

/// Tracks entries within a directory.
pub struct Dir {
    entries: Vec<DirEntry>,
}

impl Dir {
    /// Constructs an empty [Dir] with given node index and parent node index.
    pub fn new(index: usize, parent_index: usize) -> Self {
        let mut dir = Self {
            entries: Vec::new(),
        };
        dir.add_entry(DirEntry::itself(index));
        dir.add_entry(DirEntry::parent(parent_index));
        dir
    }

    /// Returns a reference to the entry with a given name.
    pub fn get_entry(&self, name: DirEntryName) -> Option<&DirEntry> {
        self.entries.iter().find(|e| e.name == name && !e.is_null())
    }

    /// Returns a mutable reference to the entry with a given name.
    pub fn get_mut_entry(&mut self, name: DirEntryName) -> Option<&mut DirEntry> {
        self.entries
            .iter_mut()
            .find(|e| e.name == name && !e.is_null())
    }

    /// Adds an entry to the directory.
    pub fn add_entry(&mut self, entry: DirEntry) {
        let vacancy = self.entries.iter_mut().find(|e| e.is_null());
        match vacancy {
            Some(v) => *v = entry,
            None => self.entries.push(entry),
        }
    }

    /// Removes the entry from the directory, returning its node index.
    pub fn remove_entry(&mut self, name: DirEntryName) -> Result<usize> {
        let entry = self.get_mut_entry(name).ok_or(Error::EntryNotFound)?;
        let node_index = entry.node_index;
        entry.node_index = 0;
        Ok(node_index)
    }

    /// Returns a view of the directory as a slice of [DirEntry].
    pub fn as_slice(&self) -> &[DirEntry] {
        self.entries.as_slice()
    }

    /// Constructs a [Dir] from a slice of [DirEntry].
    pub fn from_slice(entries: &[DirEntry]) -> Self {
        Self {
            entries: entries.to_vec(),
        }
    }
}

/// Represents a [Dir] entry.
#[repr(C)]
#[derive(Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable)]
pub struct DirEntry {
    filetype: FileType,
    _pad: [u8; 7],
    node_index: usize,
    name: DirEntryName,
}

impl DirEntry {
    /// Constructs a directory entry with given parameters
    pub fn new(node_index: usize, filetype: FileType, name: DirEntryName) -> Self {
        Self {
            node_index,
            _pad: [0u8; 7],
            filetype,
            name,
        }
    }

    /// Constructs a `.` directory entry with a given index.
    pub fn itself(index: usize) -> Self {
        Self::new(
            index,
            FileType::Dir,
            DirEntryName::try_from(".").expect("'.' must be a valid directory entry name"),
        )
    }

    /// Constructs a `..` directory entry with a given index.
    pub fn parent(index: usize) -> Self {
        Self::new(
            index,
            FileType::Dir,
            DirEntryName::try_from("..").expect("'..' must be a valid directory entry name"),
        )
    }

    /// Checks if the directory entry does not point to any node.
    pub fn is_null(&self) -> bool {
        self.node_index == 0
    }

    pub fn filetype(&self) -> FileType {
        self.filetype
    }

    pub fn node_index(&self) -> usize {
        self.node_index
    }
}

/// How long a directory entry name can be.
const MAX_NAME_LEN: usize = 64;

/// Represents the name of a directory entry.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct DirEntryName {
    bytes: [u8; MAX_NAME_LEN],
}

impl DirEntryName {
    /// Returns the directory entry name as a string slice `&str`.
    ///
    /// # Errors
    /// Returns `Err` if:
    /// - `self.bytes` is not a valid UTF-8 string (data corruption?)
    pub fn as_str(&self) -> Result<&str> {
        <&str>::try_from(self)
    }
}

impl TryFrom<&str> for DirEntryName {
    type Error = Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        let len = value.len();
        if len > MAX_NAME_LEN {
            return Err(Error::NameTooLong);
        }
        let mut bytes = [0u8; MAX_NAME_LEN];
        bytes[..len].copy_from_slice(value.as_bytes());
        Ok(Self { bytes })
    }
}

impl<'a> TryFrom<&'a DirEntryName> for &'a str {
    type Error = Error;

    fn try_from(value: &'a DirEntryName) -> std::result::Result<Self, Self::Error> {
        let len = value
            .bytes
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(MAX_NAME_LEN);
        str::from_utf8(&value.bytes[..len]).map_err(|_| Error::CorruptedName)
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    EntryNotFound,
    NameTooLong,
    CorruptedName,
}
