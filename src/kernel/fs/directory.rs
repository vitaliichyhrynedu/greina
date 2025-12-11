use zerocopy::{FromBytes, Immutable, IntoBytes, TryFromBytes};

/// Tracks entries within a directory.
pub struct Directory {
    entries: Vec<DirectoryEntry>,
}

impl Directory {
    /// Constructs an empty [Directory] with given node index and parent node index.
    pub fn new(index: usize, parent_index: usize) -> Self {
        let mut dir = Self {
            entries: Vec::new(),
        };
        dir.add_entry(DirectoryEntry::itself(index));
        dir.add_entry(DirectoryEntry::parent(parent_index));
        dir
    }

    /// Returns a reference to an entry with a given name.
    pub fn get_entry(&self, name: Name) -> Option<&DirectoryEntry> {
        self.entries.iter().find(|e| e.name == name && !e.is_null())
    }

    /// Returns a mutable reference to an entry with a given name.
    pub fn get_mut_entry(&mut self, name: Name) -> Option<&mut DirectoryEntry> {
        self.entries
            .iter_mut()
            .find(|e| e.name == name && !e.is_null())
    }

    /// Adds an entry to the directory.
    pub fn add_entry(&mut self, entry: DirectoryEntry) {
        let vacancy = self.entries.iter_mut().find(|e| e.is_null());
        match vacancy {
            Some(v) => *v = entry,
            None => self.entries.push(entry),
        }
    }

    /// Removes the entry from the directory.
    pub fn remove_entry(&mut self, name: Name) -> Result<(), Error> {
        let entry = self.get_mut_entry(name).ok_or(Error::EntryNotFound)?;
        entry.index = 0;
        Ok(())
    }

    /// Returns a view of the directory as a slice of [DirectoryEntry].
    pub fn as_slice(&self) -> &[DirectoryEntry] {
        self.entries.as_slice()
    }

    /// Constructs a [Directory] from a slice of [DirectoryEntry].
    pub fn from_slice(entries: &[DirectoryEntry]) -> Self {
        Self {
            entries: entries.to_vec(),
        }
    }
}

/// Represents a [Directory] entry.
#[repr(C)]
#[derive(Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable)]
pub struct DirectoryEntry {
    filetype: FileType,
    _pad: [u8; 7],
    index: usize,
    name: Name,
}

impl DirectoryEntry {
    /// Constructs a directory entry with given parameters
    pub fn new(index: usize, filetype: FileType, name: Name) -> Self {
        Self {
            index,
            _pad: [0u8; 7],
            filetype,
            name,
        }
    }

    /// Constructs a `.` directory entry with a given index.
    pub fn itself(index: usize) -> Self {
        Self::new(
            index,
            FileType::Directory,
            Name::new(".").expect("'.' must be a valid directory entry name"),
        )
    }

    /// Constructs a `..` directory entry with a given index.
    pub fn parent(index: usize) -> Self {
        Self::new(
            index,
            FileType::Directory,
            Name::new("..").expect("'..' must be a valid directory entry name"),
        )
    }

    /// Checks if the directory entry does not point to any node.
    pub fn is_null(&self) -> bool {
        self.index == 0
    }
}

/// How long a directory entry name can be.
const MAX_NAME_LEN: usize = 64;

/// Represents the name of a directory entry.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[derive(FromBytes, IntoBytes, Immutable)]
pub struct Name {
    bytes: [u8; MAX_NAME_LEN],
}

impl Name {
    /// Tries to construct a valid directory entry name from a string.
    pub fn new(string: &str) -> Option<Self> {
        let len = string.len();
        if len > MAX_NAME_LEN {
            return None;
        }
        let mut bytes = [0u8; MAX_NAME_LEN];
        bytes[..len].copy_from_slice(string.as_bytes());
        Some(Self { bytes })
    }

    /// Returns the string representation of the name.
    pub fn as_str(&self) -> &str {
        let len = self
            .bytes
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(MAX_NAME_LEN);
        str::from_utf8(&self.bytes[..len]).expect("'bytes' must contain a valid UTF-8 string")
    }
}

/// Represents file types.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[derive(TryFromBytes, IntoBytes, Immutable)]
pub enum FileType {
    File,
    Directory,
}

pub enum Error {
    EntryNotFound,
}
