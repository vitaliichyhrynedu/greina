#[derive(Clone, Copy)]
pub struct Path<'a>(&'a str);

impl<'a> Path<'a> {
    /// Constructs a path from a `&str`.
    pub fn new(string: &'a str) -> Self {
        Self::from(string)
    }

    /// Returns an iterator over the parts of the path.
    pub fn as_parts(&self) -> impl DoubleEndedIterator<Item = &'a str> {
        let root = if self.0.starts_with('/') {
            Some(&self.0[..1])
        } else {
            None
        };
        root.into_iter()
            .chain(self.0.split('/').filter(|s| !s.is_empty()))
    }

    /// Splits the path into its parent path and the file name.
    /// Returns `None` if the path is empty or is the root directory `/`.
    pub fn split_last(&self) -> Option<(Path<'a>, &'a str)> {
        let name = self.as_parts().next_back()?;
        if name == "/" {
            return None;
        }
        let parent_len = name.as_ptr() as usize - self.0.as_ptr() as usize;
        let parent = &self.0[..parent_len];
        Some((Path::new(parent), name))
    }
}

impl<'a> From<&'a str> for Path<'a> {
    fn from(value: &'a str) -> Self {
        Self(value)
    }
}
