#![allow(dead_code)]

#[cfg(test)]
mod tests;

use std::{fmt::Debug, marker::PhantomData};

use zerocopy::{
    FromZeros, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned,
    little_endian::{U16, U64},
};

use crate::{
    block::BLOCK_SIZE,
    tree::{Error, Result},
};

#[derive(Debug)]
enum NodeVariant<D> {
    Branch(Branch<D>),
    Leaf(Leaf<D>),
}

impl<D> NodeVariant<D>
where
    D: AsRef<[u8; BLOCK_SIZE as usize]>,
{
    fn try_new(data: D) -> Result<Self> {
        let (header, _) =
            Header::try_ref_from_prefix(data.as_ref()).map_err(|_| Error::Uninterpretable)?;
        let node = if header.height.get() == 0 {
            Self::Leaf(Leaf::try_new(data)?)
        } else {
            Self::Branch(Branch::try_new(data)?)
        };
        Ok(node)
    }
}

/// A handle to the tree's node.
/// Items in a node are guaranteed to be sorted by key.
///
/// # Type parameters
/// - `D` determines whether the handle is mutable or immutable.
/// - `I` determines whether the node is a branch or a leaf node.
#[derive(Debug)]
struct Node<D, I> {
    data: D,
    _item_type: PhantomData<I>,
}

impl<D, I> Node<D, I>
where
    D: AsRef<[u8; BLOCK_SIZE as usize]>,
    I: Item,
{
    fn try_new(data: D) -> Result<Self> {
        let (header, _) =
            Header::try_ref_from_prefix(data.as_ref()).map_err(|_| Error::Uninterpretable)?;
        <[I]>::try_ref_from_prefix_with_elems(
            &data.as_ref()[HEADER_SIZE..],
            header.item_count.into(),
        )
        .map_err(|_| Error::Uninterpretable)?;
        Ok(Self {
            data,
            _item_type: PhantomData,
        })
    }

    fn data(&self) -> &[u8; BLOCK_SIZE as usize] {
        self.data.as_ref()
    }

    fn header(&self) -> &Header {
        let (header, _) = Header::try_ref_from_prefix(&self.data.as_ref()[..HEADER_SIZE])
            .expect("'self.data' must hold a valid header");
        header
    }

    fn is_leaf(&self) -> bool {
        self.header().height == 0
    }

    fn items(&self) -> &[I] {
        let (items, _) = <[I]>::try_ref_from_prefix_with_elems(
            &self.data()[HEADER_SIZE..],
            self.header().item_count.into(),
        )
        .expect("'self.data' must hold a valid item list");
        items
    }

    /// Binary searches for the item corresponding to the key.
    fn get_item_id(&self, key: &Key) -> Result<usize> {
        self.items()
            .binary_search_by_key(key, |item| *item.key())
            .map_err(|_| Error::KeyNotFound)
    }

    /// Binary searches for the item corresponding to the key.
    fn get_item(&self, key: &Key) -> Result<&I> {
        self.get_item_id(key).map(|id| &self.items()[id])
    }

    /// Returns a slice of items with corresponding `obj_id` and `data_type` fields of the key.
    fn get_items(&self, obj_id: u64, data_type: DataType) -> &[I] {
        let start = self.items().partition_point(|item| {
            let key = item.key();
            (key.obj_id.get(), key.data_type) < (obj_id, data_type)
        });

        let end = self.items().partition_point(|item| {
            let key = item.key();
            (key.obj_id.get(), key.data_type) <= (obj_id, data_type)
        });

        &self.items()[start..end]
    }

    const ITEM_SIZE: usize = size_of::<I>();

    /// Returns the number of free bytes remaining in the node.
    fn free_space(&self) -> usize {
        let header = self.header();

        let item_count: usize = header.item_count.get().into();
        let items_size = item_count * Self::ITEM_SIZE;
        let data_offset: usize = header.data_offset.get().into();

        data_offset - (HEADER_SIZE + items_size)
    }

    /// Checks if items and their associated data can be inserted.
    fn can_insert(&self, item_count: usize, data_size: usize) -> bool {
        let req = item_count * Self::ITEM_SIZE + data_size;
        self.free_space() >= req
    }
}

impl<D, I> Node<D, I>
where
    D: AsMut<[u8; BLOCK_SIZE as usize]> + AsRef<[u8; BLOCK_SIZE as usize]>,
    I: Item,
{
    /// Formats the block's data as an empty node of given height and returns a handle to it.
    fn format(mut data: D, height: u16) -> Self {
        let mut header = Header::default();
        header.height.set(height);
        data.as_mut()[..HEADER_SIZE].copy_from_slice(header.as_bytes());
        Self::try_new(data).expect("'node' must be a valid node")
    }

    fn data_mut(&mut self) -> &mut [u8; BLOCK_SIZE as usize] {
        self.data.as_mut()
    }

    fn header_mut(&mut self) -> &mut Header {
        let (header, _) = Header::try_mut_from_prefix(&mut self.data.as_mut()[..HEADER_SIZE])
            .expect("'self.data' must hold a valid header");
        header
    }

    fn items_mut(&mut self) -> &mut [I] {
        let count = self.header().item_count.into();
        let (items, _) =
            <[I]>::try_mut_from_prefix_with_elems(&mut self.data_mut()[HEADER_SIZE..], count)
                .expect("'self.data' must hold a valid item list");
        items
    }

    /// Prepends the items to the start of the item list.
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - `self` can fit `items`.
    /// - The order of the item list is preserved.
    /// - If `I` is `LeafItem`, items' data offsets must be valid.
    unsafe fn prepend_items(&mut self, items: &[I]) {
        let old_count: usize = self.header().item_count.into();
        let prepend_count = items.len();
        let prepend_size = prepend_count * Self::ITEM_SIZE;

        // Shift items
        let start = HEADER_SIZE;
        let end = start + old_count * Self::ITEM_SIZE;
        let dest = start + prepend_size;
        self.data_mut().copy_within(start..end, dest);

        self.data_mut()[start..dest].copy_from_slice(items.as_bytes());

        let new_count: u16 = (old_count + prepend_count).try_into().unwrap();
        self.header_mut().item_count.set(new_count);
    }

    /// Appends the items to the end of the item list.
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - `self` can fit `items`.
    /// - The order of the item list is preserved.
    /// - If `I` is `LeafItem`, items' data offsets must be valid.
    unsafe fn append_items(&mut self, items: &[I]) {
        let old_count: usize = self.header().item_count.into();
        let append_count = items.len();
        let append_size = append_count * Self::ITEM_SIZE;

        let start = HEADER_SIZE + old_count * Self::ITEM_SIZE;
        let end = start + append_size;
        self.data_mut()[start..end].copy_from_slice(items.as_bytes());

        let new_count: u16 = (old_count + append_count).try_into().unwrap();
        self.header_mut().item_count.set(new_count);
    }

    /// Inserts an item into the item list.
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - `self` can fit `item`.
    /// - If `I` is `LeafItem`, items' pointers to data must be valid.
    unsafe fn insert_item(&mut self, item: I) -> Result<()> {
        let items = self.items();
        let id = match items.binary_search_by_key(item.key(), |item| *item.key()) {
            Ok(_) => return Err(Error::KeyExists),
            Err(id) => id,
        };
        let to_shift = items.len() - id;

        // Shift items
        let start = HEADER_SIZE + id * Self::ITEM_SIZE;
        let end = start + to_shift * Self::ITEM_SIZE;
        let dest = start + Self::ITEM_SIZE;
        self.data_mut().copy_within(start..end, dest);

        self.data_mut()[start..dest].copy_from_slice(item.as_bytes());

        self.header_mut().item_count += 1;

        Ok(())
    }

    /// Removes the item at given index.
    ///
    /// # Safety
    /// The caller must ensure that the index is in bounds.
    unsafe fn remove_item_at(&mut self, id: usize) -> I {
        let items = self.items_mut();
        let target = items[id];

        let next = id + 1;
        let to_shift = items.len() - next;

        items.copy_within(next..(next + to_shift), id);
        self.header_mut().item_count -= 1;

        target
    }

    /// Removes the item corresponding to the key.
    fn remove_item(&mut self, key: &Key) -> Result<I> {
        let id = self.get_item_id(key)?;
        // SAFETY: Bounds guaranteed by `get_item_id`
        unsafe { Ok(self.remove_item_at(id)) }
    }
}

type Branch<D> = Node<D, BranchItem>;

impl<D> Branch<D>
where
    D: AsRef<[u8; BLOCK_SIZE as usize]>,
{
    /// Binary searches for the child containing the item corresponding to the key.
    fn get(&self, key: &Key) -> NodePtr {
        let id = self.items().partition_point(|i| i.key <= *key);
        // If `id == 0`, the key is smaller than the smallest key in the tree,
        // so we follow the leftmost path
        self.items()[id.saturating_sub(1)].child
    }
}

impl<D> Branch<D>
where
    D: AsMut<[u8; BLOCK_SIZE as usize]> + AsRef<[u8; BLOCK_SIZE as usize]>,
{
    /// Constructs an item and inserts it into the branch.
    fn insert(&mut self, key: &Key, child: NodePtr) -> Result<()> {
        if !self.can_insert(1, 0) {
            return Err(Error::Overflow);
        }
        let item = BranchItem { key: *key, child };
        // SAFETY: Capacity guaranteed by `can_insert`
        unsafe { self.insert_item(item)? }
        Ok(())
    }

    /// Removes the item corresponding to the key from the branch.
    fn remove(&mut self, key: &Key) -> Result<BranchItem> {
        self.remove_item(key)
    }

    /// Moves items starting from `mid` into the right sibling.
    /// Returns the key of the first item in `right`.
    ///
    /// # Panics
    /// Panics if `self` is empty.
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - `right` is the right sibling of `self`.
    unsafe fn rotate_right(&mut self, mid: u16, right: &mut Node<D, BranchItem>) -> Result<Key> {
        let move_items = &self.items()[mid.into()..];

        if !right.can_insert(move_items.len(), 0) {
            return Err(Error::Overflow);
        }

        let split_key = move_items[0].key;

        // SAFETY:
        // - Capacity guaranteed by `can_insert`
        // - Order guaranteed by `right` being the right sibling of `self`
        unsafe { right.prepend_items(move_items) }

        self.header_mut().item_count.set(mid);

        Ok(split_key)
    }

    /// Moves the second half of items into the right sibling.
    /// Returns the key of the first item in `right`.
    ///
    /// # Panics
    /// Panics if `self` is empty.
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - `right` is empty.
    /// - `right` is the right sibling of `self`.
    unsafe fn split(&mut self, right: &mut Node<D, BranchItem>) -> Result<Key> {
        let item_count = self.header().item_count.get();
        let mid = item_count / 2;
        // SAFETY: Sibship guaranteed by caller
        unsafe { self.rotate_right(mid, right) }
    }

    /// Appends the items from the right sibling into `self`.
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - `right` is the right sibling of `self`.
    unsafe fn merge(&mut self, right: &Node<D, BranchItem>) -> Result<()> {
        let right_items = right.items();

        if !self.can_insert(right_items.len(), 0) {
            return Err(Error::Overflow);
        }

        // SAFETY:
        // - Capacity guaranteed by `can_insert`
        // - Order guaranteed by `right` being the right sibling of `self`
        unsafe { self.append_items(right_items) }

        Ok(())
    }
}

type Leaf<D> = Node<D, LeafItem>;

impl<D> Leaf<D>
where
    D: AsRef<[u8; BLOCK_SIZE as usize]>,
{
    /// Returns a reference to the data associated with the item.
    ///
    /// # Safety
    /// - `item` must be a reference to an existing item.
    unsafe fn get_by_item(&self, item: &LeafItem) -> &[u8] {
        let start = usize::from(item.offset);
        let end = start + usize::from(item.size);
        &self.data()[start..end]
    }

    /// Returns a reference to the data associated with the item corresponding to the key.
    fn get(&self, key: &Key) -> Result<&[u8]> {
        let item = self.get_item(key)?;
        // SAFETY: Existence of `item` guaranteed by it coming from `self.get_item`
        unsafe { Ok(self.get_by_item(item)) }
    }
}

impl<D> Leaf<D>
where
    D: AsMut<[u8; BLOCK_SIZE as usize]> + AsRef<[u8; BLOCK_SIZE as usize]>,
{
    /// Constructs an item and inserts it and its associated data into the leaf.
    fn insert(&mut self, key: &Key, data: &[u8]) -> Result<()> {
        if !self.can_insert(1, data.len()) {
            return Err(Error::Overflow);
        }

        // Construct item
        let size = data.len().try_into().unwrap();
        let offset = self.header().data_offset.get() - size;
        let item = LeafItem {
            key: *key,
            offset: U16::new(offset),
            size: U16::new(size),
            _pad: Default::default(),
        };

        // SAFETY: Capacity guaranteed by `can_insert`
        unsafe { self.insert_item(item)? }

        // Insert data
        let start = usize::from(offset);
        let end = start + usize::from(size);
        self.data_mut()[start..end].copy_from_slice(data);
        self.header_mut().data_offset.set(offset);

        Ok(())
    }

    /// Removes the item at index and its associated data from the leaf.
    ///
    /// # Safety
    /// The caller must ensure that the index is in bounds.
    unsafe fn remove_at(&mut self, id: usize) -> LeafItem {
        // SAFETY: Bounds guaranteed by caller
        let target = unsafe { self.remove_item_at(id) };

        // Calculate span of data that needs to be shifted
        let start = usize::from(self.header().data_offset);
        let end = usize::from(target.offset);
        self.header_mut().data_offset += target.size;

        if start != end {
            // Compact the data area
            let dest = start + usize::from(target.size);
            self.data_mut().copy_within(start..end, dest);

            // Update the items' data offsets
            let items = self.items_mut();
            for item in items {
                if item.offset < target.offset {
                    item.offset += target.size;
                }
            }
        }

        target
    }

    /// Removes the item corresponding to the key and its associated data from the leaf.
    fn remove(&mut self, key: &Key) -> Result<LeafItem> {
        let id = self.get_item_id(key)?;
        // SAFETY: Bounds guaranteed by id coming from `get_item_id`
        unsafe { Ok(self.remove_at(id)) }
    }

    /// Moves items starting from `mid` and their data into the right sibling.
    /// Returns the key of the first item in `right`.
    ///
    /// # Panics
    /// Panics if `self` is empty.
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - `right` is the right sibling of `self`.
    unsafe fn rotate_right(&mut self, mid: u16, right: &mut Node<D, LeafItem>) -> Result<Key> {
        let move_items = &self.items()[mid.into()..];

        let move_data_size = move_items.iter().map(|item| usize::from(item.size)).sum();
        if !right.can_insert(move_items.len(), move_data_size) {
            return Err(Error::Overflow);
        }

        let split_key = move_items[0].key;

        // SAFETY:
        // - Capacity guaranteed by `can_insert`
        // - Order guaranteed by `right` being the right sibling of `self`
        // - Item's offsets are updated when their data is copied
        unsafe { right.prepend_items(move_items) }

        // Copy data from `self` into `other`
        let mut right_data_cursor = right.header().data_offset.get();
        for (i, item) in move_items.iter().enumerate() {
            let data = unsafe {
                // SAFETY: Existense of `item` guaranteed by it coming from `self.items`
                self.get_by_item(item)
            };

            right_data_cursor -= item.size;
            // Update offset
            right.items_mut()[i].offset.set(right_data_cursor);

            let start = usize::from(right_data_cursor);
            let end = start + usize::from(item.size);
            right.data_mut()[start..end].copy_from_slice(data);
        }
        right.header_mut().data_offset.set(right_data_cursor);

        // Remove items and their data starting from the end
        let start = usize::from(mid);
        let end = usize::from(self.header().item_count.get());
        for i in (start..end).rev() {
            // SAFETY: Bounds guaranteed by `end` being `self.header().item_count`
            unsafe { self.remove_at(i) };
        }

        Ok(split_key)
    }

    /// Moves the second half of items and their data into the right sibling.
    /// Returns the key of the first item in `right`.
    ///
    /// # Panics
    /// Panics if `self` is empty.
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - `right` is empty.
    /// - `right` is the right sibling of `self`.
    unsafe fn split(&mut self, right: &mut Node<D, LeafItem>) -> Result<Key> {
        let item_count = self.header().item_count;
        let mid = item_count / 2;
        // SAFETY: Sibship guaranteed by caller
        unsafe { self.rotate_right(mid.into(), right) }
    }

    /// Appends the items and their data from the right sibling into `self`.
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - `right` is the right sibling of `self`.
    unsafe fn merge(&mut self, right: &Node<D, LeafItem>) -> Result<()> {
        let right_items = right.items();

        let right_data_offset = right.header().data_offset.get();
        let right_data_size = BLOCK_SIZE as u16 - right_data_offset;

        if !self.can_insert(right_items.len(), right_data_size.into()) {
            return Err(Error::Overflow);
        }

        let old_item_count = self.header().item_count.get();
        let old_data_offset = self.header().data_offset;

        // SAFETY:
        // - Capacity guaranteed by `can_insert`
        // - Order guaranteed by `right` being the right sibling of `self`
        // - Item's offsets are updated after their data is copied
        unsafe { self.append_items(right_items) }

        // Copy data from `other` to `self`
        let right_data = &right.data()[right_data_offset.into()..];
        let end = usize::from(old_data_offset);
        let start = end - usize::from(right_data_size);
        self.data_mut()[start..end].copy_from_slice(right_data);
        self.header_mut().data_offset -= right_data_size;

        // Update items' offsets
        let new_items = &mut self.items_mut()[old_item_count.into()..];
        let shift = BLOCK_SIZE as u16 - old_data_offset;
        for item in new_items {
            item.offset -= shift;
        }

        Ok(())
    }
}

const HEADER_SIZE: usize = size_of::<Header>();

/// A header stored at the beginning of a node.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
struct Header {
    // The distance from this node to a leaf node
    height: U16,
    item_count: U16,
    // The absolute offset of the data area in a leaf node
    data_offset: U16,
    _pad: [u8; 2],
}

impl Default for Header {
    fn default() -> Self {
        Self {
            height: Default::default(),
            item_count: Default::default(),
            data_offset: U16::new(BLOCK_SIZE as u16),
            _pad: Default::default(),
        }
    }
}

/// A unique identifier of an item in the tree.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[derive(FromZeros, IntoBytes, Immutable, Unaligned)]
struct Key {
    // The id of the object associated with this item
    obj_id: U64,
    data_type: DataType,
    _pad: [u8; 7],
    // Additional information that depends on the data type
    offset: U64,
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.obj_id
            .cmp(&other.obj_id)
            .then(self.data_type.cmp(&other.data_type))
            .then(self.offset.cmp(&other.offset))
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.obj_id == other.obj_id
            && self.data_type == other.data_type
            && self.offset == other.offset
    }
}

impl Eq for Key {}

/// The type of data associated with an item stored in a leaf node.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[derive(FromZeros, IntoBytes, Immutable, Unaligned)]
enum DataType {
    Uninit = 0,

    // Items used in the extent tree

    // A contiguous range of free blocks
    Extent,

    // Items used in the node tree

    // A filesystem object
    Node,
    // A contiguous range of blocks that belongs to a node
    NodeExtent,
    // A mapping of a name to a node
    Entry,
}

trait Item: Debug + Clone + Copy + FromZeros + IntoBytes + Immutable + Unaligned {
    fn key(&self) -> &Key;
}

/// An item stored in a branch node.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[derive(FromZeros, IntoBytes, Immutable, Unaligned)]
struct BranchItem {
    key: Key,
    // The pointer to the child node
    child: NodePtr,
}

impl Item for BranchItem {
    fn key(&self) -> &Key {
        &self.key
    }
}

/// A pointer to the block containing the tree's node.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[derive(FromZeros, IntoBytes, Immutable, Unaligned)]
struct NodePtr(U64);

/// An item stored in a leaf node.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
#[derive(FromZeros, IntoBytes, Immutable, Unaligned)]
struct LeafItem {
    key: Key,
    // The absolute offset of the item's data
    offset: U16,
    // The size of the item's data
    size: U16,
    _pad: [u8; 4],
}

impl Item for LeafItem {
    fn key(&self) -> &Key {
        &self.key
    }
}
