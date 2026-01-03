use zerocopy::{FromZeros, Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::block::{BLOCK_SIZE, BlockAddr};

/// A handle to the tree's node.
struct Node<D> {
    data: D,
}

impl<D> Node<D>
where
    D: AsRef<[u8; BLOCK_SIZE as usize]>,
{
    fn try_new(data: D) -> Result<Self> {
        Header::try_ref_from_prefix(data.as_ref()).map_err(|_| Error::Uninterpretable)?;
        Ok(Self { data })
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

    fn items<I: Item>(&self) -> &[I] {
        let count = self.header().item_count as usize;
        let data = &self.data()[HEADER_SIZE..];
        let (items, _) = <[I]>::try_ref_from_prefix_with_elems(data, count)
            .expect("'self.data' must hold a valid item list");
        items
    }

    /// Returns a reference to the item corresponding to the key.
    fn get_item<I: Item>(&self, key: &Key) -> Option<&I> {
        self.items::<I>()
            .binary_search_by_key(key, |item| *item.key())
            .ok()
            .map(|id| &self.items()[id])
    }

    /// Returns the number of free bytes remaining in the node.
    fn free_space<I: Item>(&self) -> usize {
        let item = size_of::<I>();
        let header = self.header();
        let items = header.item_count as usize * item;
        header.data_offset as usize - (HEADER_SIZE + items)
    }

    /// Checks if an item `I` and its associated data `len` can be inserted.
    fn can_insert<I: Item>(&self, len: usize) -> bool {
        self.free_space::<I>() >= (size_of::<I>() + len)
    }
}

impl<D> Node<D>
where
    D: AsMut<[u8; BLOCK_SIZE as usize]> + AsRef<[u8; BLOCK_SIZE as usize]>,
{
    /// Formats the block's data as an empty node with a default header and returns a handle to it.
    fn format(mut data: D) -> Self {
        let header = Header::default();
        (&mut data.as_mut()[..HEADER_SIZE]).copy_from_slice(header.as_bytes());
        Self::try_new(data).expect("'data' must be a valid node")
    }

    fn mut_data(&mut self) -> &mut [u8; BLOCK_SIZE as usize] {
        self.data.as_mut()
    }

    fn mut_header(&mut self) -> &mut Header {
        let (header, _) = Header::try_mut_from_prefix(&mut self.data.as_mut()[..HEADER_SIZE])
            .expect("'self.data' must hold a valid header");
        header
    }

    fn mut_items<I: Item>(&mut self) -> &mut [I] {
        let count = self.header().item_count as usize;
        let data = &mut self.mut_data()[HEADER_SIZE..];
        let (items, _) = <[I]>::try_mut_from_prefix_with_elems(data, count)
            .expect("'self.data' must hold a valid item list");
        items
    }

    /// Reserves `count` item slots at the end of the item list.
    ///
    /// # Panics:
    /// Panics if the space required for the item slots overflows the node.
    fn reserve_items<I: Item>(&mut self, count: u16) {
        let old_count = self.header().item_count;
        let item = size_of::<I>();
        let start = HEADER_SIZE + old_count as usize * item;
        let end = start + count as usize * item;

        // Panic: will panic if `end > BLOCK_SIZE`.
        (&mut self.mut_data()[start..end]).fill(0);
        self.mut_header().item_count += count;
    }

    /// Inserts an item into the item list.
    fn insert_item<I: Item>(&mut self, item: I) -> Result<()> {
        if !self.can_insert::<I>(0) {
            return Err(Error::NoSpace);
        }

        let items = self.items::<I>();
        let id = match items.binary_search_by_key(item.key(), |item| *item.key()) {
            Ok(_) => return Err(Error::KeyExists),
            Err(id) => id,
        };
        let item_count = items.len();
        let to_shift = item_count - id;

        self.reserve_items::<I>(1);
        let items = self.mut_items();
        items.copy_within(id..(id + to_shift), id + 1);
        items[id] = item;

        Ok(())
    }

    /// Removes the item corresponding to the key.
    fn remove_item<I: Item>(&mut self, key: &Key) -> Result<I> {
        let items = self.mut_items::<I>();

        let id = items
            .binary_search_by_key(key, |item| *item.key())
            .map_err(|_| Error::KeyNotFound)?;
        let target = items[id];

        let next = id + 1;
        let item_count = items.len();
        let to_shift = item_count - next;

        items.copy_within(next..(next + to_shift), id);
        self.mut_header().item_count -= 1;

        Ok(target)
    }
}

/// A handle to the tree's branch node.
struct Branch<D> {
    node: Node<D>,
}

impl<D> Branch<D>
where
    D: AsRef<[u8; BLOCK_SIZE as usize]>,
{
    fn try_new(node: Node<D>) -> Result<Self> {
        let item_count = node.header().item_count;
        <[BranchItem]>::try_ref_from_prefix_with_elems(
            &node.data.as_ref()[HEADER_SIZE..],
            item_count as usize,
        )
        .map_err(|_| Error::Uninterpretable)?;
        Ok(Self { node })
    }

    fn items(&self) -> &[BranchItem] {
        self.node.items::<BranchItem>()
    }

    /// Binary searches for the child containing the item corresponding to the key.
    fn get_child(&self, key: &Key) -> BlockAddr {
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
    /// Formats the block's data as an empty branch of given height and returns a handle to it.
    fn format(data: D, height: u16) -> Self {
        let mut node = Node::format(data);
        node.mut_header().height = height;
        Self::try_new(node).expect("'node' must be a valid node")
    }

    fn mut_items(&mut self) -> &mut [BranchItem] {
        self.node.mut_items::<BranchItem>()
    }

    /// Constructs an item and inserts it into the branch.
    fn insert(&mut self, key: &Key, child: BlockAddr) -> Result<()> {
        let item = BranchItem { key: *key, child };
        self.node.insert_item(item)
    }

    /// Removes the item corresponding to the key from the branch.
    fn remove(&mut self, key: &Key) -> Result<BranchItem> {
        self.node.remove_item(key)
    }

    /// Splits `self` by transfering the second half of its items into `other`.
    /// Returns the split key, which corresponds to the first item in `other`.
    ///
    /// # Panics:
    /// Panics if `other` is not an empty branch node.
    fn split(&mut self, other: &mut Branch<D>) -> Result<Key> {
        let item_count = self.node.header().item_count;
        let mid = item_count / 2;

        let other_items = &self.items()[mid as usize..];
        let other_item_count = other_items.len() as u16;
        other.node.reserve_items::<BranchItem>(other_item_count);
        // Panic: `copy_from_slice` will panic if `other` wasn't empty
        other.mut_items().copy_from_slice(other_items);
        let other_first_key = other_items[0].key;

        self.node.mut_header().item_count = mid;

        Ok(other_first_key)
    }

    fn merge() -> Result<()> {
        todo!()
    }
}

/// A handle to the tree's leaf node.
struct Leaf<D> {
    node: Node<D>,
}

impl<D> Leaf<D>
where
    D: AsRef<[u8; BLOCK_SIZE as usize]>,
{
    fn try_new(node: Node<D>) -> Result<Self> {
        let item_count = node.header().item_count;
        <[LeafItem]>::try_ref_from_prefix_with_elems(
            &node.data.as_ref()[HEADER_SIZE..],
            item_count as usize,
        )
        .map_err(|_| Error::Uninterpretable)?;
        Ok(Self { node })
    }

    fn items(&self) -> &[LeafItem] {
        self.node.items::<LeafItem>()
    }

    /// Returns a slice of items with corresponding `obj_id` and `item_type` fields of the key.
    fn get_items(&self, obj_id: u64, item_type: ItemType) -> &[LeafItem] {
        let start = self.items().partition_point(|i| {
            let key = &i.key;
            (key.obj_id, key.item_type) < (obj_id, item_type)
        });

        let end = self.items().partition_point(|i| {
            let key = &i.key;
            (key.obj_id, key.item_type) <= (obj_id, item_type)
        });

        &self.items()[start..end]
    }

    /// Returns a reference to the data associated with the item corresponding to the key.
    fn get_data(&self, key: &Key) -> Option<&[u8]> {
        let item = self.node.get_item::<LeafItem>(key)?;
        let start = item.offset as usize;
        let end = start + item.len as usize;
        Some(&self.node.data()[start..end])
    }
}

impl<D> Leaf<D>
where
    D: AsMut<[u8; BLOCK_SIZE as usize]> + AsRef<[u8; BLOCK_SIZE as usize]>,
{
    /// Formats the block's data as an empty leaf and returns a mutable view into it.
    fn format(data: D) -> Self {
        let node = Node::format(data);
        Self::try_new(node).expect("'node' must be a valid node")
    }

    fn mut_items(&mut self) -> &mut [LeafItem] {
        self.node.mut_items::<LeafItem>()
    }

    /// Constructs an item and inserts it and its associated data into the leaf.
    fn insert(&mut self, key: &Key, data: &[u8]) -> Result<()> {
        if !self.node.can_insert::<LeafItem>(data.len()) {
            return Err(Error::NoSpace);
        }

        // Construct the item
        let len = data.len() as u16;
        let offset = self.node.header().data_offset - len;
        let item = LeafItem {
            key: *key,
            offset,
            len,
            _pad: Default::default(),
        };
        self.node.insert_item::<LeafItem>(item)?;

        // Insert the data
        let start = offset as usize;
        let end = start + len as usize;
        (&mut self.node.mut_data()[start..end]).copy_from_slice(data);
        self.node.mut_header().data_offset = offset;

        Ok(())
    }

    /// Removes the item corresponding to the key and its associated data from the leaf.
    fn remove(&mut self, key: &Key) -> Result<LeafItem> {
        let target = self.node.remove_item::<LeafItem>(key)?;

        let start = self.node.header().data_offset as usize;
        let end = target.offset as usize;
        self.node.mut_header().data_offset += target.len;
        if start != end {
            // Compact the data area
            let dest = start + target.len as usize;
            self.node.mut_data().copy_within(start..end, dest);

            // Update the items' data offsets
            let items = self.mut_items();
            for item in items {
                if item.offset < target.offset {
                    item.offset += target.len;
                }
            }
        }

        Ok(target)
    }

    /// Splits `self` by transfering the second half of its items into `other`.
    /// Returns the split key, which corresponds to the first item in `other`.
    fn split(&mut self, other: &mut Leaf<D>) -> Result<Key> {
        let item_count = self.node.header().item_count;
        let mid = item_count / 2;

        let other_items = &self.items()[mid as usize..];
        let other_first_key = other_items[0].key;
        let other_keys: Vec<_> = other_items.iter().map(|item| item.key).collect();

        // NOTE: this is O(n^2) due to O(n) compaction for each `self.remove`, can be optimized
        for key in other_keys {
            let data = self
                .get_data(&key)
                .expect("'item' must be an existing item");
            other.insert(&key, data)?;
            self.remove(&key)?;
        }

        Ok(other_first_key)
    }

    fn merge() -> Result<()> {
        todo!()
    }
}

const HEADER_SIZE: usize = size_of::<Header>();

/// A header stored at the beginning of a tree's node.
#[repr(C)]
#[derive(Clone, Copy)]
#[derive(TryFromBytes, IntoBytes, Immutable, KnownLayout)]
struct Header {
    // The distance from this node to a leaf node
    height: u16,
    item_count: u16,
    // The absolute offset of the data area in a leaf node
    data_offset: u16,
    _pad: [u8; 2],
}

impl Default for Header {
    fn default() -> Self {
        Self {
            height: 0,
            item_count: 0,
            data_offset: BLOCK_SIZE as u16,
            _pad: Default::default(),
        }
    }
}

/// A unique identifier of an item in the tree.
#[repr(C)]
#[derive(Clone, Copy)]
#[derive(FromZeros, IntoBytes, Immutable)]
struct Key {
    // The id of the object associated with this item
    obj_id: u64,
    item_type: ItemType,
    _pad: [u8; 7],
    // Additional information that depends on the type of the item
    offset: u64,
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.obj_id
            .cmp(&other.obj_id)
            .then(self.item_type.cmp(&other.item_type))
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
            && self.item_type == other.item_type
            && self.offset == other.offset
    }
}

impl Eq for Key {}

/// A type of an item stored in a tree's leaf node.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[derive(FromZeros, IntoBytes, Immutable)]
enum ItemType {
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

trait Item: Clone + Copy + FromZeros + IntoBytes + Immutable {
    fn key(&self) -> &Key;
}

/// An item stored in a tree's branch node.
#[repr(C)]
#[derive(Clone, Copy)]
#[derive(FromZeros, IntoBytes, Immutable)]
struct BranchItem {
    key: Key,
    // The block address of the child node
    child: BlockAddr,
}

impl Item for BranchItem {
    fn key(&self) -> &Key {
        &self.key
    }
}

/// An item stored in a tree's leaf node.
#[repr(C)]
#[derive(Clone, Copy)]
#[derive(FromZeros, IntoBytes, Immutable)]
struct LeafItem {
    key: Key,
    // The absolute offset of the item's data
    offset: u16,
    // The length of the item's data
    len: u16,
    _pad: [u8; 4],
}

impl Item for LeafItem {
    fn key(&self) -> &Key {
        &self.key
    }
}

type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
enum Error {
    Uninterpretable,
    KeyExists,
    NoSpace,
    KeyNotFound,
}
