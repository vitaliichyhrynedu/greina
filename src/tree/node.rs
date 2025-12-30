use zerocopy::{FromZeros, Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::block::{BLOCK_SIZE, BlockAddr};

/// A handle to the tree's node, overlaid on a reference to the block's data
struct Node<'a> {
    data: &'a [u8; BLOCK_SIZE as usize],
    header: &'a Header,
}

impl<'a> Node<'a> {
    fn is_leaf(&self) -> bool {
        self.header.height == 0
    }
}

impl<'a> TryFrom<&'a [u8; BLOCK_SIZE as usize]> for Node<'a> {
    type Error = Error;

    fn try_from(data: &'a [u8; BLOCK_SIZE as usize]) -> Result<Self> {
        let (header, _) = Header::try_ref_from_prefix(data).map_err(|_| Error::Uninterpretable)?;
        Ok(Self { data, header })
    }
}

/// A mutable handle to the tree's node, overlaid on a mutable reference to the block's data
struct NodeMut<'a> {
    data: &'a mut [u8; BLOCK_SIZE as usize],
}

impl<'a> NodeMut<'a> {
    /// Formats the block's data with a default header and returns a mutable view into it.
    fn format(data: &'a mut [u8; BLOCK_SIZE as usize]) -> Self {
        let header = Header::default();
        (&mut data[..HEADER_SIZE]).copy_from_slice(header.as_bytes());
        Self::try_from(data).expect("'data' must be interpretable as 'NodeMut'")
    }

    /// Interprets `self` as [Node].
    fn as_node(&self) -> Node<'_> {
        Node::try_from(&*self.data).expect("'self' must be interpretable as 'Node'")
    }

    /// Wrapper over [Node::header].
    fn header(&self) -> &Header {
        self.as_node().header
    }

    fn mut_header(&mut self) -> &mut Header {
        Header::try_mut_from_prefix(self.data)
            .map(|r| r.0)
            .expect("'self.data' must contain a valid 'Header'")
    }

    /// Wrapper over [Node::is_leaf].
    fn is_leaf(&self) -> bool {
        self.as_node().is_leaf()
    }
}

impl<'a> TryFrom<&'a mut [u8; BLOCK_SIZE as usize]> for NodeMut<'a> {
    type Error = Error;

    fn try_from(data: &'a mut [u8; BLOCK_SIZE as usize]) -> Result<Self> {
        Header::try_mut_from_prefix(data).map_err(|_| Error::Uninterpretable)?;
        Ok(Self { data })
    }
}

/// A handle to the tree's branch node, overlaid on a reference to the block's data
struct Branch<'a> {
    node: Node<'a>,
    items: &'a [BranchItem],
}

impl<'a> Branch<'a> {
    /// Binary searches for the child containing the item corresponding to the key.
    fn get_child(&self, key: &Key) -> BlockAddr {
        // Find the index of the first item whose item.key > key
        let id = self.items.partition_point(|i| i.key <= *key);

        // If `id == 0`, then `key < items[0].key`, implying the item doesn't exist yet
        // So if it's to be inserted, we follow the leftmost path
        self.items[id.saturating_sub(1)].child
    }

    /// Returns the number of free bytes remaining in the branch.
    fn free_space(&self) -> usize {
        let items = self.node.header.item_count as usize * BRANCH_ITEM_SIZE;
        let used = HEADER_SIZE + items;
        BLOCK_SIZE as usize - used
    }

    /// Checks if there's enough space for a new item.
    fn can_insert(&self) -> bool {
        self.free_space() >= BRANCH_ITEM_SIZE
    }
}

impl<'a> TryFrom<Node<'a>> for Branch<'a> {
    type Error = Error;

    fn try_from(node: Node<'a>) -> Result<Self> {
        let (items, _) = <[BranchItem]>::try_ref_from_prefix_with_elems(
            &node.data[HEADER_SIZE..],
            node.header.item_count as usize,
        )
        .map_err(|_| Error::Uninterpretable)?;
        Ok(Self { node, items })
    }
}

/// A mutable handle to the tree's branch node, overlaid on a mutable reference to the block's data
struct BranchMut<'a> {
    node: NodeMut<'a>,
}

impl<'a> BranchMut<'a> {
    /// Formats the block's data as an empty branch of given height and returns a mutable view into it.
    fn format(data: &'a mut [u8; BLOCK_SIZE as usize], height: u16) -> Self {
        let mut node = NodeMut::format(data);
        node.mut_header().height = height;
        Self::try_from(node).expect("'node' must be interpretable as 'BranchMut'")
    }

    /// Interprets `self` as [Branch].
    fn as_branch(&self) -> Branch<'_> {
        Branch::try_from(self.node.as_node()).expect("'self' must be interpretable as 'Branch'")
    }

    /// Wrapper over [Branch::items].
    fn items(&self) -> &[BranchItem] {
        self.as_branch().items
    }

    /// Returns a mutable reference to items in the branch.
    fn mut_items(&mut self) -> &mut [BranchItem] {
        // Safety: it's assumed `self.node.header().item_count` doesn't overflow
        let item_count = self.node.header().item_count;
        let (items, _) = <[BranchItem]>::try_mut_from_prefix_with_elems(
            &mut self.node.data[HEADER_SIZE..],
            item_count as usize,
        )
        .expect("'self.data' must contain a valid '[BranchItem]'");
        items
    }

    /// Wrapper over [Branch::can_insert].
    fn can_insert(&self) -> bool {
        self.as_branch().can_insert()
    }

    /// Allocates a new zeroed out item.
    fn alloc_item(&mut self) {
        let header = self.node.mut_header();
        let item_count = header.item_count;
        header.item_count += 1;
        let start = HEADER_SIZE + item_count as usize * BRANCH_ITEM_SIZE;
        let end = start + BRANCH_ITEM_SIZE;
        (&mut self.node.data[start..end]).fill(0);
    }

    /// Inserts an item into the branch.
    fn insert(&mut self, key: &Key, child: BlockAddr) -> Result<()> {
        if !self.can_insert() {
            return Err(Error::NoSpace);
        }

        let items = self.items();
        let id = match items.binary_search_by_key(key, |item| item.key) {
            Ok(_) => return Err(Error::KeyExists),
            Err(id) => id,
        };
        let item_count = items.len();
        let to_shift = item_count - id;

        // Insert the item
        self.alloc_item();
        let items = self.mut_items();
        items.copy_within(id..(id + to_shift), id + 1);
        items[id] = BranchItem { key: *key, child };

        Ok(())
    }

    /// Removes an item from the branch.
    fn remove(&mut self, key: &Key) -> Result<BranchItem> {
        let items = self.mut_items();

        let id = items
            .binary_search_by_key(key, |i| i.key)
            .map_err(|_| Error::KeyNotFound)?;
        let target = items[id];

        let next = id + 1;
        let item_count = items.len();
        let to_shift = item_count - next;

        // Remove the item
        items.copy_within(next..(next + to_shift), id);
        self.node.mut_header().item_count -= 1;

        Ok(target)
    }

    /// Keeps the lower half of items in `self` and inserts the upper half into `other`, returning
    /// the key of the first item of `other`.
    fn split(&mut self, other: &mut BranchMut) -> Result<Key> {
        let item_count = self.node.header().item_count;
        let mid = item_count / 2;

        let upper_items = &self.items()[mid as usize..];
        let other_first_key = upper_items[0].key;

        for item in upper_items {
            other.insert(&item.key, item.child)?;
        }

        self.node.mut_header().item_count = mid;

        Ok(other_first_key)
    }
}

impl<'a> TryFrom<NodeMut<'a>> for BranchMut<'a> {
    type Error = Error;

    fn try_from(node: NodeMut<'a>) -> Result<Self> {
        let item_count = node.header().item_count;
        <[BranchItem]>::try_mut_from_prefix_with_elems(
            &mut node.data[HEADER_SIZE..],
            item_count as usize,
        )
        .map_err(|_| Error::Uninterpretable)?;
        Ok(Self { node })
    }
}

/// A handle to the tree's leaf node, overlaid on a reference to the block's data
struct Leaf<'a> {
    node: Node<'a>,
    items: &'a [LeafItem],
}

impl<'a> Leaf<'a> {
    /// Returns a reference to the item corresponding to the key.
    fn get_item(&self, key: &Key) -> Option<&LeafItem> {
        self.items
            .binary_search_by_key(key, |i| i.key)
            .ok()
            .map(|id| &self.items[id])
    }

    /// Returns a slice of items with corresponding `obj_id` and `item_type` fields of the key.
    fn get_items(&self, obj_id: u64, item_type: ItemType) -> &'a [LeafItem] {
        let start = self.items.partition_point(|i| {
            let key = &i.key;
            (key.obj_id, key.item_type) < (obj_id, item_type)
        });

        let end = self.items.partition_point(|i| {
            let key = &i.key;
            (key.obj_id, key.item_type) <= (obj_id, item_type)
        });

        &self.items[start..end]
    }

    /// Returns a reference to the data associated with the item corresponding to the key.
    fn get_data(&self, key: &Key) -> Option<&'a [u8]> {
        let item = self.get_item(key)?;
        let start = item.offset as usize;
        let end = start + item.len as usize;
        self.node.data.get(start..end)
    }

    /// Returns the number of free bytes remaining in the leaf.
    fn free_space(&self) -> usize {
        let items = self.node.header.item_count as usize * LEAF_ITEM_SIZE;
        self.node.header.data_offset as usize - (HEADER_SIZE + items)
    }

    /// Checks if there's enough space for a new item and its data.
    fn can_insert(&self, len: usize) -> bool {
        let req = LEAF_ITEM_SIZE + len;
        self.free_space() >= req
    }
}

impl<'a> TryFrom<Node<'a>> for Leaf<'a> {
    type Error = Error;

    fn try_from(node: Node<'a>) -> Result<Self> {
        let (items, _) = <[LeafItem]>::try_ref_from_prefix_with_elems(
            &node.data[HEADER_SIZE..],
            node.header.item_count as usize,
        )
        .map_err(|_| Error::Uninterpretable)?;
        Ok(Self { node, items })
    }
}

/// A mutable handle to the tree's leaf node, overlaid on a mutable reference to the block's data
struct LeafMut<'a> {
    node: NodeMut<'a>,
}

impl<'a> LeafMut<'a> {
    /// Formats the block's data as an empty leaf and returns a mutable view into it.
    fn format(data: &'a mut [u8; BLOCK_SIZE as usize]) -> Self {
        let node = NodeMut::format(data);
        Self::try_from(node).expect("'node' must be interpretable as 'LeafMut'")
    }

    /// Interprets `self` as [Leaf].
    fn as_leaf(&self) -> Leaf<'_> {
        Leaf::try_from(self.node.as_node()).expect("'self' must be interpretable as 'Leaf'")
    }

    /// Wrapper over [Leaf::items].
    fn items(&self) -> &[LeafItem] {
        self.as_leaf().items
    }

    fn mut_items(&mut self) -> &mut [LeafItem] {
        let item_count = self.node.header().item_count;
        let (items, _) = <[LeafItem]>::try_mut_from_prefix_with_elems(
            &mut self.node.data[HEADER_SIZE..],
            item_count as usize,
        )
        .expect("'self.data' must contain a valid '[LeafItem]'");
        items
    }

    /// Wrapper over [Leaf::can_insert].
    fn can_insert(&self, len: usize) -> bool {
        self.as_leaf().can_insert(len)
    }

    /// Allocates a new zeroed out item.
    fn alloc_item(&mut self) {
        let header = self.node.mut_header();
        let item_count = header.item_count;
        header.item_count += 1;
        let start = HEADER_SIZE + item_count as usize * LEAF_ITEM_SIZE;
        let end = start + LEAF_ITEM_SIZE;
        (&mut self.node.data[start..end]).fill(0);
    }

    /// Inserts an item into the leaf.
    fn insert(&mut self, key: &Key, data: &[u8]) -> Result<()> {
        if !self.can_insert(data.len()) {
            return Err(Error::NoSpace);
        }

        let items = self.items();
        let id = match items.binary_search_by_key(key, |i| i.key) {
            Ok(_) => return Err(Error::KeyExists),
            Err(id) => id,
        };
        let item_count = items.len();
        let to_shift = item_count - id;

        // Construct the item
        let len = data.len() as u16;
        let offset = self.node.header().data_offset - len;
        let item = LeafItem {
            key: *key,
            offset,
            len,
            _pad: Default::default(),
        };

        // Insert the item
        self.alloc_item();
        let items = self.mut_items();
        items.copy_within(id..(id + to_shift), id + 1);
        items[id] = item;

        // Insert the data
        let start = offset as usize;
        let end = start + len as usize;
        (&mut self.node.data[start..end]).copy_from_slice(data);
        self.node.mut_header().data_offset = offset;

        Ok(())
    }

    /// Removes an item and its data from the leaf.
    fn remove(&mut self, key: &Key) -> Result<LeafItem> {
        let items = self.mut_items();
        let id = items
            .binary_search_by_key(key, |i| i.key)
            .map_err(|_| Error::KeyNotFound)?;
        let target = items[id];

        let next = id + 1;
        let item_count = items.len();
        let to_shift = item_count - next;

        // Remove the item
        items.copy_within(next..(next + to_shift), id);
        self.node.mut_header().item_count -= 1;

        // Remove the item's data
        let start = self.node.header().data_offset as usize;
        let end = target.offset as usize;
        let dest = start + target.len as usize;
        self.node.data.copy_within(start..end, dest);
        self.node.mut_header().data_offset += target.len;

        // Update the offsets of items whose data was shifted
        let items = self.mut_items();
        for item in items {
            if item.offset < target.offset {
                item.offset += target.len;
            }
        }

        Ok(target)
    }

    /// Keeps the lower half of items and their associated data in `self` and inserts the upper
    /// half into `other`, returning the key of the first item of `other`.
    fn split(&mut self, other: &mut LeafMut) -> Result<Key> {
        let item_count = self.node.header().item_count;
        let mid = item_count / 2;

        let new_data_offset = self.items()[mid as usize].offset;

        let upper_items = &self.items()[mid as usize..];
        let other_first_key = upper_items[0].key;

        for item in upper_items {
            other.insert(
                &item.key,
                self.as_leaf()
                    .get_data(&item.key)
                    .expect("'item' must be an existing item"),
            )?;
        }

        let header = self.node.mut_header();
        header.item_count = mid;
        header.data_offset = new_data_offset;

        Ok(other_first_key)
    }
}

impl<'a> TryFrom<NodeMut<'a>> for LeafMut<'a> {
    type Error = Error;

    fn try_from(node: NodeMut<'a>) -> Result<Self> {
        let item_count = node.header().item_count;
        <[LeafItem]>::try_mut_from_prefix_with_elems(
            &mut node.data[HEADER_SIZE..],
            item_count as usize,
        )
        .map_err(|_| Error::Uninterpretable)?;
        Ok(Self { node })
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

const BRANCH_ITEM_SIZE: usize = size_of::<BranchItem>();

/// An item stored in a tree's branch node.
#[repr(C)]
#[derive(Clone, Copy)]
#[derive(FromZeros, IntoBytes, Immutable)]
struct BranchItem {
    key: Key,
    // The block address of the child node
    child: BlockAddr,
}

const LEAF_ITEM_SIZE: usize = size_of::<LeafItem>();

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

type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
enum Error {
    Uninterpretable,
    KeyExists,
    NoSpace,
    KeyNotFound,
}
