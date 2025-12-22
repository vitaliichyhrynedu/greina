use zerocopy::{Immutable, TryFromBytes};

use crate::block::BlockPtr;

// Node must
pub enum Node {
    Inner {
        //
    },
    Leaf {},
}

#[repr(C)]
#[derive(Clone, Copy)]
#[derive(TryFromBytes, Immutable)]
pub struct Key {
    obj_id: u64,
    item_offset: u16,
}

#[repr(C)]
#[derive(Clone, Copy)]
#[derive(TryFromBytes, Immutable)]
pub enum Item {
    Node {},
    Entry {},
    Extent {},
}
