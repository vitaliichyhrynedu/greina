use super::*;
use proptest::prelude::*;

type NodeData = Box<[u8; BLOCK_SIZE as usize]>;

fn new_node_data() -> NodeData {
    vec![0u8; BLOCK_SIZE as usize]
        .into_boxed_slice()
        .try_into()
        .unwrap()
}

fn arb_data_type() -> impl Strategy<Value = DataType> {
    prop_oneof![
        Just(DataType::Extent),
        Just(DataType::Node),
        Just(DataType::NodeExtent),
        Just(DataType::Entry),
    ]
}

prop_compose! {
    fn arb_key()(
        obj_id in any::<u64>(),
        data_type in arb_data_type(),
        offset in any::<u64>()
        ) -> Key {
        Key {
            obj_id: obj_id.into(),
            data_type,
            _pad: Default::default(),
            offset: offset.into()
        }
    }
}

fn arb_child() -> impl Strategy<Value = NodePtr> {
    any::<u64>().prop_map(|ptr| NodePtr(ptr.into()))
}

fn arb_branch(upto_pair_count: usize) -> impl Strategy<Value = Branch<NodeData>> {
    let arb_pair = (arb_key(), arb_child());
    let arb_pairs = prop::collection::vec(arb_pair, upto_pair_count);

    arb_pairs.prop_map(|pairs| {
        let data = new_node_data();
        let mut branch = Branch::format(data, 0);
        for (key, val) in pairs {
            let _ = branch.insert(&key, val);
        }
        branch
    })
}

#[derive(Debug)]
enum BranchAction {
    Insert(Key, NodePtr),
    Remove(Key),
    Split(Branch<NodeData>),
    Merge(Branch<NodeData>),
}

fn arb_branch_action<I: Item>() -> impl Strategy<Value = BranchAction> {
    prop_oneof![
        // Insert
        (arb_key(), arb_child()).prop_map(|(key, child)| BranchAction::Insert(key, child)),
        // Remove
        arb_key().prop_map(BranchAction::Remove),
        // Split
        arb_branch(0).prop_map(BranchAction::Split),
        // Merge
        arb_branch(16).prop_map(BranchAction::Merge),
    ]
}

fn arb_data(size: usize) -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..=size)
}

fn arb_leaf(pair_count: usize) -> impl Strategy<Value = Leaf<NodeData>> {
    let arb_pair = (arb_key(), arb_data(BLOCK_SIZE as usize));
    let arb_pairs = prop::collection::vec(arb_pair, pair_count);

    arb_pairs.prop_map(|pairs| {
        let data = new_node_data();
        let mut leaf = Leaf::format(data, 0);
        for (key, val) in pairs {
            let _ = leaf.insert(&key, &val);
        }
        leaf
    })
}

#[derive(Debug)]
enum LeafAction {
    Insert(Key, Vec<u8>),
    Remove(Key),
    Split(Leaf<NodeData>),
    Merge(Leaf<NodeData>),
}

fn arb_leaf_action<I: Item>() -> impl Strategy<Value = LeafAction> {
    prop_oneof![
        // Insert
        (arb_key(), arb_data(BLOCK_SIZE as usize))
            .prop_map(|(key, data)| LeafAction::Insert(key, data)),
        // Remove
        arb_key().prop_map(LeafAction::Remove),
        // Split
        arb_leaf(0).prop_map(LeafAction::Split),
        // Merge
        arb_leaf(16).prop_map(LeafAction::Merge),
    ]
}
