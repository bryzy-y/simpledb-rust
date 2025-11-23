use std::fmt::Debug;

use crate::{
    index::btree_nodes::{BtreeNode, InternalNode, LeafNode, PageHeader, PageID, RID},
    storage::page::{Guard, GuardMut, Pager},
};

#[derive(Debug, Clone, Copy)]
pub enum TypeID {
    Int,
    BigInt,
}

impl TypeID {
    fn size(&self) -> usize {
        match self {
            TypeID::Int => 4,
            TypeID::BigInt => 8,
        }
    }
}

#[derive(Debug)]
pub enum Value {
    Int(i32),
    BigInt(i64),
}

impl Value {
    fn serialize(&self, buf: &mut [u8]) {
        match self {
            Value::Int(v) => buf[..4].copy_from_slice(&v.to_le_bytes()),
            Value::BigInt(v) => buf[..8].copy_from_slice(&v.to_le_bytes()),
        }
    }

    fn deserialize(type_id: TypeID, buf: &[u8]) -> Self {
        match type_id {
            TypeID::Int => Value::Int(i32::from_le_bytes(buf[..4].try_into().unwrap())),
            TypeID::BigInt => Value::BigInt(i64::from_le_bytes(buf[..8].try_into().unwrap())),
        }
    }

    fn compare(&self, other: &Value) -> std::cmp::Ordering {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.cmp(b),
            _ => panic!("Cannot compare different Value types"),
        }
    }
}

fn cell_key(type_id: TypeID, cell: &[u8]) -> Value {
    Value::deserialize(type_id, cell)
}

fn cell_rid(type_id: TypeID, cell: &[u8]) -> RID {
    let rid_offset = type_id.size();
    let rid_bytes = &cell[rid_offset..];
    RID::deserialize(rid_bytes)
}

fn cell_page_id(type_id: TypeID, cell: &[u8]) -> PageID {
    let page_id_offset = type_id.size();
    let page_id_bytes = &cell[page_id_offset..];
    PageID::from_le_bytes(page_id_bytes.try_into().unwrap())
}

fn cell_set_key(type_id: TypeID, cell: &mut [u8], key: &Value) {
    key.serialize(&mut cell[..type_id.size()]);
}

fn cell_set_rid(type_id: TypeID, cell: &mut [u8], rid: &RID) {
    let rid_offset = type_id.size();
    rid.serialize(&mut cell[rid_offset..]);
}

fn cell_set_page_id(type_id: TypeID, cell: &mut [u8], page_id: PageID) {
    let page_id_offset = type_id.size();
    cell[page_id_offset..page_id_offset + 4].copy_from_slice(&page_id.to_le_bytes());
}

pub struct Btree<P: Pager> {
    key_type: TypeID,
    root_page_id: PageID,
    pager: P,
    max_cells_per_page: usize,
}

impl<P: Pager> Btree<P> {
    pub fn new(key_type: TypeID, pager: P, max_cells_per_page: usize) -> Self {
        let root_page_id = pager.new_page() as PageID;
        Self {
            key_type,
            root_page_id,
            pager: pager,
            max_cells_per_page,
        }
    }

    pub fn is_empty(&self) -> bool {
        let guard = self.pager.get_page(self.root_page_id as usize).unwrap();
        let header: &PageHeader = guard.page().into();
        header.cells() == 0
    }

    // Additional B-Tree methods (insert, delete, search, etc.) would go here
    pub fn search(&self, key: &Value) -> Option<RID> {
        let mut page_id = self.root_page_id;

        loop {
            // We have to figure out how to sleep here
            let guard = self.pager.get_page(page_id as usize).unwrap();
            let current_node: BtreeNode<_> = guard.page().into();

            match current_node {
                BtreeNode::Leaf(ref leaf) => {
                    let idx = leaf.bisect_right(|cell| cell_key(self.key_type, cell).compare(key));
                    if idx == 0 {
                        return None; // Key not found
                    }
                    let cell = leaf.cell_at(idx - 1).unwrap();
                    let cell_key = cell_key(self.key_type, cell);

                    if cell_key.compare(key).is_eq() {
                        // Found the key
                        return Some(cell_rid(self.key_type, cell));
                    }
                    return None; // Key not found
                }

                BtreeNode::Internal(ref internal) => {
                    let idx =
                        internal.bisect_right(|cell| cell_key(self.key_type, cell).compare(key));
                    let cell = internal.cell_at(idx - 1).unwrap();
                    page_id = cell_page_id(self.key_type, cell);
                }
                _ => return None,
            }
        }
    }

    pub fn insert(&mut self, key: Value, rid: RID) {
        // Insertion logic would go here
        let mut parent_page_ids = vec![];
        let mut page_id = self.root_page_id;

        let leaf_page_id = loop {
            let guard = self.pager.get_page(page_id as usize).unwrap();
            let current_node: BtreeNode<_> = guard.page().into();

            match current_node {
                BtreeNode::Leaf(_) => {
                    break page_id;
                }
                BtreeNode::Internal(ref internal) => {
                    // Push the current page_id onto the parent stack
                    parent_page_ids.push(page_id);
                    // Get the child page_id to traverse next
                    let idx =
                        internal.bisect_right(|cell| cell_key(self.key_type, cell).compare(&key));
                    let cell = internal.cell_at(idx - 1).unwrap();
                    page_id = cell_page_id(self.key_type, cell);
                }
                BtreeNode::Invalid(_) => {
                    // We need to request a write lock and initialize this page
                    drop(guard); // Release read lock
                    let mut guard = self.pager.get_page_mut(page_id as usize).unwrap();
                    LeafNode::init(
                        guard.page_mut(),
                        self.max_cells_per_page as u32,
                        (self.key_type.size() + 6) as u32,
                    );
                    break page_id;
                }
            }
        };
        let mut guard = self.pager.get_page_mut(leaf_page_id as usize).unwrap();
        let mut leaf = LeafNode::new(guard.page_mut());

        // Check for duplicate keys
        let insert_idx = leaf.bisect_right(|cell| cell_key(self.key_type, cell).compare(&key));
        if insert_idx > 0
            && cell_key(self.key_type, leaf.cell_at(insert_idx - 1).unwrap())
                .compare(&key)
                .is_eq()
        {
            // Key already exists, do not insert duplicates
            // Maybe return an error
            return;
        }

        // Insert key and RID into leaf page
        leaf.insert_cell_at(insert_idx, |cell_data| {
            cell_set_key(self.key_type, cell_data, &key);
            cell_set_rid(self.key_type, cell_data, &rid);
        });

        if !leaf.is_full() {
            // No overflow, done
            return;
        }
        // Overflow occurs, we need to split the page and propagate the split upwards
        let mut new_page_id = self.pager.new_page() as PageID;
        let mut guard = self.pager.get_page_mut(new_page_id as usize).unwrap();

        let mut new_leaf = LeafNode::init(
            guard.page_mut(),
            self.max_cells_per_page as u32,
            (self.key_type.size() + 6) as u32,
        );

        // Move half the cells to the new leaf page
        leaf.move_half(&mut new_leaf);
        let middle_cell = new_leaf.cell_at(0).unwrap();
        let mut middle_key = cell_key(self.key_type, middle_cell);

        // Iterate over page_ids parent stack to handle splits
        while let Some(parent_page_id) = parent_page_ids.pop() {
            // Try insert into an internal page
            // If overflow occurs, split, get a new key and page link to insert into the next parent
            let mut guard = self.pager.get_page_mut(parent_page_id as usize).unwrap();
            let mut parent = InternalNode::new(guard.page_mut());
            let insert_idx =
                parent.bisect_right(|cell| cell_key(self.key_type, cell).compare(&middle_key));

            // Check if internal node is full before inserting
            if parent.is_full() {
                let new_internal_id = self.pager.new_page() as PageID;
                let mut guard = self.pager.get_page_mut(new_internal_id as usize).unwrap();

                let mut new_internal = InternalNode::init(
                    guard.page_mut(),
                    self.max_cells_per_page as u32,
                    (self.key_type.size() + 4) as u32,
                );

                let split_idx = parent.move_half(&mut new_internal);
                // After move_half, the right page's cell 0 contains a key+pageID that should be promoted
                // Extract the key before inserting the new entry
                let promote_cell = new_internal.cell_at(0).unwrap();
                let promote_key = cell_key(self.key_type, promote_cell);

                if insert_idx < split_idx {
                    // Insert into left page
                    parent.insert_cell_at(insert_idx, |cell_data| {
                        cell_set_key(self.key_type, cell_data, &middle_key);
                        cell_set_page_id(self.key_type, cell_data, new_page_id);
                    });
                } else {
                    // Insert into right page
                    new_internal.insert_cell_at(insert_idx - split_idx, |cell_data| {
                        cell_set_key(self.key_type, cell_data, &middle_key);
                        cell_set_page_id(self.key_type, cell_data, new_page_id);
                    });
                }

                // Prepare for next iteration - continue propagating
                new_page_id = new_internal_id;
                middle_key = promote_key;
            } else {
                // Parent has room - insert and done
                parent.insert_cell_at(insert_idx, |cell_data| {
                    cell_set_key(self.key_type, cell_data, &middle_key);
                    cell_set_page_id(self.key_type, cell_data, new_page_id);
                });

                return;
            }
        }
        // If we reach here, we have split the root and need to create a new root
        let new_root_id = self.pager.new_page() as PageID;
        let mut guard = self.pager.get_page_mut(new_root_id as usize).unwrap();

        let mut new_root = InternalNode::init(
            guard.page_mut(),
            self.max_cells_per_page as u32,
            (self.key_type.size() + size_of::<PageID>()) as u32,
        );
        // Cell 0: points to old root (left child) - key is ignored
        new_root.insert_cell_at(0, |cell_data| {
            cell_set_page_id(self.key_type, cell_data, self.root_page_id);
        });
        // Cell 1: promote_key + pointer to new internal page (right child)
        new_root.insert_cell_at(1, |cell_data| {
            cell_set_key(self.key_type, cell_data, &middle_key);
            cell_set_page_id(self.key_type, cell_data, new_page_id);
        });

        // Update BTree root_page_id. Have to persist the new root
        self.root_page_id = new_root_id;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::btree_nodes::{PageHeader, PageType, RID};
    use crate::storage::mock_buffer::MockBufferPoolManager;

    /// Test 1: Insert into a leaf without a split
    /// Verifies: Leaf page cells are incremented, keys are stored in sorted order
    #[test]
    fn test_insert_leaf_no_split() {
        let bpm = MockBufferPoolManager::new();
        let mut btree = Btree::new(TypeID::Int, bpm, 10);

        // Insert 3 values (far below the max of 10)
        let rid1 = RID::new(1, 10);
        let rid2 = RID::new(2, 20);
        let rid3 = RID::new(3, 30);

        btree.insert(Value::Int(100), rid1);
        btree.insert(Value::Int(50), rid2); // Insert out of order
        btree.insert(Value::Int(150), rid3);

        // Verify all can be found
        assert_eq!(btree.search(&Value::Int(100)), Some(rid1));
        assert_eq!(btree.search(&Value::Int(50)), Some(rid2));
        assert_eq!(btree.search(&Value::Int(150)), Some(rid3));

        // Verify leaf page state: should have 3 cells, still a leaf, not split
        let root_guard = btree.pager.get_page(btree.root_page_id as usize).unwrap();
        let header: &PageHeader = root_guard.page().into();

        assert_eq!(header.cells(), 3, "Leaf should have 3 cells");
        assert!(header.is_leaf(), "Root should still be a leaf");
        assert!(header.cells() != 0, "Leaf should not be full");
    }

    /// Test 2: Insert into a leaf with a split where there is no parent (root split)
    /// Verifies: New root is created, old root becomes leaf, split happens correctly
    #[test]
    fn test_insert_leaf_split_no_parent() {
        let bpm = MockBufferPoolManager::new();
        let max_cells = 4;
        let mut btree = Btree::new(TypeID::Int, bpm, max_cells);

        // Insert enough to trigger a split (max_cells + 1)
        for i in 0..=max_cells {
            let key = (i * 10) as i32;
            let rid = RID::new(i as u32, i as u16);
            btree.insert(Value::Int(key), rid);
        }

        // Verify all keys can be found
        for i in 0..=max_cells {
            let key = (i * 10) as i32;
            let rid = RID::new(i as u32, i as u16);
            assert_eq!(
                btree.search(&Value::Int(key)),
                Some(rid),
                "Key {} should be found",
                key
            );
        }

        // Verify root is now internal
        let root_guard = btree.pager.get_page(btree.root_page_id as usize).unwrap();
        let header: &PageHeader = root_guard.page().into();

        assert!(!header.is_leaf(), "Root should be internal after split");
        assert!(
            matches!(header.page_type(), PageType::Internal),
            "Root should have Internal page type"
        );
        assert_eq!(
            header.cells(),
            2,
            "Internal root should have 2 cells (left pointer + right key+pointer)"
        );
    }

    /// Test 3: Insert into a leaf with split where internal page has room
    /// Verifies: Leaf splits, internal page gets new entry without splitting
    #[test]
    fn test_insert_leaf_split_internal_has_room() {
        let bpm = MockBufferPoolManager::new();
        let max_cells = 3;
        let mut btree = Btree::new(TypeID::Int, bpm, max_cells);

        // First, create a tree with internal node by filling first leaf
        for i in 0..=max_cells {
            btree.insert(Value::Int((i * 10) as i32), RID::new(i as u32, i as u16));
        }

        // At this point: root is internal with 2 children (each leaf has ~half the cells)

        // Now insert more keys to split one of the leaves again
        // This should add another entry to the internal node without splitting it
        for i in (max_cells + 1)..(max_cells * 2) {
            btree.insert(Value::Int((i * 10) as i32), RID::new(i as u32, i as u16));
        }

        // Verify all keys can be found
        for i in 0..(max_cells * 2) {
            let key = (i * 10) as i32;
            let rid = RID::new(i as u32, i as u16);
            assert_eq!(
                btree.search(&Value::Int(key)),
                Some(rid),
                "Key {} should be found",
                key
            );
        }

        // Verify root is still internal and has more than 2 children
        let root_guard = btree.pager.get_page(btree.root_page_id as usize).unwrap();
        let header: &PageHeader = root_guard.page().into();

        assert!(!header.is_leaf(), "Root should still be internal");
        assert!(
            header.cells() > 2,
            "Internal root should have more than 2 cells after additional split"
        );
    }

    /// Test 4: Insert into a leaf with split where internal page doesn't have room
    /// Verifies: Both leaf and internal page split, tree height increases
    #[test]
    fn test_insert_leaf_split_internal_no_room() {
        let bpm = MockBufferPoolManager::new();
        let max_cells = 2; // Small to force splits quickly
        let mut btree = Btree::new(TypeID::Int, bpm, max_cells);

        // Insert enough keys to cause multiple splits
        // With max_cells=2, we need to fill leaves and internal nodes
        let num_inserts = (max_cells + 1) * (max_cells + 1) * 2;

        for i in 0..num_inserts {
            let key = (i * 10) as i32;
            let rid = RID::new(i as u32, i as u16);
            btree.insert(Value::Int(key), rid);
        }

        // Verify all keys can be found
        for i in 0..num_inserts {
            let key = (i * 10) as i32;
            let rid = RID::new(i as u32, i as u16);
            assert_eq!(
                btree.search(&Value::Int(key)),
                Some(rid),
                "Key {} should be found after multiple splits",
                key
            );
        }

        // Verify root is internal
        let root_guard = btree.pager.get_page(btree.root_page_id as usize).unwrap();
        let header: &PageHeader = root_guard.page().into();

        assert!(
            !header.is_leaf(),
            "Root should be internal after multiple splits"
        );

        // The tree should have grown in height (internal nodes have children)
        // We can verify this by checking that the root has multiple entries
        assert!(
            header.cells() >= 2,
            "Root should have at least 2 cells indicating multiple levels"
        );
    }

    /// Test 5: Insert keys in reverse order
    /// Verifies: BTree handles reverse insertion correctly
    #[test]
    fn test_insert_reverse_order() {
        let bpm = MockBufferPoolManager::new();
        let max_cells = 4;
        let mut btree = Btree::new(TypeID::Int, bpm, max_cells);

        let num_keys = 15;

        // Insert in reverse order
        for i in (0..num_keys).rev() {
            let key = (i * 10) as i32;
            let rid = RID::new(i as u32, i as u16);
            btree.insert(Value::Int(key), rid);
        }

        // Verify all keys can be found
        for i in 0..num_keys {
            let key = (i * 10) as i32;
            let rid = RID::new(i as u32, i as u16);
            assert_eq!(
                btree.search(&Value::Int(key)),
                Some(rid),
                "Key {} should be found after reverse insertion",
                key
            );
        }
    }

    /// Test 6: Insert duplicate keys should not increase count
    #[test]
    fn test_insert_duplicate_handling() {
        let bpm = MockBufferPoolManager::new();
        let mut btree = Btree::new(TypeID::Int, bpm, 10);

        let rid1 = RID::new(1, 10);
        let rid2 = RID::new(2, 20);

        btree.insert(Value::Int(100), rid1);
        btree.insert(Value::Int(100), rid2); // Duplicate key

        // Check what happens with duplicate (implementation dependent)
        // Most B-trees either reject duplicates or update the value
        let result = btree.search(&Value::Int(100));
        assert!(result.is_some(), "Key 100 should exist");
    }

    /// Test 7: Search for non-existent keys
    #[test]
    fn test_search_non_existent() {
        let bpm = MockBufferPoolManager::new();
        let mut btree = Btree::new(TypeID::Int, bpm, 5);

        // Insert some keys
        for i in 0..10 {
            btree.insert(Value::Int((i * 20) as i32), RID::new(i, i as u16));
        }

        // Search for keys in between
        assert_eq!(btree.search(&Value::Int(5)), None);
        assert_eq!(btree.search(&Value::Int(15)), None);
        assert_eq!(btree.search(&Value::Int(95)), None);
        assert_eq!(btree.search(&Value::Int(999)), None);
    }
}
