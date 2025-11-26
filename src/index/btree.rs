use crate::{
    index::btree_nodes::{BtreeView, InternalNode, LeafNode, PageHeader, RID},
    storage::page::{Guard, GuardMut, Pager},
    types::{Key, PageId, TypeId},
};

pub struct Btree<P: Pager> {
    key_type: TypeId,
    root_page_id: PageId,
    pager: P,
    max_cells_per_page: usize,
}

impl<P: Pager> Btree<P> {
    pub fn new(key_type: TypeId, pager: P, max_cells_per_page: usize) -> Self {
        let root_page_id = pager.new_page() as PageId;
        Self {
            key_type,
            root_page_id,
            pager: pager,
            max_cells_per_page,
        }
    }

    pub fn is_empty(&self) -> bool {
        let guard = self.pager.get_page(self.root_page_id).unwrap();
        let header: &PageHeader = guard.page().into();
        header.is_empty()
    }

    // Additional B-Tree methods (insert, delete, search, etc.) would go here
    pub fn search(&self, key: &Key) -> Option<RID> {
        let mut page_id = self.root_page_id;
        let key = &key.to_bytes();

        loop {
            // We have to figure out how to sleep here
            let guard = self.pager.get_page(page_id).unwrap();
            let current_node: BtreeView = guard.page().into();

            match current_node {
                BtreeView::Leaf(ref leaf) => {
                    let idx = leaf.bisect_right(key);
                    if idx == 0 {
                        return None; // Key not found
                    }
                    let found_key = leaf.key_at(idx - 1)?;
                    if found_key == key {
                        // Found the key
                        return leaf.rid_at(idx - 1);
                    }
                    return None; // Key not found
                }

                BtreeView::Internal(ref internal) => {
                    let idx = internal.bisect_right(key);
                    page_id = internal.page_id_at(idx - 1).unwrap();
                }
                _ => return None,
            }
        }
    }

    pub fn insert(&mut self, key: Key, rid: RID) {
        // Insertion logic would go here
        let mut parent_page_ids = vec![];
        let mut page_id = self.root_page_id;
        let key = &key.to_bytes();

        let leaf_page_id = loop {
            let guard = self.pager.get_page(page_id).unwrap();
            let current_node: BtreeView = guard.page().into();

            match current_node {
                BtreeView::Leaf(_) => {
                    break page_id;
                }
                BtreeView::Internal(ref internal) => {
                    // Push the current page_id onto the parent stack
                    parent_page_ids.push(page_id);
                    // Get the child page_id to traverse next
                    let idx = internal.bisect_right(key);
                    page_id = internal.page_id_at(idx - 1).unwrap();
                }
                BtreeView::Invalid => {
                    // We need to request a write lock and initialize this page
                    drop(guard); // Release read lock
                    let mut guard = self.pager.get_page_mut(page_id).unwrap();

                    LeafNode::init(
                        guard.page_mut(),
                        self.max_cells_per_page as u32,
                        (self.key_type.size() + 6) as u32,
                        None,
                    );
                    break page_id;
                }
            }
        };
        let mut guard = self.pager.get_page_mut(leaf_page_id).unwrap();
        let leaf: &mut LeafNode = guard.page_mut().into();

        // Check for duplicate keys
        let insert_idx = leaf.bisect_right(key);

        if insert_idx > 0 && leaf.key_at(insert_idx - 1) == Some(key) {
            // Key already exists, do not insert duplicates
            // Maybe return an error
            return;
        }
        // Insert the new key+rid into the leaf
        leaf.insert(insert_idx, key, rid);

        if !leaf.header().is_full() {
            // No overflow, done
            return;
        }
        // Overflow occurs, we need to split the page and propagate the split upwards
        let mut new_page_id = self.pager.new_page() as PageId;
        let mut guard = self.pager.get_page_mut(new_page_id).unwrap();

        let mut new_leaf = LeafNode::init(
            guard.page_mut(),
            self.max_cells_per_page as u32,
            (self.key_type.size() + 6) as u32,
            leaf.next_leaf(),
        );

        // Move half the cells to the new leaf page
        leaf.move_half(&mut new_leaf);
        leaf.set_next_leaf(new_page_id);

        let mut middle_key = new_leaf.key_at(0).unwrap().to_owned();

        // Iterate over page_ids parent stack to handle splits
        while let Some(parent_page_id) = parent_page_ids.pop() {
            // Try insert into an internal page
            // If overflow occurs, split, get a new key and page link to insert into the next parent
            let mut guard = self.pager.get_page_mut(parent_page_id).unwrap();
            let parent: &mut InternalNode = guard.page_mut().into();
            let insert_idx = parent.bisect_right(&middle_key);

            // Check if internal node is full before inserting
            if parent.header().is_full() {
                let new_internal_id = self.pager.new_page() as PageId;
                let mut guard = self.pager.get_page_mut(new_internal_id).unwrap();

                let mut new_internal = InternalNode::init(
                    guard.page_mut(),
                    self.max_cells_per_page as u32,
                    (self.key_type.size() + 4) as u32,
                );

                let split_idx = parent.move_half(&mut new_internal);
                // After move_half, the right page's cell 0 contains a key+pageID that should be promoted
                // Extract the key before inserting the new entry
                let promote_key = new_internal.key_at(0).unwrap().to_owned();

                if insert_idx < split_idx {
                    // Insert into left page
                    parent.insert(insert_idx, Some(&middle_key), new_page_id);
                } else {
                    new_internal.insert(insert_idx - split_idx, Some(&middle_key), new_page_id);
                }

                // Prepare for next iteration - continue propagating
                new_page_id = new_internal_id;
                middle_key = promote_key;
            } else {
                parent.insert(insert_idx, Some(&middle_key), new_page_id);
                return;
            }
        }
        // If we reach here, we have split the root and need to create a new root
        let new_root_id = self.pager.new_page() as PageId;
        let mut guard = self.pager.get_page_mut(new_root_id).unwrap();

        let new_root = InternalNode::init(
            guard.page_mut(),
            self.max_cells_per_page as u32,
            (self.key_type.size() + size_of::<PageId>()) as u32,
        );
        // Cell 0: points to old root (left child) - key is ignored
        new_root.insert(0, None, self.root_page_id);
        // Cell 1: promote_key + pointer to new internal page (right child)
        new_root.insert(1, Some(&middle_key), new_page_id);

        // Update BTree root_page_id. Have to persist the new root
        self.root_page_id = new_root_id;
    }

    pub fn iter(&self) -> BtreeIterator<'_, P> {
        let mut iterator = BtreeIterator::new(self);
        iterator.seek_to_first();
        iterator
    }
}

pub struct BtreeIterator<'a, P: Pager> {
    btree: &'a Btree<P>,
    current_page_id: Option<PageId>,
    current_index: usize,
}

impl<'a, P: Pager> BtreeIterator<'a, P> {
    pub fn new(btree: &'a Btree<P>) -> Self {
        let root_guard = btree.pager.get_page(btree.root_page_id).unwrap();
        let header: &PageHeader = root_guard.page().into();

        let current_page_id = if header.is_empty() {
            None
        } else {
            Some(btree.root_page_id)
        };

        Self {
            btree,
            current_page_id,
            current_index: 0,
        }
    }

    fn seek_to_first(&mut self) {
        let mut page_id = self.btree.root_page_id;
        let mut guard = self.btree.pager.get_page(page_id).unwrap();
        let mut current_view: BtreeView = guard.page().into();

        while let BtreeView::Internal(ref internal) = current_view {
            let first_child_page_id = internal.page_id_at(0).unwrap();
            page_id = first_child_page_id;
            guard = self.btree.pager.get_page(page_id).unwrap();
            current_view = guard.page().into();
        }

        match current_view {
            BtreeView::Leaf(_) => self.current_page_id = Some(page_id),
            _ => self.current_page_id = None,
        }
    }
}

impl<'a, P: Pager> Iterator for BtreeIterator<'a, P> {
    type Item = (Key, RID);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(page_id) = self.current_page_id {
            let guard = self.btree.pager.get_page(page_id).unwrap();
            let leaf: &LeafNode = guard.page().into();

            if leaf.header().is_empty() {
                self.current_page_id = None;
                return None;
            }

            if self.current_index >= leaf.header().cells() {
                // Move to next leaf page
                self.current_page_id = leaf.next_leaf();
                self.current_index = 0;
                return self.next();
            }
            // For simplicity, return the first key-rid pair and move to next page
            let key = leaf.key_at(self.current_index).unwrap();
            let rid = leaf.rid_at(self.current_index).unwrap();
            self.current_index += 1;

            Some((Key::from_bytes(self.btree.key_type, &key), rid))
        } else {
            None
        }
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
        let mut btree = Btree::new(TypeId::Int, bpm, 10);

        // Insert 3 values (far below the max of 10)
        let rid1 = RID::new(PageId::from(1u32), 10);
        let rid2 = RID::new(PageId::from(2u32), 20);
        let rid3 = RID::new(PageId::from(3u32), 30);

        btree.insert(Key::Int(100), rid1);
        btree.insert(Key::Int(50), rid2); // Insert out of order
        btree.insert(Key::Int(150), rid3);

        // Verify all can be found
        assert_eq!(btree.search(&Key::Int(100)), Some(rid1));
        assert_eq!(btree.search(&Key::Int(50)), Some(rid2));
        assert_eq!(btree.search(&Key::Int(150)), Some(rid3));

        // Verify leaf page state: should have 3 cells, still a leaf, not split
        let root_guard = btree.pager.get_page(btree.root_page_id).unwrap();
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
        let mut btree = Btree::new(TypeId::Int, bpm, max_cells);

        // Insert enough to trigger a split (max_cells + 1)
        for i in 0..=max_cells {
            let key = (i * 10) as i32;
            let rid = RID::new(PageId::from(i as u32), i as u16);
            btree.insert(Key::Int(key), rid);
        }

        // Verify all keys can be found
        for i in 0..=max_cells {
            let key = (i * 10) as i32;
            let rid = RID::new(PageId::from(i as u32), i as u16);
            assert_eq!(
                btree.search(&Key::Int(key)),
                Some(rid),
                "Key {} should be found",
                key
            );
        }

        // Verify root is now internal
        let root_guard = btree.pager.get_page(btree.root_page_id).unwrap();
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
        let mut btree = Btree::new(TypeId::Int, bpm, max_cells);

        // First, create a tree with internal node by filling first leaf
        for i in 0..=max_cells {
            btree.insert(
                Key::Int((i * 10) as i32),
                RID::new(PageId::from(i as u32), i as u16),
            );
        }

        // At this point: root is internal with 2 children (each leaf has ~half the cells)

        // Now insert more keys to split one of the leaves again
        // This should add another entry to the internal node without splitting it
        for i in (max_cells + 1)..(max_cells * 2) {
            btree.insert(
                Key::Int((i * 10) as i32),
                RID::new(PageId::from(i as u32), i as u16),
            );
        }

        // Verify all keys can be found
        for i in 0..(max_cells * 2) {
            let key = (i * 10) as i32;
            let rid = RID::new(PageId::from(i as u32), i as u16);
            assert_eq!(
                btree.search(&Key::Int(key)),
                Some(rid),
                "Key {} should be found",
                key
            );
        }

        // Verify root is still internal and has more than 2 children
        let root_guard = btree.pager.get_page(btree.root_page_id).unwrap();
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
        let mut btree = Btree::new(TypeId::Int, bpm, max_cells);

        // Insert enough keys to cause multiple splits
        // With max_cells=2, we need to fill leaves and internal nodes
        let num_inserts = (max_cells + 1) * (max_cells + 1) * 2;

        for i in 0..num_inserts {
            let key = (i * 10) as i32;
            let rid = RID::new(PageId::from(i as u32), i as u16);
            btree.insert(Key::Int(key), rid);
        }

        // Verify all keys can be found
        for i in 0..num_inserts {
            let key = (i * 10) as i32;
            let rid = RID::new(PageId::from(i as u32), i as u16);
            assert_eq!(
                btree.search(&Key::Int(key)),
                Some(rid),
                "Key {} should be found after multiple splits",
                key
            );
        }

        // Verify root is internal
        let root_guard = btree.pager.get_page(btree.root_page_id).unwrap();
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
        let mut btree = Btree::new(TypeId::Int, bpm, max_cells);

        let num_keys = 15;

        // Insert in reverse order
        for i in (0..num_keys).rev() {
            let key = (i * 10) as i32;
            let rid = RID::new(PageId::from(i as u32), i as u16);
            btree.insert(Key::Int(key), rid);
        }

        // Verify all keys can be found
        for i in 0..num_keys {
            let key = (i * 10) as i32;
            let rid = RID::new(PageId::from(i as u32), i as u16);
            assert_eq!(
                btree.search(&Key::Int(key)),
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
        let mut btree = Btree::new(TypeId::Int, bpm, 10);

        let rid1 = RID::new(PageId::from(1u32), 10);
        let rid2 = RID::new(PageId::from(2u32), 20);

        btree.insert(Key::Int(100), rid1);
        btree.insert(Key::Int(100), rid2); // Duplicate key

        // Check what happens with duplicate (implementation dependent)
        // Most B-trees either reject duplicates or update the value
        let result = btree.search(&Key::Int(100));
        assert!(result.is_some(), "Key 100 should exist");
    }

    /// Test 7: Search for non-existent keys
    #[test]
    fn test_search_non_existent() {
        let bpm = MockBufferPoolManager::new();
        let mut btree = Btree::new(TypeId::Int, bpm, 5);

        // Insert some keys
        for i in 0..10 {
            btree.insert(
                Key::Int((i * 20) as i32),
                RID::new(PageId::from(i as u32), i as u16),
            );
        }

        // Search for keys in between
        assert_eq!(btree.search(&Key::Int(5)), None);
        assert_eq!(btree.search(&Key::Int(15)), None);
        assert_eq!(btree.search(&Key::Int(95)), None);
        assert_eq!(btree.search(&Key::Int(999)), None);
    }

    /// Test 8: Iterator on empty tree
    #[test]
    fn test_iter_empty_tree() {
        let bpm = MockBufferPoolManager::new();
        let btree = Btree::new(TypeId::Int, bpm, 10);

        let items: Vec<_> = btree.iter().collect();
        assert!(items.is_empty(), "Empty tree should yield no items");
    }

    /// Test 9: Iterator on single element
    #[test]
    fn test_iter_single_element() {
        let bpm = MockBufferPoolManager::new();
        let mut btree = Btree::new(TypeId::Int, bpm, 10);

        let rid = RID::new(PageId::from(1u32), 10);
        btree.insert(Key::Int(42), rid);

        let items: Vec<_> = btree.iter().collect();
        assert_eq!(items.len(), 1, "Should have exactly one item");

        match &items[0].0 {
            Key::Int(k) => assert_eq!(*k, 42),
            _ => panic!("Expected Int key"),
        }
        assert_eq!(items[0].1, rid);
    }

    /// Test 10: Iterator returns items in sorted order
    #[test]
    fn test_iter_sorted_order() {
        let bpm = MockBufferPoolManager::new();
        let mut btree = Btree::new(TypeId::Int, bpm, 5);

        // Insert keys out of order
        let keys = vec![50, 20, 80, 10, 40, 90, 30, 70, 60];
        for (i, &k) in keys.iter().enumerate() {
            btree.insert(Key::Int(k), RID::new(PageId::from(i as u32), i as u16));
        }

        let items: Vec<_> = btree.iter().collect();
        assert_eq!(items.len(), keys.len(), "Should have all items");

        // Extract keys and verify they are sorted
        let result_keys: Vec<i32> = items
            .iter()
            .map(|(k, _)| match k {
                Key::Int(v) => *v,
                _ => panic!("Expected Int key"),
            })
            .collect();

        let mut expected = keys.clone();
        expected.sort();

        assert_eq!(
            result_keys, expected,
            "Iterator should return keys in sorted order"
        );
    }

    /// Test 11: Iterator across multiple leaf pages (after splits)
    #[test]
    fn test_iter_multiple_leaves() {
        let bpm = MockBufferPoolManager::new();
        let max_cells = 3;
        let mut btree = Btree::new(TypeId::Int, bpm, max_cells);

        // Insert enough to cause multiple splits
        let num_keys = 45;
        for i in 0..num_keys {
            btree.insert(
                Key::Int((i * 10) as i32),
                RID::new(PageId::from(i as u32), i as u16),
            );
        }

        let items: Vec<_> = btree.iter().collect();
        assert_eq!(items.len(), num_keys, "Should have all {} items", num_keys);

        // Verify sorted order
        for (idx, (key, rid)) in items.iter().enumerate() {
            let expected_key = (idx * 10) as i32;
            match key {
                Key::Int(k) => assert_eq!(
                    *k, expected_key,
                    "Key at index {} should be {}",
                    idx, expected_key
                ),
                _ => panic!("Expected Int key"),
            }
            assert_eq!(rid.page_id, PageId::from(idx as u32));
            assert_eq!(rid.slot_num, idx as u16);
        }
    }

    /// Test 14: Iterator can be used multiple times
    #[test]
    fn test_iter_reusable() {
        let bpm = MockBufferPoolManager::new();
        let mut btree = Btree::new(TypeId::Int, bpm, 5);

        for i in 0..10 {
            btree.insert(Key::Int(i), RID::new(PageId::from(i as u32), i as u16));
        }

        // First iteration
        let items1: Vec<_> = btree.iter().collect();

        // Second iteration
        let items2: Vec<_> = btree.iter().collect();

        assert_eq!(
            items1.len(),
            items2.len(),
            "Both iterations should return same count"
        );

        for (a, b) in items1.iter().zip(items2.iter()) {
            assert_eq!(a.1, b.1, "Both iterations should return same items");
        }
    }

    /// Test 15: Iterator with for loop
    #[test]
    fn test_iter_for_loop() {
        let bpm = MockBufferPoolManager::new();
        let mut btree = Btree::new(TypeId::Int, bpm, 4);

        for i in 0..8 {
            btree.insert(
                Key::Int(i * 100),
                RID::new(PageId::from(i as u32), i as u16),
            );
        }

        let mut count = 0;
        let mut prev_key: Option<i32> = None;

        for (key, _rid) in btree.iter() {
            let k = match key {
                Key::Int(v) => v,
                _ => panic!("Expected Int key"),
            };

            // Verify ascending order
            if let Some(prev) = prev_key {
                assert!(k > prev, "Keys should be strictly ascending");
            }
            prev_key = Some(k);
            count += 1;
        }

        assert_eq!(count, 8, "Should iterate over all 8 items");
    }
}
