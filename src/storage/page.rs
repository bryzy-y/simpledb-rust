use std::sync::Arc;

use crate::{
    config,
    storage::{
        buffer::Frame,
        disk::{DiskRequest, DiskScheduler},
        replacer::LRUKReplacer,
    },
};
use bytemuck::{Pod, Zeroable};
use parking_lot::{self, ArcRwLockReadGuard, ArcRwLockWriteGuard, Mutex, RawRwLock, RwLock};

type ReadGuard = ArcRwLockReadGuard<RawRwLock, Frame>;
type WriteGuard = ArcRwLockWriteGuard<RawRwLock, Frame>;

pub(crate) struct ReadPageGuard<'a> {
    page_guard: ReadGuard,
    replacer: &'a Mutex<LRUKReplacer>,
}

impl<'a> ReadPageGuard<'a> {
    pub fn new(frame: Arc<RwLock<Frame>>, replacer: &'a Mutex<LRUKReplacer>) -> Self {
        let page_guard = frame.read_arc();

        // Increment the pin count
        page_guard
            .pin_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Mark as non-evictable and record access
        {
            let mut replacer = replacer.lock();
            replacer.set_evictable(page_guard.frame_id, false);
            replacer.record_access(page_guard.frame_id);
        }

        Self {
            page_guard,
            replacer,
        }
    }

    pub fn data(&self) -> &[u8] {
        self.page_guard.data.as_slice()
    }

    pub fn page_id(&self) -> Option<usize> {
        self.page_guard.page_id
    }

    pub fn is_dirty(&self) -> bool {
        self.page_guard.is_dirty
    }

    pub fn pin_count(&self) -> u64 {
        self.page_guard
            .pin_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Drop for ReadPageGuard<'_> {
    fn drop(&mut self) {
        let last = self
            .page_guard
            .pin_count
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

        if last == 1 {
            self.replacer
                .lock()
                .set_evictable(self.page_guard.frame_id, true);
        }
    }
}

pub(crate) struct WritePageGuard<'a> {
    page_guard: WriteGuard,
    frame: Arc<RwLock<Frame>>,
    replacer: &'a Mutex<LRUKReplacer>,
    disk_scheduler: &'a DiskScheduler,
}

impl<'a> WritePageGuard<'a> {
    pub fn new(
        frame: Arc<RwLock<Frame>>,
        replacer: &'a Mutex<LRUKReplacer>,
        disk_scheduler: &'a DiskScheduler,
    ) -> Self {
        let page_guard = frame.write_arc();
        // Increment the pin count
        page_guard
            .pin_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Mark as non-evictable and record access
        {
            let mut replacer = replacer.lock();
            replacer.set_evictable(page_guard.frame_id, false);
            replacer.record_access(page_guard.frame_id);
        }

        Self {
            page_guard,
            frame,
            replacer,
            disk_scheduler,
        }
    }

    pub fn data(&mut self) -> &mut [u8] {
        // Mark as dirty. If caller has a write lock, assume they will modify the page.
        self.page_guard.is_dirty = true;
        self.page_guard.data.as_mut_slice()
    }

    pub fn page_id(&self) -> Option<usize> {
        self.page_guard.page_id
    }

    pub fn is_dirty(&self) -> bool {
        self.page_guard.is_dirty
    }

    pub fn pin_count(&self) -> u64 {
        self.page_guard
            .pin_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn flush(&mut self) {
        if !self.is_dirty() || self.page_id().is_none() {
            return;
        }
        let write_request = DiskRequest::Write {
            page_id: self.page_id().unwrap(),
            frame: self.frame.clone(),
        };
        self.disk_scheduler.schedule(write_request).ok();

        // After flushing, mark the page as not dirty
        self.page_guard.is_dirty = false;
    }
}

impl Drop for WritePageGuard<'_> {
    fn drop(&mut self) {
        let last = self
            .page_guard
            .pin_count
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

        // If this was the last pin, mark as evictable
        if last == 1 {
            self.replacer
                .lock()
                .set_evictable(self.page_guard.frame_id, true);
        }
    }
}

// We need two page types for BTree index. Internal and Leaf pages.
// We will also have a header page that stores the page_id of the root page.
#[derive(Zeroable, Clone, Copy)]
#[repr(u32)]
enum PageType {
    Invalid = 0,
    Leaf,
    Internal,
}

// Safety: `PageType` is a simple enum with no padding, so it is safe to treat it as a POD type.
// We control the definition of `PageType`, so we can ensure the "any bit pattern" requirement is met.
unsafe impl Pod for PageType {}

#[derive(Pod, Zeroable, Clone, Copy)]
#[repr(C)]
struct PageHeader {
    page_type: PageType,
    size: u32,
    max_cells: u32,
}

impl PageHeader {
    fn page_type(&self) -> &PageType {
        &self.page_type
    }

    fn is_leaf(&self) -> bool {
        matches!(self.page_type(), PageType::Leaf)
    }

    fn set_page_type(&mut self, page_type: PageType) {
        self.page_type = page_type;
    }

    fn size(&self) -> u32 {
        self.size
    }

    fn set_size(&mut self, size: u32) {
        self.size = size;
    }

    fn adjust_size(&mut self, delta: i32) {
        self.size = self.size.checked_add_signed(delta).unwrap();
    }

    fn max_cells(&self) -> u32 {
        self.max_cells
    }

    fn set_max_cells(&mut self, max_cells: u32) {
        self.max_cells = max_cells;
    }
}

// Use a repr(align(8)) wrapper to ensure proper alignment for the page data
#[repr(align(8))]
struct AlignedData([u8; config::PAGE_SIZE]);

struct Page {
    data: Box<AlignedData>,
}

impl Page {
    pub fn new() -> Self {
        Self {
            data: Box::new(AlignedData([0u8; config::PAGE_SIZE])),
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.data.0
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data.0
    }
}

#[repr(transparent)]
struct LeafPage<K> {
    data: [u8; config::PAGE_SIZE],

    _marker: std::marker::PhantomData<K>,
}

impl<'a, K> TryFrom<&'a Page> for &'a LeafPage<K> {
    type Error = bytemuck::PodCastError;

    fn try_from(value: &'a Page) -> Result<Self, Self::Error> {
        // Safety: `LeafPage` is a transparent wrapper around a byte array of the same size as `Page`.
        // The AlignedPageData ensures proper alignment.
        unsafe { Ok(&*(value.data().as_ptr() as *const LeafPage<K>)) }
    }
}

impl<'a, K> TryFrom<&'a mut Page> for &'a mut LeafPage<K> {
    type Error = bytemuck::PodCastError;

    fn try_from(value: &'a mut Page) -> Result<Self, Self::Error> {
        // Safety: `LeafPage` is a transparent wrapper around a byte array of the same size as `Page`.
        // The AlignedPageData ensures proper alignment.
        unsafe { Ok(&mut *(value.data_mut().as_mut_ptr() as *mut LeafPage<K>)) }
    }
}

trait TypeInfo {
    const SIZE: usize;

    fn encode(&self, buf: &mut [u8]);
    fn decode(buf: &[u8]) -> Self;
    fn compare(&self, other: &Self) -> std::cmp::Ordering;
}

type PageID = u32;

#[derive(Debug, Clone, Copy)]
struct RID {
    page_id: PageID,
    slot_num: u16,
}

impl RID {
    fn new(page_id: PageID, slot_num: u16) -> Self {
        Self { page_id, slot_num }
    }

    fn decode(buf: &[u8]) -> Self {
        let page_id = u32::from_le_bytes(buf[..4].try_into().unwrap());
        let slot_num = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        Self { page_id, slot_num }
    }

    fn encode(&self, buf: &mut [u8]) {
        buf[..4].copy_from_slice(&self.page_id.to_le_bytes());
        buf[4..6].copy_from_slice(&self.slot_num.to_le_bytes());
    }
}

trait LeafCell<K: TypeInfo> {
    fn key(&self) -> K;
    fn rid(&self) -> RID;
}

trait LeafCellMut<K: TypeInfo> {
    fn set_key(&mut self, key: &K);
    fn set_rid(&mut self, rid: &RID);
}

impl<K: TypeInfo> LeafCell<K> for &[u8] {
    fn key(&self) -> K {
        K::decode(&self)
    }

    fn rid(&self) -> RID {
        let start = K::SIZE;
        RID::decode(&self[start..])
    }
}

impl<K: TypeInfo> LeafCellMut<K> for &mut [u8] {
    fn set_key(&mut self, key: &K) {
        key.encode(self);
    }

    fn set_rid(&mut self, rid: &RID) {
        let start = K::SIZE;
        rid.encode(&mut self[start..]);
    }
}

impl<K: TypeInfo> LeafPage<K> {
    pub fn init(&mut self, max_cells: u32) {
        let header = self.header_mut();
        header.set_page_type(PageType::Leaf);
        header.set_size(0);
        header.set_max_cells(max_cells);
    }

    pub fn header(&self) -> &PageHeader {
        let header_bytes = &self.data[..size_of::<PageHeader>()];
        bytemuck::from_bytes(header_bytes)
    }

    pub fn header_mut(&mut self) -> &mut PageHeader {
        let header_bytes = &mut self.data[..size_of::<PageHeader>()];
        bytemuck::from_bytes_mut(header_bytes)
    }

    fn cell_offset(&self, index: usize) -> Option<usize> {
        if index >= self.header().max_cells() as usize {
            return None;
        }
        let cell_size = K::SIZE + size_of::<RID>();
        let keys_offset = size_of::<PageHeader>();

        Some(keys_offset + index * cell_size)
    }

    fn end_offset(&self) -> usize {
        let header = self.header();
        let cell_size = K::SIZE + size_of::<RID>();
        let keys_offset = size_of::<PageHeader>();

        keys_offset + (header.size() as usize) * cell_size
    }

    pub fn cell_at(&self, index: usize) -> Option<impl LeafCell<K> + use<'_, K>> {
        self.cell_offset(index).map(|off| &self.data[off..])
    }

    pub fn cell_at_mut(&mut self, index: usize) -> Option<impl LeafCellMut<K> + use<'_, K>> {
        self.cell_offset(index).map(|off| &mut self.data[off..])
    }

    pub fn set_cell_at(&mut self, index: usize, key: &K, rid: &RID) {
        if let Some(mut cell) = self.cell_at_mut(index) {
            cell.set_key(key);
            cell.set_rid(rid);
        }
    }

    fn insert_index(&self, key: &K) -> usize {
        let header = self.header();
        let (mut lo, mut hi) = (0, header.size() as usize);

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cell = self.cell_at(mid).unwrap();
            let mid_key: K = cell.key();

            if mid_key.compare(key).is_le() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        lo
    }

    pub fn search(&self, key: &K) -> Option<RID> {
        let idx = self.insert_index(key);

        if idx == 0 {
            None
        } else {
            let cell = self.cell_at(idx - 1).unwrap();
            Some(cell.rid())
        }
    }

    pub fn insert(&mut self, key: K, rid: RID) -> Result<(), &'static str> {
        let header = self.header();
        let idx = self.insert_index(&key);

        if idx > 0 && self.cell_at(idx - 1).unwrap().key().compare(&key).is_eq() {
            return Err("Duplicate key insertion is not allowed");
        }

        if idx < header.size() as usize {
            // Shift cells to make space for the new key
            let cell_size = K::SIZE + size_of::<RID>();
            let offset = self.cell_offset(idx).unwrap();
            let end_offset = self.end_offset();

            self.data
                .copy_within(offset..end_offset, offset + cell_size);
        }

        self.set_cell_at(idx, &key, &rid);
        let header = self.header_mut();
        header.adjust_size(1);

        Ok(())
    }

    pub fn delete(&mut self, key: &K) {
        let idx = self.insert_index(key);
        if idx == 0 {
            return;
        }

        let existing_key = self.cell_at(idx - 1).unwrap().key();
        if existing_key.compare(key).is_eq() {
            // Shift cells to remove the key
            let cell_size = K::SIZE + size_of::<RID>();
            let offset = self.cell_offset(idx - 1).unwrap();
            let end_offset = self.end_offset();

            self.data
                .copy_within(offset + cell_size..end_offset, offset);

            // Only adjust size if we actually deleted something
            let header = self.header_mut();
            header.adjust_size(-1);
        }
    }
}

impl TypeInfo for u32 {
    const SIZE: usize = 4;

    fn encode(&self, buf: &mut [u8]) {
        buf[..4].copy_from_slice(&self.to_le_bytes());
    }

    fn decode(buf: &[u8]) -> Self {
        let mut array = [0u8; 4];
        array.copy_from_slice(&buf[..4]);
        u32::from_le_bytes(array)
    }

    fn compare(&self, other: &Self) -> std::cmp::Ordering {
        self.cmp(other)
    }
}

impl TypeInfo for i32 {
    const SIZE: usize = 4;

    fn encode(&self, buf: &mut [u8]) {
        buf[..4].copy_from_slice(&self.to_le_bytes());
    }

    fn decode(buf: &[u8]) -> Self {
        let mut array = [0u8; 4];
        array.copy_from_slice(&buf[..4]);
        i32::from_le_bytes(array)
    }

    fn compare(&self, other: &Self) -> std::cmp::Ordering {
        self.cmp(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_header_initialization() {
        let mut page = Page::new();
        let leaf: &mut LeafPage<u32> = (&mut page).try_into().unwrap();
        leaf.init(100);

        let header = leaf.header();
        assert!(header.is_leaf());
        assert_eq!(header.size(), 0);
        assert_eq!(header.max_cells(), 100);
    }

    #[test]
    fn test_leaf_page_multiple_inserts_sorted() -> Result<(), &'static str> {
        let mut page = Page::new();
        let leaf: &mut LeafPage<u32> = (&mut page).try_into().unwrap();
        leaf.init(100);

        // Insert keys in non-sorted order
        leaf.insert(30, RID::new(3, 30))?;
        leaf.insert(10, RID::new(1, 10))?;
        leaf.insert(50, RID::new(5, 50))?;
        leaf.insert(20, RID::new(2, 20))?;
        leaf.insert(40, RID::new(4, 40))?;

        assert_eq!(leaf.header().size(), 5);

        // Verify they are stored in sorted order
        assert_eq!(leaf.cell_at(0).unwrap().key(), 10);
        assert_eq!(leaf.cell_at(1).unwrap().key(), 20);
        assert_eq!(leaf.cell_at(2).unwrap().key(), 30);
        assert_eq!(leaf.cell_at(3).unwrap().key(), 40);
        assert_eq!(leaf.cell_at(4).unwrap().key(), 50);

        // Verify RIDs match
        assert_eq!(leaf.cell_at(0).unwrap().rid().page_id, 1);
        assert_eq!(leaf.cell_at(2).unwrap().rid().page_id, 3);
        assert_eq!(leaf.cell_at(4).unwrap().rid().page_id, 5);

        Ok(())
    }

    #[test]
    fn test_leaf_page_search_existing() -> Result<(), &'static str> {
        let mut page = Page::new();
        let leaf: &mut LeafPage<u32> = (&mut page).try_into().unwrap();
        leaf.init(100);

        leaf.insert(10, RID::new(1, 10))?;
        leaf.insert(20, RID::new(2, 20))?;
        leaf.insert(30, RID::new(3, 30))?;

        // Search for existing keys
        let result = leaf.search(&20);
        assert!(result.is_some());
        let rid = result.unwrap();
        assert_eq!(rid.page_id, 2);
        assert_eq!(rid.slot_num, 20);

        Ok(())
    }

    #[test]
    fn test_leaf_page_search_missing() {
        let mut page = Page::new();
        let leaf: &mut LeafPage<u32> = (&mut page).try_into().unwrap();
        leaf.init(100);

        leaf.insert(10, RID::new(1, 10)).unwrap();
        leaf.insert(30, RID::new(3, 30)).unwrap();

        // Search for non-existent key
        let result = leaf.search(&20);
        assert!(result.is_some()); // Returns the previous key's RID
        assert_eq!(result.unwrap().page_id, 1);
    }

    #[test]
    fn test_leaf_page_delete() {
        let mut page = Page::new();
        let leaf: &mut LeafPage<u32> = (&mut page).try_into().unwrap();
        leaf.init(100);

        leaf.insert(10, RID::new(1, 10)).unwrap();
        leaf.insert(20, RID::new(2, 20)).unwrap();
        leaf.insert(30, RID::new(3, 30)).unwrap();
        leaf.insert(40, RID::new(4, 40)).unwrap();

        assert_eq!(leaf.header().size(), 4);

        // Delete middle element
        leaf.delete(&20);
        assert_eq!(leaf.header().size(), 3);

        // Verify remaining keys
        assert_eq!(leaf.cell_at(0).unwrap().key(), 10);
        assert_eq!(leaf.cell_at(1).unwrap().key(), 30);
        assert_eq!(leaf.cell_at(2).unwrap().key(), 40);
    }

    #[test]
    fn test_leaf_page_delete_first() {
        let mut page = Page::new();
        let leaf: &mut LeafPage<u32> = (&mut page).try_into().unwrap();
        leaf.init(100);

        leaf.insert(10, RID::new(1, 10)).unwrap();
        leaf.insert(20, RID::new(2, 20)).unwrap();
        leaf.insert(30, RID::new(3, 30)).unwrap();

        leaf.delete(&10);
        assert_eq!(leaf.header().size(), 2);
        assert_eq!(leaf.cell_at(0).unwrap().key(), 20);
        assert_eq!(leaf.cell_at(1).unwrap().key(), 30);
    }

    #[test]
    fn test_leaf_page_delete_last() {
        let mut page = Page::new();
        let leaf: &mut LeafPage<u32> = (&mut page).try_into().unwrap();
        leaf.init(100);

        leaf.insert(10, RID::new(1, 10)).unwrap();
        leaf.insert(20, RID::new(2, 20)).unwrap();
        leaf.insert(30, RID::new(3, 30)).unwrap();

        leaf.delete(&30);
        assert_eq!(leaf.header().size(), 2);
        assert_eq!(leaf.cell_at(0).unwrap().key(), 10);
        assert_eq!(leaf.cell_at(1).unwrap().key(), 20);
    }

    #[test]
    fn test_leaf_page_delete_nonexistent() {
        let mut page = Page::new();
        let leaf: &mut LeafPage<u32> = (&mut page).try_into().unwrap();
        leaf.init(100);

        leaf.insert(10, RID::new(1, 10)).unwrap();
        leaf.insert(30, RID::new(3, 30)).unwrap();

        // Try to delete a key that doesn't exist
        leaf.delete(&20);
        assert_eq!(leaf.header().size(), 2); // Size should remain unchanged
    }

    #[test]
    fn test_leaf_page_insert_duplicate() {
        let mut page = Page::new();
        let leaf: &mut LeafPage<u32> = (&mut page).try_into().unwrap();
        leaf.init(100);

        leaf.insert(10, RID::new(1, 10)).unwrap();
        leaf.insert(20, RID::new(2, 20)).unwrap();

        // Insert duplicate key with different RID
        let result = leaf.insert(20, RID::new(2, 999));

        assert!(result.is_err());
        assert_eq!(leaf.header().size(), 2);

        // Should have 1 entry with key 20
        assert_eq!(leaf.cell_at(1).unwrap().key(), 20);
    }

    #[test]
    fn test_leaf_page_cell_offset() {
        let mut page = Page::new();
        let leaf: &mut LeafPage<u32> = (&mut page).try_into().unwrap();
        leaf.init(2);

        leaf.insert(10, RID::new(1, 10)).unwrap();
        leaf.insert(20, RID::new(2, 20)).unwrap();

        // Verify cell offsets are sequential
        let offset0 = leaf.cell_offset(0).unwrap();
        let offset1 = leaf.cell_offset(1).unwrap();
        let cell_size = u32::SIZE + size_of::<RID>();

        assert_eq!(offset1 - offset0, cell_size);
        // Out of bounds should return None
        assert!(leaf.cell_offset(2).is_none());
    }

    #[test]
    fn test_leaf_page_i32_keys() {
        let mut page = Page::new();
        let leaf: &mut LeafPage<i32> = (&mut page).try_into().unwrap();
        leaf.init(100);

        // Test with negative numbers
        leaf.insert(-10, RID::new(1, 10)).unwrap();
        leaf.insert(0, RID::new(2, 20)).unwrap();
        leaf.insert(10, RID::new(3, 30)).unwrap();
        leaf.insert(-5, RID::new(4, 40)).unwrap();

        assert_eq!(leaf.header().size(), 4);

        // Verify sorted order with negative numbers
        assert_eq!(leaf.cell_at(0).unwrap().key(), -10);
        assert_eq!(leaf.cell_at(1).unwrap().key(), -5);
        assert_eq!(leaf.cell_at(2).unwrap().key(), 0);
        assert_eq!(leaf.cell_at(3).unwrap().key(), 10);
    }

    #[test]
    fn test_leaf_page_empty_search() {
        let mut page = Page::new();
        let leaf_mut: &mut LeafPage<u32> = (&mut page).try_into().unwrap();
        leaf_mut.init(100);

        let leaf: &LeafPage<u32> = (&page).try_into().unwrap();

        // Search in empty page should return None
        assert!(leaf.search(&42).is_none());
    }

    #[test]
    fn test_leaf_page_insert_index() {
        let mut page = Page::new();
        let leaf: &mut LeafPage<u32> = (&mut page).try_into().unwrap();
        leaf.init(100);

        leaf.insert(20, RID::new(2, 20)).unwrap();
        leaf.insert(40, RID::new(4, 40)).unwrap();
        leaf.insert(60, RID::new(6, 60)).unwrap();

        // Verify insert_index finds correct positions
        assert_eq!(leaf.insert_index(&10), 0); // Before all
        assert_eq!(leaf.insert_index(&30), 1); // Between 20 and 40
        assert_eq!(leaf.insert_index(&50), 2); // Between 40 and 60
        assert_eq!(leaf.insert_index(&70), 3); // After all
    }

    #[test]
    fn test_leaf_page_large_dataset() {
        let mut page = Page::new();
        let leaf: &mut LeafPage<u32> = (&mut page).try_into().unwrap();
        leaf.init(500);

        // Insert 100 keys in random order
        let keys: Vec<u32> = (0..100).map(|i| i * 3).rev().collect();
        for &key in &keys {
            leaf.insert(key, RID::new(key, key as u16)).unwrap();
        }

        assert_eq!(leaf.header().size(), 100);

        // Verify all are in sorted order
        for i in 0..100 {
            assert_eq!(leaf.cell_at(i).unwrap().key(), (i as u32) * 3);
        }

        // Delete every other key
        for i in (0..100).step_by(2) {
            leaf.delete(&((i as u32) * 3));
        }

        assert_eq!(leaf.header().size(), 50);

        // Verify remaining keys
        for i in 0..50 {
            let expected_key = ((i * 2 + 1) as u32) * 3;
            assert_eq!(leaf.cell_at(i).unwrap().key(), expected_key);
        }
    }
}
