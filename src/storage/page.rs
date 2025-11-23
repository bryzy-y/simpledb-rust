use std::sync::Arc;

use crate::{
    config,
    storage::{
        buffer::Frame,
        disk::{DiskRequest, DiskScheduler},
        replacer::LRUKReplacer,
    },
    types::PageID,
};
use parking_lot::{self, ArcRwLockReadGuard, ArcRwLockWriteGuard, Mutex, RawRwLock, RwLock};

pub trait Pager {
    type ReadGuard<'a>: Guard
    where
        Self: 'a;
    type WriteGuard<'a>: GuardMut
    where
        Self: 'a;

    fn new_page(&self) -> PageID;
    fn get_page(&self, page_id: PageID) -> Option<Self::ReadGuard<'_>>;
    fn get_page_mut(&self, page_id: PageID) -> Option<Self::WriteGuard<'_>>;
}

pub trait Guard {
    fn page(&self) -> &Page;
}

pub trait GuardMut: Guard {
    fn page_mut(&mut self) -> &mut Page;
}

type ReadGuard = ArcRwLockReadGuard<RawRwLock, Frame>;
type WriteGuard = ArcRwLockWriteGuard<RawRwLock, Frame>;

pub struct ReadPageGuard<'a> {
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
        self.page_guard.page.data()
    }

    pub fn page(&self) -> &Page {
        &self.page_guard.page
    }

    pub fn page_id(&self) -> Option<PageID> {
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

pub struct WritePageGuard<'a> {
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
        self.page_guard.page.data_mut()
    }

    pub fn page(&self) -> &Page {
        &self.page_guard.page
    }

    pub fn page_mut(&mut self) -> &mut Page {
        &mut self.page_guard.page
    }

    pub fn page_id(&self) -> Option<PageID> {
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

impl<'a> Guard for ReadPageGuard<'a> {
    fn page(&self) -> &Page {
        self.page()
    }
}

impl<'a> Guard for WritePageGuard<'a> {
    fn page(&self) -> &Page {
        self.page()
    }
}

impl<'a> GuardMut for WritePageGuard<'a> {
    fn page_mut(&mut self) -> &mut Page {
        self.page_mut()
    }
}

// We need two page types for BTree index. Internal and Leaf pages.
// We will also have a header page that stores the page_id of the root page.
// Use a repr(align(8)) wrapper to ensure proper alignment for the page data
#[repr(align(8))]
struct AlignedData([u8; config::PAGE_SIZE]);

pub struct Page {
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
