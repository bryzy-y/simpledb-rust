use std::sync::Arc;

use crate::storage::{
    buffer::Frame,
    disk::{DiskRequest, DiskScheduler},
    replacer::LRUKReplacer,
};
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
