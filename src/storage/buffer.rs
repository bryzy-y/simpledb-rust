use crate::storage::{
    disk::{DiskRequest, DiskScheduler},
    page::{Page, Pager, ReadPageGuard, WritePageGuard},
    replacer::LRUKReplacer,
};
use parking_lot::{Mutex, MutexGuard, RwLock};
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
};

// Trait for buffer pool manager operations needed by database components

pub struct Frame {
    pub page_id: Option<usize>,
    pub frame_id: usize,
    pub is_dirty: bool,
    pub pin_count: AtomicU64,
    pub page: Page,
}

impl Frame {
    pub fn new(frame_id: usize) -> Self {
        Self {
            page_id: None,
            frame_id,
            is_dirty: false,
            pin_count: AtomicU64::new(0),
            page: Page::new(),
        }
    }

    pub fn reset(&mut self) {
        self.page_id = None;
        self.is_dirty = false;
        self.pin_count.store(0, std::sync::atomic::Ordering::SeqCst);
        self.page.data_mut().fill(0);
    }
}

struct PoolState {
    frames: Vec<Arc<RwLock<Frame>>>,
    page_table: HashMap<usize, usize>,
    free_list: Vec<usize>,
}

type PoolStateGuard<'a> = MutexGuard<'a, PoolState>;

impl PoolState {
    pub fn new(pool_size: usize) -> Self {
        let mut frames = Vec::with_capacity(pool_size);
        let mut free_list = Vec::with_capacity(pool_size);
        let page_table = HashMap::with_capacity(pool_size);

        // Initialize frames and free list
        for frame_id in 0..pool_size {
            free_list.push(frame_id);
        }

        for frame_id in 0..pool_size {
            let frame = Arc::new(RwLock::new(Frame::new(frame_id)));
            frames.push(frame);
        }

        Self {
            frames,
            page_table,
            free_list,
        }
    }
}

pub struct BufferPoolManager {
    state: Mutex<PoolState>,
    replacer: Mutex<LRUKReplacer>,
    disk_scheduler: DiskScheduler,
    next_page_id: AtomicU64,
    pool_size: usize,
}

impl BufferPoolManager {
    pub fn new(disk_scheduler: DiskScheduler, pool_size: usize) -> Self {
        let replacer = Mutex::new(LRUKReplacer::new(pool_size, 2));
        let state = Mutex::new(PoolState::new(pool_size));
        let next_page_id = AtomicU64::new(1);

        Self {
            state,
            replacer,
            disk_scheduler,
            next_page_id,
            pool_size,
        }
    }

    fn read_from_disk(&self, state: &PoolStateGuard<'_>, page_id: usize, frame_id: usize) {
        let frame = state.frames[frame_id].clone();
        let read_request = DiskRequest::Read { page_id, frame };
        self.disk_scheduler.schedule(read_request).ok();
    }

    fn get_frame(
        &self,
        mut state: PoolStateGuard<'_>,
        page_id: usize,
    ) -> Option<Arc<RwLock<Frame>>> {
        // Check if page is already in buffer pool
        if let Some(&frame_id) = state.page_table.get(&page_id) {
            let frame = state.frames[frame_id].clone();
            return Some(frame);
        }

        // Page not in buffer pool, try find a free frame
        if let Some(frame_id) = state.free_list.pop() {
            // Load page from disk into a frame
            self.read_from_disk(&state, page_id, frame_id);

            // Update frame and page table
            {
                state.frames[frame_id].write().page_id = Some(page_id);
            }
            state.page_table.insert(page_id, frame_id);
            let frame = state.frames[frame_id].clone();

            return Some(frame);
        }
        // No free frame available, evict one
        // Return read guard
        let target = self.replacer.lock().evict();

        if let Some(frame_id) = target {
            // We can evict this frame, since it is not pinned
            let old_page_id = {
                let frame = state.frames[frame_id].read();
                let old_page_id = frame.page_id;
                debug_assert_eq!(frame.pin_count.load(std::sync::atomic::Ordering::SeqCst), 0);
                debug_assert!(old_page_id.is_some());

                self.flush(old_page_id.unwrap());

                old_page_id.unwrap()
            };

            // Load new page into frame
            self.read_from_disk(&state, page_id, frame_id);
            // Update frame and page table.
            {
                // Can hold the write lock briefly here, since no one else can access this frame
                state.frames[frame_id].write().page_id = Some(page_id);
            }
            state.page_table.remove(&old_page_id);
            state.page_table.insert(page_id, frame_id);

            return Some(state.frames[frame_id].clone());
        }
        // All frames are pinned, cannot evict
        None
    }

    pub fn new_page(&self) -> usize {
        self.next_page_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst) as usize
    }

    pub fn size(&self) -> usize {
        self.pool_size
    }

    // Must get a read only page.
    pub fn get_page(&self, page_id: usize) -> Option<ReadPageGuard<'_>> {
        let state = self.state.lock();
        self.get_frame(state, page_id)
            .map(|frame| ReadPageGuard::new(frame, &self.replacer))
    }

    // Must get a mutable page
    pub fn get_page_mut(&self, page_id: usize) -> Option<WritePageGuard<'_>> {
        let state = self.state.lock();
        self.get_frame(state, page_id)
            .map(|frame| WritePageGuard::new(frame, &self.replacer, &self.disk_scheduler))
    }

    pub fn delete(&self, page_id: usize) -> bool {
        let mut state = self.state.lock();

        if let Some(&frame_id) = state.page_table.get(&page_id) {
            let frame = state.frames[frame_id].clone();
            let pin_count = frame
                .read()
                .pin_count
                .load(std::sync::atomic::Ordering::SeqCst);

            if pin_count > 0 {
                // Cannot delete a pinned page
                return false;
            }
            // Remove from replacer
            self.replacer.lock().remove(frame_id);

            // Reset frame
            {
                let mut frame_guard = frame.write();
                frame_guard.reset();
            }

            // Remove from page table and back to free list
            state.page_table.remove(&page_id);
            // Add frame back to free list
            state.free_list.push(frame_id);
            // Finally remove from disk
            let delete_request = DiskRequest::Delete(page_id);
            self.disk_scheduler.schedule(delete_request).ok();

            return true;
        }
        // Page not found, consider it deleted
        true
    }

    pub fn flush(&self, page_id: usize) -> bool {
        let page = self.get_page_mut(page_id);
        if let Some(mut page) = page {
            page.flush();
            true
        } else {
            false
        }
    }

    pub fn flush_all(&self) {
        let state = self.state.lock();
        for (_, &frame_id) in state.page_table.iter() {
            let frame = state.frames[frame_id].clone();
            WritePageGuard::new(frame, &self.replacer, &self.disk_scheduler).flush();
        }
    }

    pub fn flush_unsafe(&self, page_id: usize) -> bool {
        let state = self.state.lock();
        if let Some(&frame_id) = state.page_table.get(&page_id) {
            unsafe {
                let frame_ptr = state.frames[frame_id].data_ptr();
                // Let's avoid flushing if not dirty
                if !(*frame_ptr).is_dirty {
                    return true;
                }
                let write_request = DiskRequest::WriteUnsafe((*frame_ptr).page.data());
                self.disk_scheduler.schedule(write_request).ok();
            }
            return true;
        }
        false
    }

    pub fn flush_all_unsafe(&self) {
        let state = self.state.lock();
        for (_, &frame_id) in state.page_table.iter() {
            unsafe {
                let frame_ptr = state.frames[frame_id].data_ptr();
                // Let's avoid flushing if not dirty
                if !(*frame_ptr).is_dirty {
                    continue;
                }
                let write_request = DiskRequest::WriteUnsafe((*frame_ptr).page.data());
                self.disk_scheduler.schedule(write_request).ok();
            }
        }
    }

    pub fn pin_count(&self, page_id: usize) -> Option<u64> {
        let page = self.get_page(page_id);
        page.map(|p| p.pin_count())
    }
}

impl Pager for BufferPoolManager {
    type ReadGuard<'a> = ReadPageGuard<'a>;
    type WriteGuard<'a> = WritePageGuard<'a>;

    fn new_page(&self) -> usize {
        self.new_page()
    }

    fn get_page(&self, page_id: usize) -> Option<Self::ReadGuard<'_>> {
        self.get_page(page_id)
    }

    fn get_page_mut(&self, page_id: usize) -> Option<Self::WriteGuard<'_>> {
        self.get_page_mut(page_id)
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, path::Path, thread};

    use crate::config;
    use crate::storage::disk;

    use super::*;

    static PATH: &str = "test.db";

    #[test]
    fn test_buffer_pool_manager() {
        let disk_manager = disk::DiskManager::new(Path::new(PATH)).unwrap();
        let disk_scheduler = disk::DiskScheduler::new(disk_manager);
        let manager = Arc::new(BufferPoolManager::new(disk_scheduler, 10));

        let manager_1 = manager.clone();
        let handle_1 = thread::spawn(move || {
            let pool = manager_1;

            let read_page = pool.get_page(1).unwrap();
            let data = read_page.data();

            assert_eq!(data, &[0u8; config::PAGE_SIZE as usize]);
        });

        let manager_2 = manager.clone();
        let handle_2 = thread::spawn(move || {
            let pool = manager_2;

            {
                let mut write_page = pool.get_page_mut(2).unwrap();
                let mut data = write_page.data();

                data.write(b"Hello World").unwrap();
            }

            {
                let read_page = pool.get_page(2).unwrap();
                let data = read_page.data();

                assert_eq!(&data[0..11], b"Hello World");
            }
        });

        handle_1.join().unwrap();
        handle_2.join().unwrap();
    }
}
