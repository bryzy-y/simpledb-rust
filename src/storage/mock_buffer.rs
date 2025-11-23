use crate::storage::page::{Guard, GuardMut, Page, Pager};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
};

/// A simplified in-memory buffer pool manager for testing.
/// No disk I/O, no eviction, no replacer - just holds pages in memory.
pub struct MockBufferPoolManager {
    pages: Mutex<HashMap<usize, Arc<RwLock<MockFrame>>>>,
    next_page_id: AtomicU64,
}

pub struct MockFrame {
    pub page_id: usize,
    pub page: Page,
}

impl MockFrame {
    fn new(page_id: usize) -> Self {
        Self {
            page_id,
            page: Page::new(),
        }
    }
}

/// Simplified read guard for testing
pub struct MockReadGuard {
    frame: Arc<RwLock<MockFrame>>,
}

impl MockReadGuard {
    pub fn page(&self) -> &Page {
        unsafe {
            let ptr = self.frame.data_ptr();
            &(*ptr).page
        }
    }
}

/// Simplified write guard for testing
pub struct MockWriteGuard {
    frame: Arc<RwLock<MockFrame>>,
}

impl MockWriteGuard {
    pub fn page(&self) -> &Page {
        unsafe {
            let ptr = self.frame.data_ptr();
            &(*ptr).page
        }
    }

    pub fn page_mut(&mut self) -> &mut Page {
        unsafe {
            let ptr = self.frame.data_ptr();
            &mut (*ptr).page
        }
    }
}

impl MockBufferPoolManager {
    pub fn new() -> Self {
        Self {
            pages: Mutex::new(HashMap::new()),
            next_page_id: AtomicU64::new(0),
        }
    }

    pub fn new_page(&self) -> usize {
        let page_id = self
            .next_page_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst) as usize;

        let frame = Arc::new(RwLock::new(MockFrame::new(page_id)));
        self.pages.lock().insert(page_id, frame);

        page_id
    }

    pub fn get_page(&self, page_id: usize) -> Option<MockReadGuard> {
        let pages = self.pages.lock();
        pages.get(&page_id).map(|frame| MockReadGuard {
            frame: frame.clone(),
        })
    }

    pub fn get_page_mut(&self, page_id: usize) -> Option<MockWriteGuard> {
        let pages = self.pages.lock();
        pages.get(&page_id).map(|frame| MockWriteGuard {
            frame: frame.clone(),
        })
    }

    pub fn delete(&self, page_id: usize) -> bool {
        self.pages.lock().remove(&page_id).is_some()
    }

    pub fn size(&self) -> usize {
        self.pages.lock().len()
    }
}

impl Default for MockBufferPoolManager {
    fn default() -> Self {
        Self::new()
    }
}

// Implement BufferPool trait
impl Pager for MockBufferPoolManager {
    type ReadGuard<'a> = MockReadGuard;
    type WriteGuard<'a> = MockWriteGuard;

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

impl Guard for MockReadGuard {
    fn page(&self) -> &Page {
        self.page()
    }
}

impl Guard for MockWriteGuard {
    fn page(&self) -> &Page {
        self.page()
    }
}

impl GuardMut for MockWriteGuard {
    fn page_mut(&mut self) -> &mut Page {
        self.page_mut()
    }
}
