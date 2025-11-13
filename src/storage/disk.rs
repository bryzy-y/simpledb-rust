use anyhow::{Context, Result, anyhow};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, SendError, SyncSender};

use crate::config;
use crate::storage::buffer::Frame;

#[derive(Default, Debug, Clone, Copy)]
struct Stats {
    num_reads: u32,
    num_writes: u32,
    num_deletes: u32,
}

/*
Database Header is stored in page 0. DiskManager will read it on startup to get page capacity and other metadata.
Structure:
- Bytes 0-3: Magic Number (u32). 0xDEADBEEF
- Bytes 4-7: Database Version (u32)
- Bytes 8-11: Page Capacity (Total Pages allocated) (u32)
- 12-...: Reserved for future use
*/
#[repr(C)]
#[derive(Debug)]
struct DbHeader {
    magic_number: [u8; 4],
    version: u32,
    page_capacity: u32,
}

pub struct DiskManager {
    stats: Stats,
    db_io: File,
    db_header: DbHeader,
    // Map of page IDs to their offsets in the file
    pages: HashMap<usize, usize>,
    // Will hold the list of free page IDs. I won't persist this, but manage it in memory.
    // Once the time comes (if ever) this can be reworked to match what SQLite does with freelist pages.
    free_list: Vec<usize>,
}

pub enum DiskRequest {
    Read {
        page_id: usize,
        frame: Arc<RwLock<Frame>>,
    },
    Write {
        page_id: usize,
        frame: Arc<RwLock<Frame>>,
    },
    WriteUnsafe(*const Vec<u8>),
    Delete(usize),
}

unsafe impl Send for DiskRequest {}

pub struct DiskScheduler {
    sender: SyncSender<DiskRequest>,
}

impl Default for DbHeader {
    fn default() -> Self {
        Self {
            magic_number: [0xDE, 0xAD, 0xBE, 0xEF],
            version: 1,
            page_capacity: config::DEFAULT_DB_FILE_SIZE,
        }
    }
}

impl TryFrom<[u8; std::mem::size_of::<DbHeader>()]> for DbHeader {
    type Error = io::Error;

    fn try_from(data: [u8; std::mem::size_of::<DbHeader>()]) -> Result<Self, Self::Error> {
        let header: DbHeader = unsafe { std::mem::transmute(data) };
        if header.magic_number != [0xDE, 0xAD, 0xBE, 0xEF] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Invalid magic number in DB header - {:?}",
                    header.magic_number
                ),
            ));
        }

        Ok(header)
    }
}

impl From<&DbHeader> for &[u8] {
    fn from(header: &DbHeader) -> Self {
        unsafe {
            std::slice::from_raw_parts(
                (header as *const DbHeader) as *const u8,
                std::mem::size_of::<DbHeader>(),
            )
        }
    }
}

impl DiskManager {
    pub fn new(path: &Path) -> Result<Self> {
        let is_new = !path.exists();

        if is_new {
            let dir = path.parent().unwrap_or(Path::new("."));
            std::fs::create_dir_all(dir)?;

            let db_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;

            // Initialize the database file
            db_file
                .set_len((config::DEFAULT_DB_FILE_SIZE as u64 + 1) * config::PAGE_SIZE as u64)?;

            Ok(Self {
                stats: Stats::default(),
                db_io: db_file,
                pages: HashMap::new(),
                free_list: Vec::new(),
                db_header: DbHeader::default(),
            })
        } else {
            let db_file = OpenOptions::new().read(true).write(true).open(path)?;
            let mut header_bytes = [0u8; std::mem::size_of::<DbHeader>()];
            db_file.read_exact_at(header_bytes.as_mut(), 0)?;

            Ok(Self {
                stats: Stats::default(),
                db_io: db_file,
                pages: HashMap::new(),
                free_list: Vec::new(),
                db_header: DbHeader::try_from(header_bytes)?,
            })
        }
    }

    pub fn file_size(&self) -> Result<u64> {
        Ok(self.db_io.metadata()?.len())
    }

    pub fn write(&mut self, page_id: usize, data: &[u8]) -> Result<()> {
        let offset = match self.pages.get(&page_id) {
            Some(&offset) => offset,
            None => self
                .allocate_page()
                .context("Failed to allocate a new page")?,
        };
        self.db_io
            .seek(io::SeekFrom::Start(offset as u64))
            .with_context(|| format!("Failed to seek at offset {}", offset))?;

        self.db_io.write_all(data)?;

        self.stats.num_writes += 1;
        self.pages.insert(page_id, offset);

        self.db_io.sync_all()?;
        Ok(())
    }

    pub fn read(&mut self, page_id: usize, data: &mut [u8]) -> Result<()> {
        let offset = match self.pages.get(&page_id) {
            Some(&offset) => offset,
            None => self
                .allocate_page()
                .with_context(|| format!("Failed to allocate a new page"))?,
        };
        let file_size = self.file_size().context("Failed to get file length")?;

        if (offset as u64) > file_size {
            return Err(anyhow!("Attempted to read beyond end of file"));
        }
        self.pages.insert(page_id, offset);
        self.db_io
            .seek(io::SeekFrom::Start(offset as u64))
            .with_context(|| format!("Failed to seek the file at offset: {}", offset))?;

        self.db_io
            .read_exact(data)
            .with_context(|| format!("Failed to read the database page at offset: {}", offset))?;

        self.stats.num_reads += 1;
        Ok(())
    }

    pub fn delete_page(&mut self, page_id: usize) -> Result<()> {
        if let Some(_) = self.pages.remove(&page_id) {
            self.free_list.push(page_id);
            self.stats.num_deletes += 1;
        }
        Ok(())
    }

    fn allocate_page(&mut self) -> Result<usize> {
        if !self.free_list.is_empty() {
            let page_id = self.free_list.pop().unwrap();
            return Ok(page_id);
        }
        if self.pages.len() + 1 >= self.db_header.page_capacity as usize {
            // Page capacity increased, need to update header on disk
            self.db_header.page_capacity *= 2;

            self.db_io
                .set_len((self.db_header.page_capacity as u64 + 1) * config::PAGE_SIZE as u64)
                .context("Failed to resize database")?;

            // Write updated header to disk
            let header_bytes: &[u8] = (&self.db_header).into();

            self.db_io.write_all_at(header_bytes, 0)?;
            self.db_io.sync_all()?;
        }

        return Ok(self.pages.len() * config::PAGE_SIZE as usize);
    }
}

impl DiskScheduler {
    pub fn new(disk_manager: DiskManager) -> Self {
        let (sender, receiver) = std::sync::mpsc::sync_channel(0);
        Self::start_worker(receiver, disk_manager);

        Self { sender }
    }

    pub fn schedule(
        &self,
        request: DiskRequest,
    ) -> std::result::Result<(), SendError<DiskRequest>> {
        let _ = self.sender.send(request)?;
        Ok(())
    }

    fn start_worker(receiver: Receiver<DiskRequest>, mut disk_manager: DiskManager) {
        std::thread::spawn(move || {
            while let Ok(request) = receiver.recv() {
                match request {
                    DiskRequest::Read { page_id, frame } => {
                        let mut guard = frame.write();

                        if let Err(e) = disk_manager.read(page_id, guard.data.as_mut_slice()) {
                            eprintln!("Disk read error for page {}: {}", page_id, e);
                            return;
                        }
                    }
                    DiskRequest::Write { page_id, frame } => {
                        let frame = frame.read();
                        if let Err(e) = disk_manager.write(page_id, frame.data.as_slice()) {
                            eprintln!("Disk write error for page {}: {}", page_id, e);
                        }
                    }
                    DiskRequest::Delete(page_id) => {
                        if let Err(e) = disk_manager.delete_page(page_id) {
                            eprintln!("Disk delete error for page {}: {}", page_id, e);
                        }
                    }
                    DiskRequest::WriteUnsafe(ptr) => {
                        let data = unsafe { &*ptr };
                        if let Err(e) = disk_manager.write(0, data.as_slice()) {
                            eprintln!("Disk unsafe write error: {}", e);
                        }
                    }
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_header_serde() -> Result<()> {
        let header_bytes: [u8; _] = [
            0xDE, 0xAD, 0xBE, 0xEF, // Magic Number
            0x01, 0x00, 0x00, 0x00, // Version 1
            0x10, 0x00, 0x00, 0x00, // Page Capacity 16
        ];

        let db_header = DbHeader::try_from(header_bytes)?;
        assert_eq!(db_header.magic_number, [0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(db_header.version, 1);
        assert_eq!(db_header.page_capacity, 16);

        let bytes: &[u8] = (&db_header).into();
        assert_eq!(bytes, &header_bytes);

        Ok(())
    }

    #[test]
    fn test_disk_manager_read_write() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("simpledb.db");

        let mut buf = [0u8; config::PAGE_SIZE as usize];
        let mut data = [0u8; config::PAGE_SIZE as usize];

        let mut disk_manager = DiskManager::new(&db_path)?;

        let mut cursor = io::Cursor::new(&mut data[..]);
        cursor.write_all(b"Test string.").unwrap();

        // Check empty read
        disk_manager.read(0, &mut buf[..])?;

        disk_manager.write(0, &data[..])?;
        disk_manager.read(0, &mut buf[..])?;

        assert_eq!(&buf, &data);

        buf.fill(0);

        disk_manager.write(5, &data[..])?;
        disk_manager.read(5, &mut buf[..])?;

        assert_eq!(&buf, &data);

        Ok(())
    }

    #[test]
    fn test_allocate_page() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("simpledb.db");

        let mut disk_manager = DiskManager::new(&db_path)?;

        assert_eq!(
            disk_manager.db_header.page_capacity,
            config::DEFAULT_DB_FILE_SIZE
        );

        // Now allocate pages until we exceed capacity
        let bytes = vec![255u8; config::PAGE_SIZE as usize];
        for page in 0..(config::DEFAULT_DB_FILE_SIZE + 1) {
            disk_manager.write(page as usize, &bytes)?
        }

        assert_eq!(
            disk_manager.db_header.page_capacity,
            config::DEFAULT_DB_FILE_SIZE * 2
        );

        let mut header = [255u8; std::mem::size_of::<DbHeader>()];
        disk_manager.read(0, header.as_mut_slice())?;

        let db_header = DbHeader::try_from(header)?;
        assert_eq!(db_header.page_capacity, config::DEFAULT_DB_FILE_SIZE * 2);

        Ok(())
    }

    #[test]
    fn test_disk_manager_delete() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("simpledb.db");

        let mut buf = [0u8; config::PAGE_SIZE as usize];
        let mut data = [0u8; config::PAGE_SIZE as usize];
        let mut disk_manager = DiskManager::new(&db_path)?;
        let init_size = disk_manager.file_size()?;

        let mut cursor = io::Cursor::new(&mut data[..]);
        cursor.write_all(b"Test string.")?;

        let mut pages_to_write = 100;
        for page_id in 0..pages_to_write {
            disk_manager.write(page_id, &data)?;
            disk_manager.read(page_id, &mut buf)?;
            assert_eq!(&buf, &data);
        }

        let size_after_write = disk_manager.file_size()?;
        assert!(size_after_write > init_size);

        pages_to_write *= 2;
        for page_id in 0..pages_to_write {
            disk_manager.write(page_id, &data)?;
            disk_manager.read(page_id, &mut buf)?;
            assert_eq!(&buf, &data);

            disk_manager.delete_page(page_id)?;
        }

        // expect no change in file size after delete because we're reclaiming space
        let size_after_delete = disk_manager.file_size()?;
        assert_eq!(size_after_delete, size_after_write);

        Ok(())
    }
}
