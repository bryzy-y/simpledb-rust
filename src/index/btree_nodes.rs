use bytemuck::{Pod, Zeroable};

use crate::{storage::page::Page, types::PageId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)] // We need it to be u32, because padding.
pub enum PageType {
    Invalid = 0,
    Leaf = 1,
    Internal = 2,
}

#[derive(Pod, Zeroable, Clone, Copy)]
#[repr(C)]
pub struct PageHeader {
    page_type: u32,
    cell_size: u32,
    cells: u32,
    max_cells: u32,
}

impl PageHeader {
    pub fn page_type(&self) -> PageType {
        match self.page_type {
            0 => PageType::Invalid,
            1 => PageType::Leaf,
            2 => PageType::Internal,
            _ => panic!("Invalid page type"),
        }
    }

    pub fn cell_size(&self) -> usize {
        self.cell_size as usize
    }

    pub fn cells(&self) -> usize {
        self.cells as usize
    }

    pub fn max_cells(&self) -> usize {
        self.max_cells as usize
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self.page_type(), PageType::Leaf)
    }

    pub fn set_page_type(&mut self, page_type: PageType) {
        self.page_type = page_type as u32;
    }

    pub fn adjust_cells(&mut self, delta: i32) {
        self.cells = self.cells.checked_add_signed(delta).unwrap();
    }

    pub fn incr_cells(&mut self) {
        self.cells += 1;
    }
}

impl<'a> From<&'a Page> for &'a PageHeader {
    fn from(value: &'a Page) -> Self {
        let data = value.data();
        let header_bytes = &data[..size_of::<PageHeader>()];
        bytemuck::from_bytes(header_bytes)
    }
}

impl<'a> From<&'a mut Page> for &'a mut PageHeader {
    fn from(value: &'a mut Page) -> Self {
        let data = value.data_mut();
        let header_bytes = &mut data[..size_of::<PageHeader>()];
        bytemuck::from_bytes_mut(header_bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RID {
    page_id: PageId,
    slot_num: u16,
}

impl RID {
    pub fn new(page_id: PageId, slot_num: u16) -> Self {
        Self { page_id, slot_num }
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        let page_id = PageId::from_le_bytes(buf[..4].try_into().unwrap());
        let slot_num = u16::from_le_bytes(buf[4..6].try_into().unwrap());
        Self { page_id, slot_num }
    }

    pub fn serialize(&self, buf: &mut [u8]) {
        buf[..4].copy_from_slice(&self.page_id.to_le_bytes());
        buf[4..6].copy_from_slice(&self.slot_num.to_le_bytes());
    }
}

pub struct Leaf;
pub struct Internal;

pub struct View<'a, T> {
    page: &'a Page,
    _marker: std::marker::PhantomData<T>,
}

pub struct Node<'a, T> {
    page: &'a mut Page,
    _marker: std::marker::PhantomData<T>,
}

trait CommonNodeOps {
    fn page(&self) -> &Page;
    fn base_header(&self) -> &PageHeader;

    fn cell_offset(&self, index: usize) -> Option<usize> {
        let header = self.base_header();
        if index >= header.max_cells() as usize {
            return None;
        }
        let keys_offset = size_of::<PageHeader>();
        Some(keys_offset + index * header.cell_size() as usize)
    }

    fn end_offset(&self) -> usize {
        let header = self.base_header();
        let keys_offset = size_of::<PageHeader>();

        keys_offset + header.cells() as usize * header.cell_size() as usize
    }

    fn cell_at(&self, index: usize) -> Option<&[u8]> {
        let cell_size = self.base_header().cell_size() as usize;

        self.cell_offset(index).map(|off| {
            let cell_data = &self.page().data()[off..off + cell_size];
            cell_data
        })
    }

    fn is_full(&self) -> bool {
        let header = self.base_header();
        header.cells() >= header.max_cells()
    }

    fn is_empty(&self) -> bool {
        let header = self.base_header();
        header.cells() == 0
    }

    fn is_leaf(&self) -> bool {
        self.base_header().is_leaf()
    }

    fn bisect_right(&self, key: &[u8]) -> usize {
        let header = self.base_header();

        let (mut lo, mut hi) = match self.is_leaf() {
            true => (0, header.cells()),
            false => (1, header.cells()),
        };

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cell = self.cell_at(mid).unwrap();
            let mid_key = &cell[..key.len()];

            // mid_key <= key
            match mid_key.cmp(key) {
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {
                    lo = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    hi = mid;
                }
            }
        }
        lo
    }

    fn key_at(&self, index: usize) -> Option<&[u8]> {
        let cell = self.cell_at(index)?;
        let key_size = self.base_header().cell_size() - if self.is_leaf() { 6 } else { 4 };
        Some(&cell[..key_size])
    }

    fn page_id_at(&self, index: usize) -> Option<PageId> {
        let cell = self.cell_at(index)?;
        let key_size = self.base_header().cell_size() - size_of::<PageId>();
        Some(PageId::from_le_bytes(
            cell[key_size..key_size + 4].try_into().unwrap(),
        ))
    }

    fn rid_at(&self, index: usize) -> Option<RID> {
        let cell = self.cell_at(index)?;
        let key_size = self.base_header().cell_size() - 6;
        Some(RID::deserialize(&cell[key_size..key_size + 6]))
    }
}

impl<'a, T> CommonNodeOps for View<'a, T> {
    fn page(&self) -> &'a Page {
        self.page
    }

    fn base_header(&self) -> &PageHeader {
        self.page.into()
    }
}

impl<'a, T> CommonNodeOps for Node<'a, T> {
    fn page(&self) -> &Page {
        self.page
    }

    fn base_header(&self) -> &PageHeader {
        (&*self.page).into()
    }
}

impl<'a, T> View<'a, T> {
    fn new(page: &'a Page) -> Self {
        Self {
            page,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn header(&self) -> &PageHeader {
        self.page.into()
    }

    pub fn bisect_right(&self, key: &[u8]) -> usize {
        CommonNodeOps::bisect_right(self, key)
    }

    pub fn is_full(&self) -> bool {
        CommonNodeOps::is_full(self)
    }

    pub fn is_empty(&self) -> bool {
        CommonNodeOps::is_empty(self)
    }

    pub fn is_leaf(&self) -> bool {
        CommonNodeOps::is_leaf(self)
    }
}

impl<'a> View<'a, Leaf> {
    pub fn key_at(&self, index: usize) -> Option<&[u8]> {
        CommonNodeOps::key_at(self, index)
    }

    pub fn rid_at(&self, index: usize) -> Option<RID> {
        CommonNodeOps::rid_at(self, index)
    }
}

impl<'a> View<'a, Internal> {
    pub fn key_at(&self, index: usize) -> Option<&[u8]> {
        CommonNodeOps::key_at(self, index)
    }

    pub fn page_id_at(&self, index: usize) -> Option<PageId> {
        CommonNodeOps::page_id_at(self, index)
    }
}

impl<'a, T> Node<'a, T> {
    pub fn new(page: &'a mut Page) -> Self {
        Self {
            page,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn header_mut(&mut self) -> &mut PageHeader {
        self.page.into()
    }

    pub fn bisect_right(&self, key: &[u8]) -> usize {
        CommonNodeOps::bisect_right(self, key)
    }

    pub fn is_full(&self) -> bool {
        CommonNodeOps::is_full(self)
    }

    pub fn is_empty(&self) -> bool {
        CommonNodeOps::is_empty(self)
    }

    pub fn is_leaf(&self) -> bool {
        CommonNodeOps::is_leaf(self)
    }

    fn cell_at_mut(&mut self, index: usize) -> Option<&mut [u8]> {
        let cell_size = self.base_header().cell_size() as usize;

        self.cell_offset(index).map(|off| {
            let cell_data = &mut self.page.data_mut()[off..off + cell_size];
            cell_data
        })
    }

    fn shift_right(&mut self, cell_index: usize) {
        let offset = self.cell_offset(cell_index).unwrap();
        let cell_size = self.base_header().cell_size() as usize;
        let end_offset = self.end_offset();

        let data = self.page.data_mut();
        data.copy_within(offset..end_offset, offset + cell_size);
    }

    fn shift_left(&mut self, cell_index: usize) {
        let offset = self.cell_offset(cell_index).unwrap();
        let cell_size = self.base_header().cell_size() as usize;
        let end_offset = self.end_offset();

        let data = self.page.data_mut();
        data.copy_within(offset + cell_size..end_offset, offset);
    }

    pub fn move_half(&mut self, other: &mut Node<'_, T>) -> usize {
        let header = self.header_mut();
        let total_cells = header.cells() as usize;
        let mid_index = total_cells / 2 as usize;

        let offset = self.cell_offset(mid_index).unwrap();
        let end_offset = self.end_offset();
        let other_offset = other.cell_offset(0).unwrap();

        let data = self.page.data_mut();
        let other_data = other.page.data_mut();
        // Move the second half of the cells to the other page
        let src = &data[offset..end_offset];
        let dst = &mut other_data[other_offset..other_offset + src.len()];
        dst.copy_from_slice(src);

        // Update sizes
        let self_header = self.header_mut();
        let other_header = other.header_mut();

        self_header.cells = mid_index as u32;
        other_header.cells = (total_cells - mid_index) as u32;

        mid_index
    }
}

impl<'a> Node<'a, Leaf> {
    pub fn init(page: &'a mut Page, max_cells: u32, cell_size: u32) -> Self {
        let header: &mut PageHeader = page.into();
        header.max_cells = max_cells;
        header.cell_size = cell_size;
        header.cells = 0;
        header.set_page_type(PageType::Leaf);

        Self {
            page,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn insert(&mut self, index: usize, key: &[u8], rid: RID) {
        assert!(
            key.len() + 6 == self.header_mut().cell_size(),
            "Key size does not expected cell size"
        );

        self.shift_right(index);
        // Initialize cell data using provided function
        let cell_size = self.header_mut().cell_size() as usize;
        let offset = self.cell_offset(index).unwrap();
        let cell_data = &mut self.page.data_mut()[offset..offset + cell_size];

        // Serialize key and rid into cell
        cell_data[..key.len()].copy_from_slice(key);
        rid.serialize(&mut cell_data[key.len()..key.len() + 6]);

        // Update cell count
        let header = self.header_mut();
        header.cells += 1;
    }

    pub fn key_at(&self, index: usize) -> Option<&[u8]> {
        CommonNodeOps::key_at(self, index)
    }

    pub fn rid_at(&self, index: usize) -> Option<RID> {
        CommonNodeOps::rid_at(self, index)
    }
}

impl<'a> Node<'a, Internal> {
    pub fn init(page: &'a mut Page, max_cells: u32, cell_size: u32) -> Self {
        let header: &mut PageHeader = page.into();
        header.max_cells = max_cells;
        header.cell_size = cell_size;
        header.cells = 0;
        header.set_page_type(PageType::Internal);

        Self {
            page,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn insert(&mut self, index: usize, key: Option<&[u8]>, page_id: PageId) {
        if let Some(key_val) = key {
            self.shift_right(index);
            // Initialize cell data using provided function
            let cell_size = self.header_mut().cell_size() as usize;
            let offset = self.cell_offset(index).unwrap();
            let cell_data = &mut self.page.data_mut()[offset..offset + cell_size];

            // Serialize key and page_id into cell
            cell_data[..key_val.len()].copy_from_slice(key_val);
            cell_data[key_val.len()..key_val.len() + 4].copy_from_slice(&page_id.to_le_bytes());
        } else {
            // Special case for first pointer (no key)
            let cell_size = self.header_mut().cell_size() as usize;
            let key_size = cell_size - size_of::<PageId>();

            let cell = self.cell_at_mut(0).unwrap();
            cell[key_size..key_size + 4].copy_from_slice(&page_id.to_le_bytes());
        }

        // Update cell count
        let header = self.header_mut();
        header.cells += 1;
    }

    pub fn key_at(&self, index: usize) -> Option<&[u8]> {
        CommonNodeOps::key_at(self, index)
    }

    pub fn page_id_at(&self, index: usize) -> Option<PageId> {
        CommonNodeOps::page_id_at(self, index)
    }
}

pub enum BtreeView<'a> {
    Leaf(View<'a, Leaf>),
    Internal(View<'a, Internal>),
    Invalid,
}

pub enum BtreeNode<'a> {
    Leaf(Node<'a, Leaf>),
    Internal(Node<'a, Internal>),
    Invalid,
}

impl<'a> From<&'a Page> for BtreeView<'a> {
    fn from(value: &'a Page) -> Self {
        let header: &PageHeader = value.into();
        match header.page_type() {
            PageType::Leaf => {
                let node = View::new(value);
                BtreeView::Leaf(node)
            }
            PageType::Internal => {
                let node = View::new(value);
                BtreeView::Internal(node)
            }
            PageType::Invalid => BtreeView::Invalid,
        }
    }
}

impl<'a> From<&'a mut Page> for BtreeNode<'a> {
    fn from(value: &'a mut Page) -> Self {
        let header: &mut PageHeader = value.into();
        match header.page_type() {
            PageType::Leaf => {
                let node = Node::new(value);
                BtreeNode::Leaf(node)
            }
            PageType::Internal => {
                let node = Node::new(value);
                BtreeNode::Internal(node)
            }
            PageType::Invalid => BtreeNode::Invalid,
        }
    }
}
