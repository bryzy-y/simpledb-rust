use bytemuck::{Pod, Zeroable};

use crate::{
    storage::page::Page,
    types::{PageID, Value},
};

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

    pub fn cell_size(&self) -> u32 {
        self.cell_size
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
    page_id: PageID,
    slot_num: u16,
}

impl RID {
    pub fn new(page_id: PageID, slot_num: u16) -> Self {
        Self { page_id, slot_num }
    }

    pub fn deserialize(buf: &[u8]) -> Self {
        let page_id = PageID::from_le_bytes(buf[..4].try_into().unwrap());
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

impl<'a, T> View<'a, T> {
    pub fn new(page: &'a Page) -> Self {
        Self {
            page,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn header(&self) -> &PageHeader {
        self.page.into()
    }

    fn cell_offset(&self, index: usize) -> Option<usize> {
        let header = self.header();
        if index >= header.max_cells() as usize {
            return None;
        }
        let keys_offset = size_of::<PageHeader>();
        Some(keys_offset + index * header.cell_size() as usize)
    }

    fn end_offset(&self) -> usize {
        let header = self.header();
        let keys_offset = size_of::<PageHeader>();

        keys_offset + header.cells() as usize * header.cell_size() as usize
    }

    pub fn is_full(&self) -> bool {
        let header = self.header();
        header.cells() >= header.max_cells()
    }

    pub fn is_empty(&self) -> bool {
        let header = self.header();
        header.cells() == 0
    }

    pub fn is_leaf(&self) -> bool {
        self.header().is_leaf()
    }

    pub fn cell_at(&self, index: usize) -> Option<&[u8]> {
        let cell_size = self.header().cell_size() as usize;

        self.cell_offset(index).map(|off| {
            let cell_data = &self.page.data()[off..off + cell_size];
            cell_data
        })
    }

    pub fn bisect_right(&self, cmp: impl Fn(&[u8]) -> std::cmp::Ordering) -> usize {
        let header = self.header();

        let (mut lo, mut hi) = match self.is_leaf() {
            true => (0, header.cells()),
            false => (1, header.cells()),
        };

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cell = self.cell_at(mid).unwrap();

            // mid_key <= key
            match cmp(cell) {
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
}

impl<'a, T> Node<'a, T> {
    pub fn new(page: &'a mut Page) -> Self {
        Self {
            page,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn as_view(&'a self) -> View<'a, T> {
        View::new(self.page)
    }

    pub fn header_mut(&mut self) -> &mut PageHeader {
        self.page.into()
    }

    pub fn shift_right(&mut self, cell_index: usize) {
        let view = self.as_view();
        let offset = view.cell_offset(cell_index).unwrap();
        let cell_size = view.header().cell_size() as usize;
        let end_offset = view.end_offset();

        let data = self.page.data_mut();
        data.copy_within(offset..end_offset, offset + cell_size);
    }

    pub fn shift_left(&mut self, cell_index: usize) {
        let view = self.as_view();
        let offset = view.cell_offset(cell_index).unwrap();
        let cell_size = view.header().cell_size() as usize;
        let end_offset = view.end_offset();

        let data = self.page.data_mut();
        data.copy_within(offset + cell_size..end_offset, offset);
    }

    pub fn move_half(&mut self, other: &mut Node<'_, T>) -> usize {
        let header = self.header_mut();
        let total_cells = header.cells() as usize;
        let mid_index = total_cells / 2 as usize;

        let view = self.as_view();
        let offset = view.cell_offset(mid_index).unwrap();
        let end_offset = view.end_offset();
        let other_offset = other.as_view().cell_offset(0).unwrap();

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

    pub fn insert_cell_at(&mut self, index: usize, init: impl Fn(&mut [u8])) {
        // Make room for the new cell
        self.shift_right(index);
        // Initialize cell data using provided function
        let cell_size = self.header_mut().cell_size() as usize;
        let offset = self.as_view().cell_offset(index).unwrap();
        let data = self.page.data_mut();
        let cell_data = &mut data[offset..offset + cell_size];
        init(cell_data);
        // Update cell count
        self.header_mut().incr_cells();
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
}

impl<'a> View<'a, Leaf> {
    pub fn key_at(&self, index: usize) -> Option<&[u8]> {
        let cell = self.cell_at(index)?;
        let key_size = self.header().cell_size() as usize - 6;
        Some(&cell[..key_size])
    }

    pub fn rid_at(&self, index: usize) -> Option<RID> {
        let cell = self.cell_at(index)?;
        let key_size = self.header().cell_size() as usize - 6;
        Some(RID::deserialize(&cell[key_size..key_size + 6]))
    }
}

impl<'a> View<'a, Internal> {
    pub fn key_at(&self, index: usize) -> Option<&[u8]> {
        let cell = self.cell_at(index)?;
        let key_size = self.header().cell_size() as usize - size_of::<PageID>();
        Some(&cell[..key_size])
    }

    pub fn page_id_at(&self, index: usize) -> Option<PageID> {
        let cell = self.cell_at(index)?;
        let key_size = self.header().cell_size() as usize - size_of::<PageID>();
        Some(PageID::from_le_bytes(
            cell[key_size..key_size + 4].try_into().unwrap(),
        ))
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
