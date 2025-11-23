use std::borrow::{Borrow, BorrowMut};

use bytemuck::{Pod, Zeroable};

use crate::{storage::page::Page, types::PageID};

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

pub struct Invalid;
pub struct Leaf;
pub struct Internal;

pub struct Node<P, T> {
    page: P,
    _marker: std::marker::PhantomData<T>,
}

impl<P, T> Node<P, T> {
    pub fn new(page: P) -> Self {
        Self {
            page,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<P, T> Node<P, T>
where
    P: Borrow<Page>,
{
    pub fn header(&self) -> &PageHeader {
        self.page.borrow().into()
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
            let cell_data = &self.page.borrow().data()[off..off + cell_size];
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

impl<P, T> Node<P, T>
where
    P: BorrowMut<Page>,
{
    pub fn header_mut(&mut self) -> &mut PageHeader {
        self.page.borrow_mut().into()
    }

    pub fn shift_right(&mut self, cell_index: usize) {
        let offset = self.cell_offset(cell_index).unwrap();
        let cell_size = self.header().cell_size() as usize;
        let end_offset = self.end_offset();

        let data = self.page.borrow_mut().data_mut();
        data.copy_within(offset..end_offset, offset + cell_size);
    }

    pub fn shift_left(&mut self, cell_index: usize) {
        let offset = self.cell_offset(cell_index).unwrap();
        let cell_size = self.header().cell_size() as usize;
        let end_offset = self.end_offset();

        let data = self.page.borrow_mut().data_mut();
        data.copy_within(offset + cell_size..end_offset, offset);
    }

    pub fn move_half(&mut self, other: &mut Node<P, T>) -> usize {
        let header = self.header();
        let total_cells = header.cells() as usize;
        let mid_index = total_cells / 2 as usize;

        let offset = self.cell_offset(mid_index).unwrap();
        let end_offset = self.end_offset();
        let other_offset = other.cell_offset(0).unwrap();

        let data = self.page.borrow_mut().data_mut();
        let other_data = other.page.borrow_mut().data_mut();
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
        let cell_size = self.header().cell_size() as usize;
        let offset = self.cell_offset(index).unwrap();
        let data = self.page.borrow_mut().data_mut();
        let cell_data = &mut data[offset..offset + cell_size];
        init(cell_data);
        // Update cell count
        self.header_mut().incr_cells();
    }
}

impl<P> Node<P, Leaf>
where
    P: BorrowMut<Page>,
{
    pub fn init(mut page: P, max_cells: u32, cell_size: u32) -> Self {
        let header: &mut PageHeader = page.borrow_mut().into();
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

impl<P> Node<P, Internal>
where
    P: BorrowMut<Page>,
{
    pub fn init(mut page: P, max_cells: u32, cell_size: u32) -> Self {
        let header: &mut PageHeader = page.borrow_mut().into();
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

impl<P> Node<P, Invalid>
where
    P: BorrowMut<Page>,
{
    fn init_leaf(self, max_cells: u32, cell_size: u32) -> Node<P, Leaf> {
        Node::<P, Leaf>::init(self.page, max_cells, cell_size)
    }

    fn init_internal(self, max_cells: u32, cell_size: u32) -> Node<P, Internal> {
        Node::<P, Internal>::init(self.page, max_cells, cell_size)
    }
}

pub type LeafNode<P> = Node<P, Leaf>;
pub type InternalNode<P> = Node<P, Internal>;

pub enum BtreeNode<P> {
    Leaf(LeafNode<P>),
    Internal(InternalNode<P>),
    Invalid(Node<P, Invalid>),
}

impl<'a> From<&'a Page> for BtreeNode<&'a Page> {
    fn from(value: &'a Page) -> Self {
        let header: &PageHeader = value.into();
        match header.page_type() {
            PageType::Leaf => {
                let node = Node::new(value);
                BtreeNode::Leaf(node)
            }
            PageType::Internal => {
                let node = Node::new(value);
                BtreeNode::Internal(node)
            }
            PageType::Invalid => {
                let node = Node::new(value);
                BtreeNode::Invalid(node)
            }
        }
    }
}

impl<'a> From<&'a mut Page> for BtreeNode<&'a mut Page> {
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
            PageType::Invalid => {
                let node = Node::new(value);
                BtreeNode::Invalid(node)
            }
        }
    }
}
