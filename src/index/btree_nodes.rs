use std::num::NonZeroU32;

use bytemuck::{Pod, Zeroable};

use crate::{storage::page::Page, types::PageId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
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

#[derive(Pod, Zeroable, Clone, Copy)]
#[repr(C)]
pub struct LeafPageHeader {
    base: PageHeader,
    next_leaf: Option<NonZeroU32>,
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

    pub fn is_full(&self) -> bool {
        self.cells >= self.max_cells
    }

    pub fn is_empty(&self) -> bool {
        self.cells == 0
    }

    pub fn set_page_type(&mut self, page_type: PageType) {
        self.page_type = page_type as u32;
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

impl LeafPageHeader {
    pub fn cells(&self) -> usize {
        self.base.cells()
    }

    pub fn is_full(&self) -> bool {
        self.base.is_full()
    }

    pub fn is_empty(&self) -> bool {
        self.base.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RID {
    pub page_id: PageId,
    pub slot_num: u16,
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

#[repr(transparent)]
pub struct LeafNode {
    data: [u8; 4096],
}

#[repr(transparent)]
pub struct InternalNode {
    data: [u8; 4096],
}

impl From<&Page> for &LeafNode {
    fn from(value: &Page) -> Self {
        unsafe { &*(value.data() as *const [u8] as *const LeafNode) }
    }
}

impl From<&Page> for &InternalNode {
    fn from(value: &Page) -> Self {
        unsafe { &*(value.data() as *const [u8] as *const InternalNode) }
    }
}

impl From<&mut Page> for &mut LeafNode {
    fn from(value: &mut Page) -> Self {
        unsafe { &mut *(value.data_mut() as *mut [u8] as *mut LeafNode) }
    }
}

impl From<&mut Page> for &mut InternalNode {
    fn from(value: &mut Page) -> Self {
        unsafe { &mut *(value.data_mut() as *mut [u8] as *mut InternalNode) }
    }
}

trait Node {
    fn data(&self) -> &[u8];
    fn data_mut(&mut self) -> &mut [u8];

    fn base_header(&self) -> &PageHeader;
    fn base_header_mut(&mut self) -> &mut PageHeader;

    fn cell_offset(&self, index: usize) -> Option<usize> {
        let header = self.base_header();
        if index >= header.max_cells() as usize {
            return None;
        }
        let keys_offset = if header.is_leaf() {
            size_of::<LeafPageHeader>()
        } else {
            size_of::<PageHeader>()
        };

        Some(keys_offset + index * header.cell_size() as usize)
    }

    fn end_offset(&self) -> usize {
        let header = self.base_header();
        let keys_offset = if header.is_leaf() {
            size_of::<LeafPageHeader>()
        } else {
            size_of::<PageHeader>()
        };

        keys_offset + header.cells() as usize * header.cell_size() as usize
    }

    fn cell_at(&self, index: usize) -> Option<&[u8]> {
        let cell_size = self.base_header().cell_size() as usize;

        self.cell_offset(index).map(|off| {
            let cell_data = &self.data()[off..off + cell_size];
            cell_data
        })
    }

    fn bisect_right(&self, key: &[u8]) -> usize {
        let header = self.base_header();

        let (mut lo, mut hi) = match header.is_leaf() {
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

    fn cell_at_mut(&mut self, index: usize) -> Option<&mut [u8]> {
        let cell_size = self.base_header().cell_size() as usize;

        self.cell_offset(index).map(|off| {
            let cell_data = &mut self.data_mut()[off..off + cell_size];
            cell_data
        })
    }

    fn shift_right(&mut self, cell_index: usize) {
        let offset = self.cell_offset(cell_index).unwrap();
        let cell_size = self.base_header().cell_size() as usize;
        let end_offset = self.end_offset();

        let data = self.data_mut();
        data.copy_within(offset..end_offset, offset + cell_size);
    }

    fn shift_left(&mut self, cell_index: usize) {
        let offset = self.cell_offset(cell_index).unwrap();
        let cell_size = self.base_header().cell_size() as usize;
        let end_offset = self.end_offset();

        let data = self.data_mut();
        data.copy_within(offset + cell_size..end_offset, offset);
    }

    fn move_half(&mut self, other: &mut impl Node) -> usize {
        let header = self.base_header_mut();
        let total_cells = header.cells() as usize;
        let mid_index = total_cells / 2 as usize;

        let offset = self.cell_offset(mid_index).unwrap();
        let end_offset = self.end_offset();
        let other_offset = other.cell_offset(0).unwrap();

        let data = self.data_mut();
        let other_data = other.data_mut();
        // Move the second half of the cells to the other page
        let src = &data[offset..end_offset];
        let dst = &mut other_data[other_offset..other_offset + src.len()];
        dst.copy_from_slice(src);

        // Update sizes
        let self_header = self.base_header_mut();
        let other_header = other.base_header_mut();

        self_header.cells = mid_index as u32;
        other_header.cells = (total_cells - mid_index) as u32;

        mid_index
    }
}

impl Node for LeafNode {
    fn data(&self) -> &[u8] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn base_header(&self) -> &PageHeader {
        let header_bytes = &self.data[..size_of::<PageHeader>()];
        bytemuck::from_bytes(header_bytes)
    }

    fn base_header_mut(&mut self) -> &mut PageHeader {
        let header_bytes = &mut self.data[..size_of::<PageHeader>()];
        bytemuck::from_bytes_mut(header_bytes)
    }
}

impl Node for InternalNode {
    fn data(&self) -> &[u8] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn base_header(&self) -> &PageHeader {
        let header_bytes = &self.data[..size_of::<PageHeader>()];
        bytemuck::from_bytes(header_bytes)
    }

    fn base_header_mut(&mut self) -> &mut PageHeader {
        let header_bytes = &mut self.data[..size_of::<PageHeader>()];
        bytemuck::from_bytes_mut(header_bytes)
    }
}

impl InternalNode {
    pub fn init(page: &mut Page, max_cells: u32, cell_size: u32) -> &mut Self {
        let header: &mut PageHeader = page.into();
        header.max_cells = max_cells;
        header.cell_size = cell_size;
        header.cells = 0;
        header.set_page_type(PageType::Internal);

        page.into()
    }

    pub fn bisect_right(&self, key: &[u8]) -> usize {
        Node::bisect_right(self, key)
    }

    pub fn header(&self) -> &PageHeader {
        self.base_header()
    }

    pub fn key_at(&self, index: usize) -> Option<&[u8]> {
        let cell = self.cell_at(index)?;
        let key_size = self.base_header().cell_size() - 4;
        Some(&cell[..key_size])
    }

    pub fn page_id_at(&self, index: usize) -> Option<PageId> {
        let cell = self.cell_at(index)?;
        let key_size = self.base_header().cell_size() - size_of::<PageId>();
        Some(PageId::from_le_bytes(
            cell[key_size..key_size + 4].try_into().unwrap(),
        ))
    }

    pub fn insert(&mut self, index: usize, key: Option<&[u8]>, page_id: PageId) {
        if let Some(key_val) = key {
            self.shift_right(index);
            // Initialize cell data using provided function
            let cell_size = self.base_header_mut().cell_size() as usize;
            let offset = self.cell_offset(index).unwrap();
            let cell_data = &mut self.data[offset..offset + cell_size];

            // Serialize key and page_id into cell
            cell_data[..key_val.len()].copy_from_slice(key_val);
            cell_data[key_val.len()..key_val.len() + 4].copy_from_slice(&page_id.to_le_bytes());
        } else {
            // Special case for first pointer (no key)
            let cell_size = self.base_header_mut().cell_size() as usize;
            let key_size = cell_size - size_of::<PageId>();

            let cell = self.cell_at_mut(0).unwrap();
            cell[key_size..key_size + 4].copy_from_slice(&page_id.to_le_bytes());
        }

        // Update cell count
        let header = self.base_header_mut();
        header.cells += 1;
    }

    pub fn move_half(&mut self, other: &mut InternalNode) -> usize {
        Node::move_half(self, other)
    }
}

impl LeafNode {
    pub fn init(
        page: &mut Page,
        max_cells: u32,
        cell_size: u32,
        next_leaf: Option<PageId>,
    ) -> &mut Self {
        let header: &mut LeafPageHeader =
            bytemuck::from_bytes_mut(&mut page.data_mut()[..size_of::<LeafPageHeader>()]);

        header.base.max_cells = max_cells;
        header.base.cell_size = cell_size;
        header.base.cells = 0;
        header.base.set_page_type(PageType::Leaf);
        header.next_leaf = next_leaf.and_then(|id| NonZeroU32::new(id.as_u32()));

        page.into()
    }

    pub fn next_leaf(&self) -> Option<PageId> {
        let header = self.header();
        header.next_leaf.map(|nz| PageId::new(nz.get()))
    }

    pub fn set_next_leaf(&mut self, next_leaf: PageId) {
        let header_bytes = &mut self.data[..size_of::<LeafPageHeader>()];
        let header: &mut LeafPageHeader = bytemuck::from_bytes_mut(header_bytes);
        header.next_leaf = NonZeroU32::new(next_leaf.as_u32());
    }

    pub fn bisect_right(&self, key: &[u8]) -> usize {
        Node::bisect_right(self, key)
    }

    pub fn header(&self) -> &LeafPageHeader {
        let header_bytes = &self.data[..size_of::<LeafPageHeader>()];
        bytemuck::from_bytes(header_bytes)
    }

    pub fn key_at(&self, index: usize) -> Option<&[u8]> {
        let cell = self.cell_at(index)?;
        let key_size = self.base_header().cell_size() - 6;
        Some(&cell[..key_size])
    }

    pub fn rid_at(&self, index: usize) -> Option<RID> {
        let cell = self.cell_at(index)?;
        let key_size = self.base_header().cell_size() - 6;
        Some(RID::deserialize(&cell[key_size..key_size + 6]))
    }

    pub fn move_half(&mut self, other: &mut LeafNode) -> usize {
        Node::move_half(self, other)
    }

    pub fn insert(&mut self, index: usize, key: &[u8], rid: RID) {
        self.shift_right(index);
        // Initialize cell data using provided function
        let cell_size = self.base_header_mut().cell_size() as usize;
        let offset = self.cell_offset(index).unwrap();
        let cell_data = &mut self.data[offset..offset + cell_size];

        // Serialize key and rid into cell
        cell_data[..key.len()].copy_from_slice(key);
        rid.serialize(&mut cell_data[key.len()..key.len() + 6]);

        // Update cell count
        let header = self.base_header_mut();
        header.cells += 1;
    }
}

pub enum BtreeView<'a> {
    Leaf(&'a LeafNode),
    Internal(&'a InternalNode),
    Invalid,
}

pub enum BtreeNode<'a> {
    Leaf(&'a mut LeafNode),
    Internal(&'a mut InternalNode),
    Invalid,
}

impl<'a> From<&'a Page> for BtreeView<'a> {
    fn from(value: &'a Page) -> Self {
        let header: &PageHeader = value.into();
        match header.page_type() {
            PageType::Leaf => BtreeView::Leaf(value.into()),
            PageType::Internal => BtreeView::Internal(value.into()),
            PageType::Invalid => BtreeView::Invalid,
        }
    }
}

impl<'a> From<&'a mut Page> for BtreeNode<'a> {
    fn from(value: &'a mut Page) -> Self {
        let header: &mut PageHeader = value.into();
        match header.page_type() {
            PageType::Leaf => BtreeNode::Leaf(value.into()),
            PageType::Internal => BtreeNode::Internal(value.into()),
            PageType::Invalid => BtreeNode::Invalid,
        }
    }
}
