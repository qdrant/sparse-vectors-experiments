use crate::sparse_index::common::mmap_ops::{transmute_from_u8_to_mut_slice, transmute_to_u8, transmute_to_u8_slice};
use crate::sparse_index::immutable::posting_list::{PostingElement, PostingList};
use memmap2::{Mmap, MmapMut};
use std::fs::OpenOptions;
use std::io;
use std::mem::size_of;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use crate::sparse_index::common::{madvise, mmap_ops};


pub struct InvertedIndexMmap {
    mmap: Option<Arc<Mmap>>,
    header: InvertedIndexFileHeader,
}

impl InvertedIndexMmap {
    fn load_from_file(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)?;

        let mmap = unsafe { Mmap::map(&file)? };
        madvise::madvise(&mmap, madvise::get_global())?;

        let header = InvertedIndexFileHeader::deserialize_bytes_from(&mmap);

        Ok(Self {
            mmap: Some(Arc::new(mmap)),
            header,
        })
    }

    fn make_index(&self) -> io::Result<&PostingList> {
        let elements = vec![];
        let index = PostingList {
            elements,
        };
        Ok(&index)
    }
}

#[derive(Default, Debug)]
struct InvertedIndexFileHeader {
    pub posting_list_count: u64,
    pub total_posting_count: u64,
}

impl InvertedIndexFileHeader {
    // total size of data in bytes
    pub fn get_data_size(&self) -> u64 {
        size_of::<InvertedIndexFileHeader>() as u64 + // file header
            self.posting_list_count * size_of::<PostingListFileHeader>() as u64 + // all posting list headers
            self.total_posting_count * size_of::<PostingElement>() as u64 // all posting elements
    }

    // size of header in bytes
    pub fn raw_size() -> usize {
        size_of::<u64>() * 2
    }

    pub fn serialize_bytes_to(&self, raw_data: &mut [u8]) {
        let byte_slice = &mut raw_data[0..Self::raw_size()];
        let arr: &mut [u64] = transmute_from_u8_to_mut_slice(byte_slice);
        arr[0] = self.posting_list_count;
        arr[1] = self.total_posting_count;
    }

    pub fn deserialize_bytes_from(raw_data: &[u8]) -> InvertedIndexFileHeader {
        let byte_slice = &raw_data[0..Self::raw_size()];
        let arr: &[u64] = mmap_ops::transmute_from_u8_to_slice(byte_slice);
        InvertedIndexFileHeader {
            posting_list_count: arr[0],
            total_posting_count: arr[1],
        }
    }
}

#[derive(Default)]
struct PostingListFileHeader {
    pub elements_count: u64,
}

impl PostingListFileHeader {
    pub fn get_data_size(&self) -> u64 {
        self.elements_count * size_of::<PostingElement>() as u64
    }
}

struct InvertedIndexConverter {
    path: PathBuf,
    postings: Vec<PostingList>,
}

impl InvertedIndexConverter {
    pub fn new(path: impl Into<PathBuf>, postings: Vec<PostingList>) -> Self {
        Self {
            path: path.into(),
            postings,
        }
    }

    fn get_header(&self) -> InvertedIndexFileHeader {
        InvertedIndexFileHeader {
            posting_list_count: self.postings.len() as u64,
            total_posting_count: self.postings.iter().map(|p| p.elements.len() as u64).sum(),
        }
    }

    pub fn data_size(&self) -> u64 {
        self.get_header().get_data_size()
    }

    pub fn save(&mut self) -> io::Result<()> {
        let temp_path = self.path.with_extension("tmp");
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(temp_path.as_path())?;

            file.set_len(self.data_size())?;
            let m = unsafe { MmapMut::map_mut(&file) };
            let mut mmap = m?;

            self.serialize_to(&mut mmap);

            mmap.flush()?;
        }
        std::fs::rename(temp_path, &self.path)?;

        Ok(())
    }

    pub fn serialize_to(&self, bytes_data: &mut [u8]) {
        // save file header
        let header = self.get_header();
        header.serialize_bytes_to(bytes_data);

        let mut offset = InvertedIndexFileHeader::raw_size();
        for posting in &self.postings {
            let posting_header = PostingListFileHeader {
                elements_count: posting.elements.len() as u64,
            };
            // save posting header
            let posting_header_size = size_of::<PostingListFileHeader>();
            let posting_header_bytes = transmute_to_u8(&posting_header);
            bytes_data[offset..offset + posting_header_size].copy_from_slice(posting_header_bytes);
            offset += posting_header_size;
            // save posting element
            let posting_bytes = transmute_to_u8(&posting);
            bytes_data[offset..offset + posting_bytes.len()].copy_from_slice(posting_bytes);
            offset += posting_bytes.len();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::sparse_index::immutable::inverted_index::InvertedIndexBuilder;
    use crate::sparse_index::immutable::posting_list::PostingList;
    use tempfile::Builder;

    #[test]
    fn test_serialize_to() {
        let inverted_index = InvertedIndexBuilder::new()
            .add(1, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(2, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        let tmp_path = Builder::new()
            .prefix("test_serialize_dir")
            .tempfile()
            .unwrap();
        let mut converter = super::InvertedIndexConverter::new(tmp_path.path(), inverted_index.postings);
        assert!(converter.save().is_ok());
    }
}
