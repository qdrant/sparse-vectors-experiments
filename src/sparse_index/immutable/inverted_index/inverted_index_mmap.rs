use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::sparse_index::common::file_operations::{atomic_save_json, read_json};
use crate::sparse_index::common::madvise;
use memmap2::{Mmap, MmapMut};
use serde::{Deserialize, Serialize};

use super::inverted_index_ram::InvertedIndexRam;
use crate::sparse_index::common::mmap_ops::{
    transmute_from_u8_to_slice, transmute_to_u8, transmute_to_u8_slice,
};
use crate::sparse_index::common::types::DimId;
use crate::sparse_index::immutable::posting_list::PostingElement;

const POSTING_HEADER_SIZE: usize = size_of::<PostingListFileHeader>();
const INDEX_FILE_NAME: &str = "index.data";
const INDEX_CONFIG_FILE_NAME: &str = "index_config.json";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct InvertedIndexFileHeader {
    pub posting_count: usize,
}

/// Inverted flatten index from dimension id to posting list
pub struct InvertedIndexMmap {
    mmap: Arc<Mmap>,
    file_header: InvertedIndexFileHeader,
}

#[derive(Default, Clone)]
struct PostingListFileHeader {
    pub start_offset: u64,
    pub end_offset: u64,
}

impl InvertedIndexMmap {
    pub fn index_file_path(path: &Path) -> PathBuf {
        path.join(INDEX_FILE_NAME)
    }

    pub fn index_config_file_path(path: &Path) -> PathBuf {
        path.join(INDEX_CONFIG_FILE_NAME)
    }

    pub fn get(&self, id: &DimId) -> Option<&[PostingElement]> {
        if *id > self.file_header.posting_count as DimId {
            return None;
        }

        let header = transmute_from_u8::<PostingListFileHeader>(
            &self.mmap
                [*id as usize * POSTING_HEADER_SIZE..(*id as usize + 1) * POSTING_HEADER_SIZE],
        )
        .clone();
        let elements_bytes = &self.mmap[header.start_offset as usize..header.end_offset as usize];
        Some(transmute_from_u8_to_slice(elements_bytes))
    }

    pub fn convert_and_save<P: AsRef<Path>>(
        inverted_index_ram: &InvertedIndexRam,
        path: P,
    ) -> std::io::Result<Self> {
        let (total_posting_headers_size, total_posting_elements_size) =
            Self::calculate_file_length(inverted_index_ram);
        let file_length = total_posting_headers_size + total_posting_elements_size;
        let file_path = Self::index_file_path(path.as_ref());
        Self::create_and_ensure_length(file_path.as_ref(), file_length)?;

        let mut mmap = Self::open_write_mmap(file_path.as_ref())?;
        madvise::madvise(&mmap, madvise::get_global())?;

        // file index data
        Self::save_posting_headers(&mut mmap, inverted_index_ram, total_posting_headers_size);
        Self::save_posting_elements(&mut mmap, inverted_index_ram, total_posting_headers_size);

        let posting_count = inverted_index_ram.postings.len();

        // finalize data with index file.
        let file_header = InvertedIndexFileHeader { posting_count };
        let config_file_path = Self::index_config_file_path(path.as_ref());
        atomic_save_json(&config_file_path, &file_header)?;

        Ok(Self {
            mmap: Arc::new(mmap.make_read_only()?),
            file_header,
        })
    }

    pub fn load<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file_path = Self::index_file_path(path.as_ref());
        let mmap = Self::open_read_mmap(file_path.as_ref())?;
        madvise::madvise(&mmap, madvise::get_global())?;
        // read from index file
        let config_file_path = Self::index_config_file_path(path.as_ref());
        // if the file header does not exist, the index is malformed
        let file_header: InvertedIndexFileHeader = read_json(&config_file_path)?;
        Ok(Self {
            mmap: Arc::new(mmap),
            file_header,
        })
    }

    /// Calculate file length in bytes
    /// Returns (posting headers size, posting elements size)
    fn calculate_file_length(inverted_index_ram: &InvertedIndexRam) -> (usize, usize) {
        let total_posting_headers_size = inverted_index_ram.postings.len() * POSTING_HEADER_SIZE;

        let mut total_posting_elements_size = 0;
        for posting in &inverted_index_ram.postings {
            total_posting_elements_size += posting.elements.len() * size_of::<PostingElement>();
        }

        (total_posting_headers_size, total_posting_elements_size)
    }

    fn save_posting_headers(
        mmap: &mut MmapMut,
        inverted_index_ram: &InvertedIndexRam,
        total_posting_headers_size: usize,
    ) {
        let mut elements_offset: usize = total_posting_headers_size;
        for (id, posting) in inverted_index_ram.postings.iter().enumerate() {
            let posting_elements_size = posting.elements.len() * size_of::<PostingElement>();
            let posting_header = PostingListFileHeader {
                start_offset: elements_offset as u64,
                end_offset: (elements_offset + posting_elements_size) as u64,
            };
            elements_offset = posting_header.end_offset as usize;

            // save posting header
            let posting_header_bytes = transmute_to_u8(&posting_header);
            let start_posting_offset = id * POSTING_HEADER_SIZE;
            let end_posting_offset = (id + 1) * POSTING_HEADER_SIZE;
            mmap[start_posting_offset..end_posting_offset].copy_from_slice(posting_header_bytes);
        }
    }

    fn save_posting_elements(
        mmap: &mut MmapMut,
        inverted_index_ram: &InvertedIndexRam,
        total_posting_headers_size: usize,
    ) {
        let mut offset = total_posting_headers_size;
        for posting in &inverted_index_ram.postings {
            // save posting element
            let posting_elements_bytes = transmute_to_u8_slice(&posting.elements);
            mmap[offset..offset + posting_elements_bytes.len()]
                .copy_from_slice(posting_elements_bytes);
            offset += posting_elements_bytes.len();
        }
    }

    fn open_read_mmap(path: &Path) -> std::io::Result<Mmap> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(false)
            .append(true)
            .create(true)
            .open(path)?;
        unsafe { Mmap::map(&file) }
    }

    pub fn open_write_mmap(path: &Path) -> std::io::Result<MmapMut> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)?;

        unsafe { MmapMut::map_mut(&file) }
    }

    pub fn create_and_ensure_length(path: &Path, length: usize) -> std::io::Result<()> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        file.set_len(length as u64)?;
        Ok(())
    }
}

// To add to qdrant codebase
pub fn transmute_from_u8<T>(v: &[u8]) -> &T {
    unsafe { &*(v.as_ptr() as *const T) }
}

#[cfg(test)]
mod tests {
    use crate::sparse_index::common::types::DimId;
    use crate::sparse_index::immutable::inverted_index::inverted_index_ram::InvertedIndexBuilder;
    use crate::sparse_index::immutable::posting_list::PostingList;
    use tempfile::Builder;

    use super::*;

    fn compare_indexes(
        inverted_index_ram: &InvertedIndexRam,
        inverted_index_mmap: &InvertedIndexMmap,
    ) {
        for id in 0..inverted_index_ram.postings.len() as DimId {
            let posting_list_ram = inverted_index_ram.get(&id).unwrap().elements.as_slice();
            let posting_list_mmap = inverted_index_mmap.get(&id).unwrap();
            assert_eq!(posting_list_ram.len(), posting_list_mmap.len());
            for i in 0..posting_list_ram.len() {
                assert_eq!(posting_list_ram[i], posting_list_mmap[i]);
            }
        }
    }

    #[test]
    fn test_inverted_index_mmap() {
        let inverted_index_ram = InvertedIndexBuilder::new()
            .add(
                1,
                PostingList::from(vec![
                    (1, 10.0),
                    (2, 20.0),
                    (3, 30.0),
                    (4, 1.0),
                    (5, 2.0),
                    (6, 3.0),
                    (7, 4.0),
                    (8, 5.0),
                    (9, 6.0),
                ]),
            )
            .add(2, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .add(3, PostingList::from(vec![(1, 10.0), (2, 20.0), (3, 30.0)]))
            .build();

        let tmp_dir_path = Builder::new().prefix("test_index_dir").tempdir().unwrap();

        {
            let inverted_index_mmap =
                InvertedIndexMmap::convert_and_save(&inverted_index_ram, &tmp_dir_path).unwrap();

            compare_indexes(&inverted_index_ram, &inverted_index_mmap);
        }
        let inverted_index_mmap = InvertedIndexMmap::load(&tmp_dir_path).unwrap();

        compare_indexes(&inverted_index_ram, &inverted_index_mmap);
    }
}
