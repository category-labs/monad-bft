use std::fs::{File, OpenOptions};
#[cfg(feature = "nightly")]
use std::io::IoSlice;
use std::io::{self, Read, Write};
use std::path::PathBuf;

#[derive(Debug)]
pub(crate) struct AppendOnlyFile {
    file: File,
}

impl AppendOnlyFile {
    pub fn new(file_path: PathBuf) -> io::Result<Self> {
        // open the file in r+ (read append mode)
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(file_path)?;
        Ok(Self { file })
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.file.read_exact(buf)
    }

    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }

    // `write_all_vectored` can write header and data in one call
    // but it's currently a nig¡htly feature
    pub fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        self.file.write_all(data)
    }

    #[cfg(feature = "nightly")]
    pub fn write_all_vectored(&mut self, bufs: &mut [IoSlice<'_>]) -> io::Result<()> {
        self.file.write_all_vectored(bufs)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    pub fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }
}
