use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::PathBuf,
};

#[derive(Debug)]
pub struct AppendOnlyFile {
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

    pub fn read_only(file_path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(file_path)?;
        Ok(Self { file })
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.file.read_exact(buf)
    }

    pub fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        self.file.write_all(data)
    }

    pub fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    pub fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    pub fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }
}
