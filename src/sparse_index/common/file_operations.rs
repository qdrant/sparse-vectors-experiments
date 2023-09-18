use atomicwrites::{AtomicFile, OverwriteBehavior};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::File;
use std::io;
use std::io::{BufReader, BufWriter};
use std::path::Path;

pub fn atomic_save_json<T: Serialize>(path: &Path, object: &T) -> io::Result<()> {
    let af = AtomicFile::new(path, OverwriteBehavior::AllowOverwrite);
    let res = af.write(|f| serde_json::to_writer(BufWriter::new(f), object));
    match res {
        Ok(_) => Ok(()),
        Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
    }
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> io::Result<T> {
    Ok(serde_json::from_reader(BufReader::new(File::open(path)?))?)
}
