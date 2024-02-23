use std::{
    ffi::CString,
    path::Path,
    ptr::{null, null_mut},
};

mod bindings {
    include!(concat!(env!("OUT_DIR"), "/triedb.rs"));
}

pub struct Handle {
    db_ptr: *mut bindings::triedb,
}

impl Handle {
    pub fn try_new(dbdir_path: &Path) -> Option<Self> {
        let path = CString::new(dbdir_path.to_str().expect("invalid path"))
            .expect("failed to create CString");

        let mut db_ptr = null_mut();

        let result = unsafe { bindings::triedb_open(path.as_c_str().as_ptr(), &mut db_ptr) };

        if result != 0 {
            return None;
        }

        Some(Self { db_ptr })
    }

    pub fn read(&self, key: &[u8], key_len_nibbles: u8) -> Option<Vec<u8>> {
        let mut value_ptr = null();
        assert!(key_len_nibbles < u8::MAX - 1); // make sure doesn't overflow
        assert!((key_len_nibbles as usize + 1) / 2 <= key.len());
        let result = unsafe {
            bindings::triedb_read(self.db_ptr, key.as_ptr(), key_len_nibbles, &mut value_ptr)
        };
        if result == -1 {
            return None;
        }

        if result == 0 {
            return Some(Vec::new());
        }

        // check that there's no unexpected error
        assert!(result > 0);

        let value_len = result.try_into().unwrap();
        let value = unsafe {
            let value = std::slice::from_raw_parts(value_ptr, value_len).to_vec();
            bindings::triedb_finalize(value_ptr);
            value
        };

        Some(value)
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        let result = unsafe { bindings::triedb_close(self.db_ptr) };
        assert_eq!(result, 0);
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use crate::Handle;

    #[test]
    fn read() {
        let handle = Handle::try_new(Path::new("/dummy")).unwrap();

        // this key is hardcoded into mock triedb
        let result = handle.read(&[1, 2, 3], 6);
        assert_eq!(result, Some(vec![4, 5, 6]));

        // this key is hardcoded into mock triedb
        let result = handle.read(&[7, 8, 9], 6);
        assert_eq!(result, Some(vec![10, 11, 12]));

        let result = handle.read(&[0], 2);
        assert_eq!(result, None);
    }

    #[test]
    #[should_panic]
    fn read_invalid() {
        let handle = Handle::try_new(Path::new("/dummy")).unwrap();

        // too many nibbles
        let _ = handle.read(&[1, 2, 3], 7);
    }
}
