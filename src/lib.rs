#![warn(rust_2018_idioms)]

use lazy_static::lazy_static;
use std::alloc::Layout;
use std::alloc::{self};
use std::num::NonZeroUsize;
use std::ptr;
use std::slice;
use std::sync::Mutex;

use error::{ErasureCoderError, ErasureCoderResult};

mod c_api;
pub mod error;

pub const DEFAULT_WORD_SIZE: usize = 8;
pub const DEFAULT_PACKET_SIZE: usize = 2048;
pub const DEFAULT_MIN_SIZE: usize = DEFAULT_WORD_SIZE * DEFAULT_PACKET_SIZE;

lazy_static! {
    static ref LOCK: Mutex<()> = Mutex::new(());
}

pub struct ErasureCoder {
    /// k - The number of data blocks to encode. We need at least k such encoded blocks to decode.
    data_fragments: usize,
    /// m - The number of additional coding blocks to add when encoding the data blocks. We can tolerate as many as m missing blocks when decoding, e.g. "erasures".
    parity_fragments: usize,
    word_size: usize,
    packet_size: usize,

    _bitmatrix: *mut i32,
    _schedule: *mut *mut i32,
}

unsafe impl Send for ErasureCoder {}
unsafe impl Sync for ErasureCoder {}

impl ErasureCoder {
    /// Creates new erasure coder, with `k = data_fragments` and `m = parity_fragments` .
    ///
    /// May fail if Jerasure cannot generate matrix for the given `k` and `m`.
    pub fn new(
        data_fragments: NonZeroUsize,
        parity_fragments: NonZeroUsize,
        packet_size: NonZeroUsize,
        word_size: NonZeroUsize,
    ) -> ErasureCoderResult<Self> {
        let _guard = LOCK.lock().unwrap();

        let data_fragments = data_fragments.get();
        let parity_fragments = parity_fragments.get();
        let word_size = word_size.get();

        let (bitmatix, schedule) = unsafe {
            let matrix = c_api::cauchy_good_general_coding_matrix(
                data_fragments as i32,
                parity_fragments as i32,
                word_size as i32,
            );

            if matrix.is_null() {
                libc::free(matrix as *mut libc::c_void);
                return Err(ErasureCoderError::MatrixNull);
            }

            let bitmatrix = c_api::jerasure_matrix_to_bitmatrix(
                data_fragments as i32,
                parity_fragments as i32,
                word_size as i32,
                matrix,
            );

            libc::free(matrix as *mut libc::c_void);

            if bitmatrix.is_null() {
                libc::free(bitmatrix as *mut libc::c_void);
                return Err(ErasureCoderError::BitmatrixNull);
            }

            let schedule = c_api::jerasure_smart_bitmatrix_to_schedule(
                data_fragments as i32,
                parity_fragments as i32,
                word_size as i32,
                bitmatrix,
            );

            if schedule.is_null() {
                libc::free(bitmatrix as *mut libc::c_void);
                libc::free(schedule as *mut libc::c_void);
                return Err(ErasureCoderError::BitmatrixNull);
            }

            (bitmatrix, schedule)
        };

        Ok(Self {
            data_fragments,
            parity_fragments,
            word_size,
            packet_size: packet_size.get(),

            _bitmatrix: bitmatix,
            _schedule: schedule,
        })
    }
    /// Number of total fragments produced by the erasure coder (`k + m`)
    pub fn fragments(&self) -> usize {
        self.data_fragments + self.parity_fragments
    }

    /// Erasure encodes a slice of bytes.
    ///
    /// It is required that the data fits within `k` arrays of length `size`, where `size` is a multiple of `word_size * packet_size`.
    /// If needed, the data will be padded with zeros.
    ///
    /// Returns a Vec with `k + m` fragments, each of length `size`.
    pub fn encode(&self, data: &[u8]) -> Vec<Vec<u8>> {
        // dynamicly choose size

        let size = self.determine_size(data.len());

        let data = self.spread(data, size);

        //data to pointers

        let data_ptrs = data
            .iter()
            .map(|vec| vec.as_ptr())
            .collect::<Vec<*const u8>>();

        // coding ptrs

        let layout = Layout::array::<u8>(size).unwrap();

        let mut coding_ptrs = vec![ptr::null_mut(); self.parity_fragments];

        for i in 0..self.parity_fragments {
            coding_ptrs[i] = unsafe { alloc::alloc(layout) };
        }

        unsafe {
            c_api::jerasure_schedule_encode(
                self.data_fragments as i32,
                self.parity_fragments as i32,
                self.word_size as i32,
                self._schedule,
                data_ptrs.as_slice().as_ptr(),
                coding_ptrs.as_slice().as_ptr(),
                size,
                self.packet_size as i32,
            );
        }

        // construct result (k + m) vector of blocks

        let mut result = Vec::with_capacity(self.fragments());

        for i in 0..self.data_fragments {
            result.push(Vec::from(unsafe {
                slice::from_raw_parts(data_ptrs[i], size)
            }));
        }

        for i in 0..self.parity_fragments {
            result.push(Vec::from(unsafe {
                slice::from_raw_parts(coding_ptrs[i], size)
            }));
        }

        result
    }

    /// Decodes the erasure coded fragments and return a Vec with the original encoded data, possibly with trailing zeros.
    ///
    /// Fragments are expected to be chronologically sorted according to index, with data blocks first, followed by coding blocks.
    ///
    /// Erasures contain ids between `0` and `k + m`, corresponding to the indexes of the fragments which are missing and need to be reconstructed.
    /// Erasures are expected to be in chronological order.
    pub fn decode(
        &self,
        fragments: &Vec<Vec<u8>>,
        mut erasures: Vec<i32>,
    ) -> ErasureCoderResult<Vec<u8>> {
        if fragments.len() + erasures.len() != self.fragments() {
            return Err(ErasureCoderError::InvalidNumberOfFragments(
                self.fragments(),
                erasures.len(),
                fragments.len(),
            ));
        }

        let size = fragments[0].len();

        // calculate number of erased data fragments

        let n_data_erased = erasures
            .iter()
            .filter(|&id| *id < self.data_fragments as i32)
            .count();

        // Layout

        let layout = Layout::array::<u8>(size).unwrap();

        //data to pointers

        let mut data = Vec::with_capacity(self.data_fragments);

        data.extend_from_slice(&fragments[0..self.data_fragments - n_data_erased]);

        for i in 0..n_data_erased {
            let id = erasures[i];
            data.insert(id as usize, unsafe {
                Vec::from_raw_parts(alloc::alloc(layout), size, size)
            });
        }

        assert_eq!(data.len(), self.data_fragments);

        let data_ptrs = data
            .iter_mut()
            .map(|vec| vec.as_mut_ptr())
            .collect::<Vec<*mut u8>>();

        // init coding pointers

        let mut coding = Vec::with_capacity(self.parity_fragments);

        coding.extend_from_slice(&fragments[(self.data_fragments - n_data_erased)..]);

        for i in n_data_erased..erasures.len() {
            let relative_id = erasures[i] - self.data_fragments as i32;

            let coding_fragment = unsafe { Vec::from_raw_parts(alloc::alloc(layout), size, size) };

            if relative_id >= coding.len() as i32 {
                coding.push(coding_fragment);
            } else {
                coding.insert(relative_id as usize, coding_fragment);
            }
        }

        assert_eq!(coding.len(), self.parity_fragments);

        let coding_ptrs = coding
            .iter_mut()
            .map(|vec| vec.as_mut_ptr())
            .collect::<Vec<*mut u8>>();

        // Jerasure expect there to be a -1 at the end of the erasure array

        erasures.push(-1);

        // Jerasure provides a smart (not naive) option for erasure decoding, (hopefully) providing performance gains.
        // There exist a bug in which jerasure segfaults if smart is set and there are no erasures (data + coding is intact)
        // See: https://github.com/tsuraan/Jerasure/issues/16
        // Avoid this by simply using "dumb" option in the case that there were no erasures
        let smart = (erasures.len() > 1) as i32;

        // execute jerasure_schedule_decode_lazy

        let ret = unsafe {
            c_api::jerasure_schedule_decode_lazy(
                self.data_fragments as i32,
                self.parity_fragments as i32,
                self.word_size as i32,
                self._bitmatrix,
                erasures.as_slice().as_ptr(),
                data_ptrs.as_slice().as_ptr(),
                coding_ptrs.as_slice().as_ptr(),
                size as i32,
                self.packet_size as i32,
                smart,
            )
        };

        if ret != 0 {
            return Err(ErasureCoderError::FailedToDecode);
        }

        // from pointers to bytes

        let mut result = Vec::with_capacity(self.data_fragments);

        for i in 0..self.data_fragments {
            result.extend_from_slice(unsafe { slice::from_raw_parts(data_ptrs[i], size) });
        }

        Ok(result)
    }

    /// Reconstructs any missing fragments, returning a Vec with all fragments.
    ///
    /// Fragments are expected to be chronologically sorted according to index, with data blocks first, followed by coding blocks.
    ///
    /// Erasures contain ids between `0` and `k + m`, corresponding to the indexes of the fragments which are missing and need to be reconstructed.
    /// Erasures are expected to be in chronological order.
    pub fn reconstruct(
        &self,
        fragments: &Vec<Vec<u8>>,
        mut erasures: Vec<i32>,
    ) -> ErasureCoderResult<Vec<Vec<u8>>> {
        if fragments.len() + erasures.len() != self.fragments() {
            return Err(ErasureCoderError::InvalidNumberOfFragments(
                self.fragments(),
                erasures.len(),
                fragments.len(),
            ));
        }

        let size = fragments[0].len();

        // calculate number of erased data fragments

        let n_data_erased = erasures
            .iter()
            .filter(|&id| *id < self.data_fragments as i32)
            .count();

        // Layout

        let layout = Layout::array::<u8>(size).unwrap();

        //data to pointers

        let mut data = Vec::with_capacity(self.data_fragments);

        data.extend_from_slice(&fragments[0..self.data_fragments - n_data_erased]);

        for i in 0..n_data_erased {
            let id = erasures[i];
            data.insert(id as usize, unsafe {
                Vec::from_raw_parts(alloc::alloc(layout), size, size)
            });
        }

        assert_eq!(data.len(), self.data_fragments);

        let data_ptrs = data
            .iter_mut()
            .map(|vec| vec.as_mut_ptr())
            .collect::<Vec<*mut u8>>();

        // init coding pointers

        let mut coding = Vec::with_capacity(self.parity_fragments);

        coding.extend_from_slice(&fragments[(self.data_fragments - n_data_erased)..]);

        for i in n_data_erased..erasures.len() {
            let relative_id = erasures[i] - self.data_fragments as i32;

            let coding_fragment = unsafe { Vec::from_raw_parts(alloc::alloc(layout), size, size) };

            if relative_id >= coding.len() as i32 {
                coding.push(coding_fragment);
            } else {
                coding.insert(relative_id as usize, coding_fragment);
            }
        }

        assert_eq!(coding.len(), self.parity_fragments);

        let coding_ptrs = coding
            .iter_mut()
            .map(|vec| vec.as_mut_ptr())
            .collect::<Vec<*mut u8>>();

        // Jerasure expect there to be a -1 at the end of the erasure array

        erasures.push(-1);

        // Jerasure provides a smart (not naive) option for erasure decoding, providing performance gains.
        // There exist a bug in which jerasure segfaults if smart is set and there are no erasures (data + coding is intact)
        // See: https://github.com/tsuraan/Jerasure/issues/16
        // Avoid this by simply using "dumb" option in the case that there were no erasures
        let smart = (erasures.len() > 1) as i32;

        // execute jerasure_schedule_decode_lazy

        let ret = unsafe {
            c_api::jerasure_schedule_decode_lazy(
                self.data_fragments as i32,
                self.parity_fragments as i32,
                self.word_size as i32,
                self._bitmatrix,
                erasures.as_slice().as_ptr(),
                data_ptrs.as_slice().as_ptr(),
                coding_ptrs.as_slice().as_ptr(),
                size as i32,
                self.packet_size as i32,
                smart,
            )
        };

        if ret != 0 {
            return Err(ErasureCoderError::FailedToDecode);
        }

        // from pointers to bytes

        let mut result = Vec::with_capacity(self.fragments());

        for i in 0..self.data_fragments {
            result.push(Vec::from(unsafe {
                slice::from_raw_parts(data_ptrs[i], size)
            }));
        }

        for i in 0..self.parity_fragments {
            result.push(Vec::from(unsafe {
                slice::from_raw_parts(coding_ptrs[i], size)
            }));
        }

        Ok(result)
    }

    fn spread(&self, data: &[u8], size: usize) -> Vec<Vec<u8>> {
        let mut result: Vec<Vec<u8>> = data.chunks(size).map(|chunk| chunk.to_vec()).collect();

        let tail_len = result.last().unwrap().len();

        if tail_len < size {
            result
                .last_mut()
                .unwrap()
                .extend(std::iter::repeat(0).take(size - tail_len));
        }

        while result.len() < self.data_fragments {
            result.push(vec![0; size])
        }

        result
    }

    fn determine_size(&self, data_len: usize) -> usize {
        // return the smallest size which captures the data.
        // Has to be multiple of word size * packet size.

        // need to be able to store the data in k * size arrays.
        // size  > data_len // data_fragments

        let ratio = data_len / self.data_fragments;

        let min_size = self.packet_size * self.word_size;

        min_size * (ratio / min_size + 1)
    }
}

impl Drop for ErasureCoder {
    fn drop(&mut self) {
        unsafe {
            libc::free(self._bitmatrix as *mut libc::c_void);
            libc::free(self._schedule as *mut libc::c_void);
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use bincode::{deserialize, serialize};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Value {
        id: usize,
        inner: Vec<u128>,
    }

    #[test]
    fn it_works() {
        let k = NonZeroUsize::new(4).unwrap();
        let m = NonZeroUsize::new(2).unwrap();

        let encoder = ErasureCoder::new(
            k,
            m,
            NonZeroUsize::new(DEFAULT_PACKET_SIZE).unwrap(),
            NonZeroUsize::new(DEFAULT_WORD_SIZE).unwrap(),
        )
        .unwrap();

        let value = Value {
            id: 100,
            inner: vec![10000, 200000, 113231231, 2312312, 232332],
        };

        let data = serialize(&value).unwrap();

        let encoded = encoder.encode(&data);

        assert_eq!(encoded.len(), k.get() + m.get());

        let decoded = encoder.decode(&encoded, vec![]).unwrap();

        let decoded_value: Value = deserialize(&decoded).unwrap();

        println!("Decoded value: {:?}", decoded_value);
    }

    #[test]
    fn large_n() {
        let f = 25;
        let n = 4 * f;

        let k = NonZeroUsize::new(n - 2 * f).unwrap();
        let m = NonZeroUsize::new(2 * f).unwrap();

        let encoder = ErasureCoder::new(
            k,
            m,
            NonZeroUsize::new(DEFAULT_PACKET_SIZE).unwrap(),
            NonZeroUsize::new(DEFAULT_WORD_SIZE).unwrap(),
        )
        .unwrap();
        let value = vec![vec![32u128; 16]; 100];

        let data = serialize(&value).unwrap();

        let encoded = encoder.encode(&data);

        assert_eq!(encoded.len(), k.get() + m.get());

        let decoded = encoder.decode(&encoded, vec![]).unwrap();

        let decoded_value: Value = deserialize(&decoded).unwrap();

        println!("Decoded value: {:?}", decoded_value);
    }

    #[test]
    fn encodes_decodes_with_single_erasure() {
        let k = NonZeroUsize::new(4).unwrap();
        let m = NonZeroUsize::new(2).unwrap();

        let encoder = ErasureCoder::new(
            k,
            m,
            NonZeroUsize::new(DEFAULT_PACKET_SIZE).unwrap(),
            NonZeroUsize::new(DEFAULT_WORD_SIZE).unwrap(),
        )
        .unwrap();
        let value = Value {
            id: 100,
            inner: vec![10000, 200000, 113231231, 2312312, 232332],
        };

        let data = serialize(&value).unwrap();

        let mut encoded = encoder.encode(&data);

        assert_eq!(encoded.len(), k.get() + m.get());

        // erase first data element

        encoded.remove(0);

        // decode

        let decoded = encoder.decode(&encoded, vec![0]).unwrap();

        let decoded_value: Value = deserialize(&decoded).unwrap();

        println!("Decoded value: {:?}", decoded_value);

        assert_eq!(value, decoded_value);
    }

    #[test]
    fn encodes_decodes_with_multiple_erasure() {
        let f = 33;
        let n = 3 * f + 1;

        let k = NonZeroUsize::new(n - f).unwrap();
        let m = NonZeroUsize::new(f).unwrap();

        let encoder = ErasureCoder::new(
            k,
            m,
            NonZeroUsize::new(DEFAULT_PACKET_SIZE).unwrap(),
            NonZeroUsize::new(DEFAULT_WORD_SIZE).unwrap(),
        )
        .unwrap();
        let value = Value {
            id: 100,
            inner: vec![10000, 200000, 113231231, 2312312, 232332],
        };

        let data = serialize(&value).unwrap();

        let mut encoded = encoder.encode(&data);

        assert_eq!(encoded.len(), k.get() + m.get());

        // erase multiple data elements

        encoded.remove(0);
        encoded.remove(0);
        encoded.remove(0);

        // decode

        let decoded = encoder.decode(&encoded, vec![0, 1, 2]).unwrap();

        let decoded_value: Value = deserialize(&decoded).unwrap();

        println!("Decoded value: {:?}", decoded_value);

        assert_eq!(value, decoded_value);
    }

    #[test]
    fn encodes_reconstructs_with_multiple_erasures() {
        let f = 33;
        let n = 3 * f + 1;

        let k = NonZeroUsize::new(n - f).unwrap();
        let m = NonZeroUsize::new(f).unwrap();

        let encoder = ErasureCoder::new(
            k,
            m,
            NonZeroUsize::new(DEFAULT_PACKET_SIZE).unwrap(),
            NonZeroUsize::new(DEFAULT_WORD_SIZE).unwrap(),
        )
        .unwrap();
        let value = Value {
            id: 100,
            inner: vec![10000, 200000, 113231231, 2312312, 232332],
        };

        let data = serialize(&value).unwrap();

        let encoded = encoder.encode(&data);

        assert_eq!(encoded.len(), k.get() + m.get());

        let mut clone_encoded = encoded.clone();

        // erase multiple data elements

        clone_encoded.remove(0);
        clone_encoded.remove(0);
        clone_encoded.remove(0);

        // decode

        let reconstructed = encoder.reconstruct(&clone_encoded, vec![0, 1, 2]).unwrap();

        assert_eq!(encoded.len(), reconstructed.len());

        assert_eq!(encoded, reconstructed);
    }

    #[test]
    fn encodes_reconstructs_with_no_codings() {
        let f = 33;
        let n = 3 * f + 1;

        let k = NonZeroUsize::new(n - f).unwrap();
        let m = NonZeroUsize::new(f).unwrap();

        let encoder = ErasureCoder::new(
            k,
            m,
            NonZeroUsize::new(DEFAULT_PACKET_SIZE).unwrap(),
            NonZeroUsize::new(DEFAULT_WORD_SIZE).unwrap(),
        )
        .unwrap();
        let value = Value {
            id: 100,
            inner: vec![10000, 200000, 113231231, 2312312, 232332],
        };

        let data = serialize(&value).unwrap();

        let encoded = encoder.encode(&data);

        assert_eq!(encoded.len(), k.get() + m.get());

        let mut clone_encoded = encoded.clone();

        // erase multiple data elements

        clone_encoded.drain(k.get()..);

        // decode

        let erasure_indexes = (k.get() as i32)..(encoded.len() as i32);

        let erasures = erasure_indexes.collect::<Vec<i32>>();

        let reconstructed = encoder.reconstruct(&clone_encoded, erasures).unwrap();

        assert_eq!(encoded.len(), reconstructed.len());

        assert_eq!(encoded, reconstructed);
    }

    #[test]
    fn encodes_reconstructs_with_some_codings() {
        let f = 33;
        let n = 3 * f + 1;

        let k = NonZeroUsize::new(n - f).unwrap();
        let m = NonZeroUsize::new(f).unwrap();

        let encoder = ErasureCoder::new(
            k,
            m,
            NonZeroUsize::new(DEFAULT_PACKET_SIZE).unwrap(),
            NonZeroUsize::new(DEFAULT_WORD_SIZE).unwrap(),
        )
        .unwrap();

        let value = Value {
            id: 100,
            inner: vec![10000, 200000, 113231231, 2312312, 232332],
        };

        let data = serialize(&value).unwrap();

        let encoded = encoder.encode(&data);

        assert_eq!(encoded.len(), k.get() + m.get());

        let mut clone_encoded = encoded.clone();

        // erase multiple data elements

        clone_encoded.drain(k.get()..k.get() + 5);

        // decode

        let erasure_indexes = (k.get() as i32)..(k.get() as i32 + 5);

        let erasures = erasure_indexes.collect::<Vec<i32>>();

        println!("Erasures: {:#?}", erasures);

        let reconstructed = encoder.reconstruct(&clone_encoded, erasures).unwrap();

        assert_eq!(encoded.len(), reconstructed.len());

        assert_eq!(encoded, reconstructed);
    }

    #[test]
    #[should_panic]
    fn panics_on_too_many_erasures() {
        let k = NonZeroUsize::new(4).unwrap();
        let m = NonZeroUsize::new(2).unwrap();

        let encoder = ErasureCoder::new(
            k,
            m,
            NonZeroUsize::new(DEFAULT_PACKET_SIZE).unwrap(),
            NonZeroUsize::new(DEFAULT_WORD_SIZE).unwrap(),
        )
        .unwrap();
        let value = Value {
            id: 100,
            inner: vec![10000, 200000, 113231231, 2312312, 232332],
        };

        let data = serialize(&value).unwrap();

        let mut encoded = encoder.encode(&data);

        assert_eq!(encoded.len(), k.get() + m.get());

        // erase too many fragments

        encoded.remove(0);
        encoded.remove(0);
        encoded.remove(0);
        encoded.remove(0);

        // decode

        let decoded = encoder.decode(&encoded, vec![0, 1, 2, 3]).unwrap();

        let decoded_value: Value = deserialize(&decoded).unwrap();

        println!("Decoded value: {:?}", decoded_value);

        assert_eq!(value, decoded_value);
    }
}
