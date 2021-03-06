use std::alloc::{self};
use std::ptr;
use std::slice;
use std::{alloc::Layout, mem};
use std::{mem::size_of, num::NonZeroUsize};

use error::{ErasureCoderError, ErasureCoderResult};

mod c_api;
pub mod error;

const DEFAULT_WORD_SIZE: usize = 8;
const DEFAULT_PACKET_SIZE: usize = 1;
const MIN_SIZE: usize = DEFAULT_WORD_SIZE * DEFAULT_PACKET_SIZE;

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

impl ErasureCoder {
    /// Creates new erasure coder, with `k = data_fragments` and `m = parity_fragments` .
    ///
    /// May fail if Jerasure cannot generate matrix for the given `k` and `m`.
    pub fn new(
        data_fragments: NonZeroUsize,
        parity_fragments: NonZeroUsize,
    ) -> ErasureCoderResult<Self> {
        let data_fragments = data_fragments.get();
        let parity_fragments = parity_fragments.get();
        let word_size = DEFAULT_WORD_SIZE;
        let packet_size = DEFAULT_PACKET_SIZE;

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
            packet_size,

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

        //spread data across k arrays of size *size*, padding with zeros if needed.

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
        fragments: Vec<Vec<u8>>,
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
            let id = erasures[i];
            coding.insert(id as usize, unsafe {
                Vec::from_raw_parts(alloc::alloc(layout), size, size)
            });
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

        let mut result = Vec::with_capacity(self.data_fragments);

        for i in 0..self.data_fragments {
            result.extend_from_slice(unsafe { slice::from_raw_parts(data_ptrs[i], size) });
        }

        Ok(result)
    }

    fn spread(&self, data: &[u8], size: usize) -> Vec<Vec<u8>> {
        let mut result = Vec::new();

        // include data

        let mut copy = data.to_owned();

        while !copy.is_empty() {
            let entry = match copy.len() {
                len if len >= size => copy.drain(0..size).collect(),
                _ => {
                    let mut remaining = copy.drain(..).collect::<Vec<u8>>();
                    remaining.extend(vec![0; size - remaining.len()]);
                    remaining
                }
            };

            result.push(entry)
        }

        // pad with additional vecs until we have k pointers

        result.extend((0..self.data_fragments - result.len()).map(|_| vec![0; size]));

        result
    }

    fn determine_size(&self, data_len: usize) -> usize {
        // return the smallest size which captures the data.
        // Has to be multiple of word size * packet size.

        // need to be able to store the data in k * size arrays.
        // size  > data_len // data_fragments

        let mut size = self.data_fragments * MIN_SIZE;

        while size < data_len {
            size <<= 2;
        }

        size / self.data_fragments
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

    #[derive(Debug, Serialize, Deserialize)]
    struct Value {
        id: usize,
        inner: Vec<u128>,
    }

    #[test]
    fn it_works() {
        let k = NonZeroUsize::new(4).unwrap();
        let m = NonZeroUsize::new(2).unwrap();

        let encoder = ErasureCoder::new(k, m).unwrap();

        let value = Value {
            id: 100,
            inner: vec![10000, 200000, 113231231, 2312312, 232332],
        };

        let data = serialize(&value).unwrap();

        let encoded = encoder.encode(&data);

        assert_eq!(encoded.len(), k.get() + m.get());

        let decoded = encoder.decode(encoded, vec![]).unwrap();

        let decoded_value: Value = deserialize(&decoded).unwrap();

        println!("Decoded value: {:?}", decoded_value);
    }

    #[test]
    fn encodes_decodes_with_single_erasure() {
        let k = NonZeroUsize::new(4).unwrap();
        let m = NonZeroUsize::new(2).unwrap();

        let encoder = ErasureCoder::new(k, m).unwrap();

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

        let decoded = encoder.decode(encoded, vec![0]).unwrap();

        let decoded_value: Value = deserialize(&decoded).unwrap();

        println!("Decoded value: {:?}", decoded_value);
    }

    #[test]
    fn encodes_decodes_with_multiple_erasure() {
        let f = 33;
        let n = 3 * f + 1;

        let k = NonZeroUsize::new(n - f).unwrap();
        let m = NonZeroUsize::new(f).unwrap();

        let encoder = ErasureCoder::new(k, m).unwrap();

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

        let decoded = encoder.decode(encoded, vec![0, 1, 2]).unwrap();

        let decoded_value: Value = deserialize(&decoded).unwrap();

        println!("Decoded value: {:?}", decoded_value);
    }

    #[test]
    #[should_panic]
    fn panics_on_too_many_erasures() {
        let k = NonZeroUsize::new(4).unwrap();
        let m = NonZeroUsize::new(2).unwrap();

        let encoder = ErasureCoder::new(k, m).unwrap();

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

        let decoded = encoder.decode(encoded, vec![0, 1, 2, 3]).unwrap();

        let decoded_value: Value = deserialize(&decoded).unwrap();

        println!("Decoded value: {:?}", decoded_value);
    }
}
