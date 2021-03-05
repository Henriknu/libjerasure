use std::alloc::{self};
use std::ptr;
use std::slice;
use std::{alloc::Layout, mem};
use std::{mem::size_of, num::NonZeroUsize};

mod c_api;

const DEFAULT_WORD_SIZE: usize = 4;
const DEFAULT_PACKET_SIZE: usize = 1;
const DEFAULT_SIZE: usize = DEFAULT_WORD_SIZE * DEFAULT_PACKET_SIZE;

pub struct ErasureCoder {
    // k - The number of data blocks to encode. We need at least k such encoded blocks to decode.
    data_fragments: usize,
    // m - The number of additional coding blocks to add when encoding the data blocks. We can tolerate as many as m missing blocks when decoding, e.g. "erasures".
    parity_fragments: usize,
    word_size: usize,
    packet_size: usize,
    size: usize,

    _schedule: *mut *mut i32,
}

impl ErasureCoder {
    pub fn new(data_fragments: NonZeroUsize, parity_fragments: NonZeroUsize) -> Self {
        println!("Running constructor");

        let data_fragments = data_fragments.get();
        let parity_fragments = parity_fragments.get();
        let word_size = DEFAULT_WORD_SIZE;
        let packet_size = DEFAULT_PACKET_SIZE;
        let size = DEFAULT_SIZE;

        let schedule = unsafe {
            let matrix = c_api::cauchy_good_general_coding_matrix(
                data_fragments as i32,
                parity_fragments as i32,
                word_size as i32,
            );

            assert!(!matrix.is_null(), "Matrix was null");
            let bitmatrix = c_api::jerasure_matrix_to_bitmatrix(
                data_fragments as i32,
                parity_fragments as i32,
                word_size as i32,
                matrix,
            );

            assert!(!bitmatrix.is_null(), "Bitmatrix was null");

            libc::free(matrix as *mut libc::c_void);

            let schedule = c_api::jerasure_smart_bitmatrix_to_schedule(
                data_fragments as i32,
                parity_fragments as i32,
                word_size as i32,
                bitmatrix,
            );

            assert!(!schedule.is_null(), "schedule was null");

            libc::free(bitmatrix as *mut libc::c_void);

            schedule
        };

        Self {
            data_fragments,
            parity_fragments,
            word_size,
            packet_size,
            size,

            _schedule: schedule,
        }
    }

    pub fn fragments(&self) -> usize {
        self.data_fragments + self.parity_fragments
    }

    pub fn encode(&self, data: &[u8]) -> Vec<Vec<u8>> {
        //make sure data align to self.size

        let aligned_data = self.align(data);

        //data to pointers

        let data_ptrs = aligned_data
            .iter()
            .map(|vec| vec.as_ptr())
            .collect::<Vec<*const u8>>();

        // coding ptrs

        let layout = Layout::array::<u8>(self.size).unwrap();

        let mut coding_ptrs = [ptr::null_mut(); DEFAULT_SIZE];

        for i in 0..self.parity_fragments {
            coding_ptrs[i] = unsafe { alloc::alloc(layout) };
        }

        dbg!("Before encoding");

        unsafe {
            c_api::jerasure_schedule_encode(
                self.data_fragments as i32,
                self.parity_fragments as i32,
                self.word_size as i32,
                self._schedule,
                data_ptrs.as_ptr(),
                coding_ptrs.as_ptr(),
                self.size,
                self.packet_size as i32,
            );
        }

        // construct result (k + m) vector of blocks

        let mut result = Vec::with_capacity(self.fragments());

        for i in 0..self.data_fragments {
            let encoded_fragments = unsafe { slice::from_raw_parts(data_ptrs[i], DEFAULT_SIZE) };
            result.push(Vec::from(encoded_fragments));
        }

        for i in 0..self.parity_fragments {
            let encoded_fragments = unsafe { slice::from_raw_parts(coding_ptrs[i], DEFAULT_SIZE) };
            result.push(Vec::from(encoded_fragments));
        }

        result
    }
    pub fn decode(&self) -> Vec<u8> {
        //data to pointers

        // init coding pointers

        // execute jerasure_schedule_decode_lazy

        // from pointers to bytes
        todo!()
    }
    pub fn reconstruct(&self) -> Vec<u8> {
        todo!()
    }

    fn align(&self, data: &[u8]) -> Vec<Vec<u8>> {
        let mut result = Vec::new();

        // include data

        let mut copy = data.to_owned();

        while !copy.is_empty() {
            let entry = match copy.len() {
                len if len >= self.size => copy.drain(0..self.size).collect(),
                _ => {
                    let mut remaining = copy.drain(..).collect::<Vec<u8>>();
                    remaining.extend(vec![0; self.size - remaining.len()]);
                    remaining
                }
            };

            result.push(entry)
        }

        // pad with additional vecs until we have k pointers

        let missing = self.parity_fragments - result.len() + 2;

        result.extend((0..missing).map(|_| vec![0; self.size]));

        assert_eq!(result.len(), self.data_fragments);

        result
    }
}

impl Drop for ErasureCoder {
    fn drop(&mut self) {
        unsafe {
            libc::free(self._schedule as *mut libc::c_void);
        }
        println!("Dropped erasurecoder");
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn it_builds() {
        let k = NonZeroUsize::new(4).unwrap();
        let m = NonZeroUsize::new(2).unwrap();

        let encoder = ErasureCoder::new(k, m);
    }

    #[test]
    fn encodes() {
        let k = NonZeroUsize::new(4).unwrap();
        let m = NonZeroUsize::new(2).unwrap();

        let encoder = ErasureCoder::new(k, m);

        let data = vec![10, 20, 30, 40];

        let encoded = encoder.encode(&data);

        assert_eq!(encoded.len(), k.get() + m.get());

        println!("Fragments: {:?}", encoded);
    }
}
