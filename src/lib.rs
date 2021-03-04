use std::num::NonZeroUsize;
use std::ptr;
use std::slice;

mod c_api;

const WORD_SIZE: NonZeroUsize = NonZeroUsize::new(32).unwrap();
const PACKET_SIZE: NonZeroUsize = NonZeroUsize::new(4096).unwrap();

pub struct ErasureCoder {
    data_fragments: NonZeroUsize,
    parity_fragments: NonZeroUsize,
    _matrix: *mut i32,
    _schedule: *mut *mut i32,
}

impl ErasureCoder {
    pub fn new(data_fragments: NonZeroUsize, parity_fragments: NonZeroUsize) -> Self {
        let matrix = unsafe {
            let matrix = c_api::cauchy_good_general_coding_matrix(
                parity_fragments.get() as i32,
                data_fragments.get() as i32,
                WORD_SIZE.get() as i32,
            );
            c_api::jerasure_matrix_to_bitmatrix(
                parity_fragments.get() as i32,
                data_fragments.get() as i32,
                WORD_SIZE.get() as i32,
                matrix,
            )
        };

        let schedule = unsafe {
            c_api::jerasure_smart_bitmatrix_to_schedule(
                parity_fragments.get() as i32,
                data_fragments.get() as i32,
                WORD_SIZE.get() as i32,
                matrix,
            )
        };

        Self {
            data_fragments,
            parity_fragments,
            _matrix: matrix,
            _schedule: schedule,
        }
    }

    pub fn fragments(&self) -> NonZeroUsize {
        unsafe {
            NonZeroUsize::new_unchecked(self.data_fragments.get() + self.parity_fragments.get())
        }
    }

    pub fn encode(&self, data: &[u8]) -> Vec<Vec<u8>> {
        //data to pointers

        let mut coding_ptr = ptr::null();
        let coding_len = data.len();

        // init coding pointers

        // execute jerasure_schedule_encode

        unsafe {
            c_api::jerasure_schedule_encode(
                self.parity_fragments.get() as i32,
                self.data_fragments.get() as i32,
                WORD_SIZE.get() as i32,
                self._schedule,
                data.as_ptr(),
                coding_ptr,
                coding_len,
                PACKET_SIZE.get() as i32,
            )
        };

        // from pointers to bytes

        let result = Vec::with_capacity(self.fragments().get());

        let data_fragments =
            unsafe { slice::from_raw_parts(coding_ptr, self.data_fragments.get()) };

        let result =
            unsafe { Vec::from_raw_parts(coding_ptr, coding_len, self.data_fragments.get()) };
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
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
