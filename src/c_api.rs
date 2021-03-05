use libc::*;
use std::ptr;

#[link(name = "gf_complete", kind = "static")]
#[link(name = "Jerasure", kind = "static")]
extern "C" {

    pub(crate) fn cauchy_good_general_coding_matrix(k: c_int, m: c_int, w: c_int) -> *mut c_int;

    pub(crate) fn jerasure_matrix_to_bitmatrix(
        k: c_int,
        m: c_int,
        w: c_int,
        bitmatrix: *mut c_int,
    ) -> *mut c_int;

    fn jerasure_bitmatrix_encode(
        k: c_int,
        m: c_int,
        w: c_int,
        bitmatrix: *mut c_int,
        data_ptrs: *mut *mut c_int,
        coding_ptrs: *mut *mut c_int,
        size: c_int,
        packetsize: c_int,
    );

    fn jerasure_bitmatrix_decode(
        k: c_int,
        m: c_int,
        w: c_int,
        bitmatrix: *mut c_int,
        row_k_ones: c_int,
        erasures: *mut c_int,
        data_ptrs: *mut *mut c_int,
        coding_ptrs: *mut *mut c_int,
        size: c_int,
        packetsize: c_int,
    ) -> c_int;

    pub(crate) fn jerasure_smart_bitmatrix_to_schedule(
        k: c_int,
        m: c_int,
        w: c_int,
        bitmatrix: *mut c_int,
    ) -> *mut *mut c_int;

    pub(crate) fn jerasure_schedule_encode(
        k: c_int,
        m: c_int,
        w: c_int,
        schedule: *mut *mut c_int,
        data_ptrs: *const *const u8,
        coding_ptrs: *const *mut u8,
        size: usize,
        packetsize: c_int,
    );

    fn jerasure_schedule_decode_lazy(
        k: c_int,
        m: c_int,
        w: c_int,
        bitmatrix: *mut c_int,
        erasures: *mut c_int,
        data_ptrs: *mut *mut c_int,
        coding_ptrs: *mut *mut c_int,
        size: c_int,
        packetsize: c_int,
        smart: c_int,
    ) -> c_int;

}
