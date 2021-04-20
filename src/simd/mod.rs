#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(non_snake_case, overflowing_literals)]
pub unsafe fn simd_parse_crlf(buf: &[u8]) -> usize {
    use core::arch::x86_64::*;

    let mut ptr = buf.as_ptr();
    let dash_r_map: __m256i = _mm256_set1_epi8(0x0d);
    let dash_n_map: __m256i = _mm256_set1_epi8(0x0a);

    let mut len = buf.len();
    let mut res = 0;
    while len >= 32 {
        len -= 32;
        let data = _mm256_lddqu_si256(ptr as *const _);
        let bits1 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(dash_r_map, data)) as u32;
        let bits2 = _mm256_movemask_epi8(_mm256_cmpeq_epi8(dash_n_map, data)) as u32;
        let v = (bits2 >> 1) & bits1;
        // println!("{:b}, {:b}, {:b}", bits1, bits2, v);
        let cnt = _tzcnt_u32(v) as usize;
        res += cnt;
        if len == 0 || cnt != 32 {
            break;
        }
        ptr = ptr.add(32);
    }
    res
}