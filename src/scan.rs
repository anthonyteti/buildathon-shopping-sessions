//! Line scanning: bitmasks of '\n', ',' and '"' over a 128-byte window, and field
//! splitting from those masks. The scalar `split` in main.rs is the reference; with
//! SS_CHECK_SPLIT=1 every line is split both ways and compared.

/// Bytes that must be readable from a line start for the fast path (window plus slack
/// for 16-byte reads at any field start inside the window).
pub const FAST_SLACK: usize = 160;

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
pub unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    use std::arch::x86_64::*;
    unsafe {
        let vn = _mm256_set1_epi8(b'\n' as i8);
        let vc = _mm256_set1_epi8(b',' as i8);
        let vq = _mm256_set1_epi8(b'"' as i8);
        let (mut n, mut c, mut q) = (0u128, 0u128, 0u128);
        for i in 0..4 {
            let v = _mm256_loadu_si256(p.add(32 * i) as *const __m256i);
            n |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, vn)) as u32 as u128) << (32 * i);
            c |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, vc)) as u32 as u128) << (32 * i);
            q |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, vq)) as u32 as u128) << (32 * i);
        }
        (n, c, q)
    }
}

#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
#[inline(always)]
pub unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    use std::arch::x86_64::*;
    unsafe {
        let vn = _mm_set1_epi8(b'\n' as i8);
        let vc = _mm_set1_epi8(b',' as i8);
        let vq = _mm_set1_epi8(b'"' as i8);
        let (mut n, mut c, mut q) = (0u128, 0u128, 0u128);
        for i in 0..8 {
            let v = _mm_loadu_si128(p.add(16 * i) as *const __m128i);
            n |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, vn)) as u16 as u128) << (16 * i);
            c |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, vc)) as u16 as u128) << (16 * i);
            q |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, vq)) as u16 as u128) << (16 * i);
        }
        (n, c, q)
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
pub unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    use std::arch::aarch64::*;
    unsafe {
        const BITS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let bm = vld1q_u8(BITS.as_ptr());
        #[inline(always)]
        unsafe fn mm(c: [uint8x16_t; 4], bm: uint8x16_t) -> u64 {
            unsafe {
                let s0 = vpaddq_u8(vandq_u8(c[0], bm), vandq_u8(c[1], bm));
                let s1 = vpaddq_u8(vandq_u8(c[2], bm), vandq_u8(c[3], bm));
                let s = vpaddq_u8(s0, s1);
                let s = vpaddq_u8(s, s);
                vgetq_lane_u64(vreinterpretq_u64_u8(s), 0)
            }
        }
        let vn = vdupq_n_u8(b'\n');
        let vc = vdupq_n_u8(b',');
        let vq = vdupq_n_u8(b'"');
        let (mut n, mut c, mut q) = (0u128, 0u128, 0u128);
        for h in 0..2 {
            let v: [uint8x16_t; 4] = [
                vld1q_u8(p.add(64 * h)),
                vld1q_u8(p.add(64 * h + 16)),
                vld1q_u8(p.add(64 * h + 32)),
                vld1q_u8(p.add(64 * h + 48)),
            ];
            let cn = [vceqq_u8(v[0], vn), vceqq_u8(v[1], vn), vceqq_u8(v[2], vn), vceqq_u8(v[3], vn)];
            let cc = [vceqq_u8(v[0], vc), vceqq_u8(v[1], vc), vceqq_u8(v[2], vc), vceqq_u8(v[3], vc)];
            let cq = [vceqq_u8(v[0], vq), vceqq_u8(v[1], vq), vceqq_u8(v[2], vq), vceqq_u8(v[3], vq)];
            n |= (mm(cn, bm) as u128) << (64 * h);
            c |= (mm(cc, bm) as u128) << (64 * h);
            q |= (mm(cq, bm) as u128) << (64 * h);
        }
        (n, c, q)
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
pub unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    let (mut n, mut c, mut q) = (0u128, 0u128, 0u128);
    for i in 0..128 {
        let b = unsafe { *p.add(i) };
        n |= ((b == b'\n') as u128) << i;
        c |= ((b == b',') as u128) << i;
        q |= ((b == b'"') as u128) << i;
    }
    (n, c, q)
}

/// Splits a line of `len` < 128 bytes into N fields using the comma and quote masks of
/// its window (bits at or past `len` are ignored). Same result as the scalar `split`.
#[inline(always)]
pub fn fields<'a, const N: usize>(line: &'a [u8], cm: u128, qm: u128, out: &mut [&'a [u8]; N]) -> bool {
    let len = line.len();
    debug_assert!(len < 128);
    let lm = (1u128 << len) - 1;
    let mut c = cm & lm;
    if c.count_ones() as usize != N - 1 {
        return false;
    }
    let mut st = [0usize; N];
    let mut en = [0usize; N];
    let mut s = 0usize;
    for k in 0..N {
        let e = if k + 1 < N {
            let e = c.trailing_zeros() as usize;
            c &= c - 1;
            e
        } else {
            len
        };
        st[k] = s;
        en[k] = e;
        s = e + 1;
    }
    let q = qm & lm;
    if q == 0 {
        for k in 0..N {
            out[k] = unsafe { line.get_unchecked(st[k]..en[k]) };
        }
        return true;
    }
    for k in 0..N {
        let (s, e) = (st[k], en[k]);
        let fm = ((1u128 << (e - s)) - 1) << s;
        let fq = q & fm;
        if fq == 0 {
            out[k] = unsafe { line.get_unchecked(s..e) };
        } else if e - s >= 2 && fq == ((1u128 << s) | (1u128 << (e - 1))) {
            out[k] = unsafe { line.get_unchecked(s + 1..e - 1) };
        } else {
            return false;
        }
    }
    true
}
