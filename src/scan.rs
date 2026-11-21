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

#[inline(always)]
fn prefix_xor(mut x: u128) -> u128 {
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^= x << 32;
    x ^= x << 64;
    x
}

/// Splits a line of `len` < 128 bytes into N fields using the comma and quote masks of
/// its window (bits at or past `len` are ignored). Same result as the scalar `split`.
///
/// Quote rule as bit math: every quote must sit on the first or last byte of a field,
/// never on a one-byte field, and every field must hold an even number of quotes
/// (the prefix parity of the quote mask is 0 at every separator). Together these mean
/// each field has no quotes or exactly one pair wrapping it.
#[inline(always)]
pub fn fields<'a, const N: usize>(line: &'a [u8], cm: u128, qm: u128, out: &mut [&'a [u8]; N]) -> bool {
    let len = line.len();
    debug_assert!(len < 128);
    if len < 64 {
        return fields64::<N>(line, cm as u64, qm as u64, out);
    }
    // 64 <= len < 128: work on the two 64-bit halves
    let mh = (1u64 << (len - 64)) - 1;
    let clo = cm as u64;
    let chi = (cm >> 64) as u64 & mh;
    if (clo.count_ones() + chi.count_ones()) as usize != N - 1 {
        return false;
    }
    let qlo = qm as u64;
    let qhi = (qm >> 64) as u64 & mh;
    if (qlo | qhi) != 0 {
        let c = clo as u128 | (chi as u128) << 64;
        let q = qlo as u128 | (qhi as u128) << 64;
        let seps = c | (1u128 << len);
        let starts = (c << 1) | 1;
        let ends = seps >> 1;
        let bad = (q & !(starts | ends)) | (q & starts & ends) | (prefix_xor(q) & seps);
        if bad != 0 {
            return false;
        }
    }
    // separator positions: the low half's commas, then the high half's
    let mut pos = [0usize; N];
    let mut k = 0usize;
    let mut x = clo;
    while x != 0 {
        unsafe { *pos.get_unchecked_mut(k) = x.trailing_zeros() as usize };
        x &= x - 1;
        k += 1;
    }
    let mut y = chi;
    while y != 0 {
        unsafe { *pos.get_unchecked_mut(k) = 64 + y.trailing_zeros() as usize };
        y &= y - 1;
        k += 1;
    }
    pos[N - 1] = len;
    let base = line.as_ptr();
    let mut s = 0usize;
    for k in 0..N {
        let e = pos[k];
        // a quote at a field start means a validated quoted field (line[len] is '\n')
        let qd = (unsafe { *base.add(s) } == b'"') as usize;
        out[k] = unsafe { std::slice::from_raw_parts(base.add(s + qd), e - s - 2 * qd) };
        s = e + 1;
    }
    true
}

#[inline(always)]
fn prefix_xor64(mut x: u64) -> u64 {
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^= x << 32;
    x
}

/// `fields` for lines shorter than 64 bytes, on 64-bit masks.
#[inline(always)]
fn fields64<'a, const N: usize>(line: &'a [u8], cm: u64, qm: u64, out: &mut [&'a [u8]; N]) -> bool {
    let len = line.len();
    let lm = (1u64 << len) - 1;
    let c = cm & lm;
    if c.count_ones() as usize != N - 1 {
        return false;
    }
    let q = qm & lm;
    if q != 0 {
        let seps = c | (1u64 << len);
        let starts = (c << 1) | 1;
        let ends = seps >> 1;
        let bad = (q & !(starts | ends)) | (q & starts & ends) | (prefix_xor64(q) & seps);
        if bad != 0 {
            return false;
        }
    }
    let mut x = c;
    let mut s = 0usize;
    for k in 0..N {
        let e = if k + 1 < N {
            let e = x.trailing_zeros() as usize;
            x &= x - 1;
            e
        } else {
            len
        };
        let qd = (unsafe { *line.as_ptr().add(s) } == b'"') as usize;
        out[k] = unsafe { std::slice::from_raw_parts(line.as_ptr().add(s + qd), e - s - 2 * qd) };
        s = e + 1;
    }
    true
}
