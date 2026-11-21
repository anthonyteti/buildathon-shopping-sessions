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
        for i in 0..3 {
            let v = _mm256_loadu_si256(p.add(32 * i) as *const __m256i);
            n |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, vn)) as u32 as u128) << (32 * i);
            c |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, vc)) as u32 as u128) << (32 * i);
            q |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, vq)) as u32 as u128) << (32 * i);
        }
        // most lines end within 96 bytes; the last 32 only when they do not
        if n == 0 {
            let v = _mm256_loadu_si256(p.add(96) as *const __m256i);
            n |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, vn)) as u32 as u128) << 96;
            c |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, vc)) as u32 as u128) << 96;
            q |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, vq)) as u32 as u128) << 96;
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

/// Separator positions of a line of N fields (len < 128): pos[k] is the end of field k
/// (pos[N-1] = len). Same validity rules as `fields`; fields are read with `field`.
#[inline(always)]
pub fn positions<const N: usize>(len: usize, cm: u128, qm: u128, pos: &mut [usize; N]) -> Option<bool> {
    let (clo, chi, qlo, qhi) = if len < 64 {
        let lm = (1u64 << len) - 1;
        (cm as u64 & lm, 0u64, qm as u64 & lm, 0u64)
    } else {
        let mh = (1u64 << (len - 64)) - 1;
        (cm as u64, (cm >> 64) as u64 & mh, qm as u64, (qm >> 64) as u64 & mh)
    };
    if (clo.count_ones() + chi.count_ones()) as usize != N - 1 {
        return None;
    }
    let quoted = (qlo | qhi) != 0;
    if quoted && quotes_bad(len, clo, chi, qlo, qhi) {
        return None;
    }
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
    Some(quoted)
}

/// Field k of a line split by `positions` (quotes stripped when the line has any).
#[inline(always)]
pub fn field<'a, const N: usize>(base: *const u8, pos: &[usize; N], quoted: bool, k: usize) -> &'a [u8] {
    let s = if k == 0 { 0 } else { pos[k - 1] + 1 };
    let e = pos[k];
    let qd = (quoted && unsafe { *base.add(s) } == b'"') as usize;
    unsafe { std::slice::from_raw_parts(base.add(s + qd), e - s - 2 * qd) }
}

/// Prefix XOR of a 64-bit mask (bit i = parity of bits 0..=i).
#[inline(always)]
fn pxor64(x: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "pclmulqdq"))]
    unsafe {
        use std::arch::x86_64::*;
        let r = _mm_clmulepi64_si128(_mm_set_epi64x(0, x as i64), _mm_set1_epi8(-1), 0);
        _mm_cvtsi128_si64(r) as u64
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "pclmulqdq")))]
    prefix_xor64(x)
}

/// The quote rule of `fields` on the two 64-bit halves of a line's masks (len < 128).
#[inline(always)]
fn quotes_bad(len: usize, clo: u64, chi: u64, qlo: u64, qhi: u64) -> bool {
    let (elo, ehi) = if len < 64 { (1u64 << len, 0u64) } else { (0u64, 1u64 << (len - 64)) };
    let seps_lo = clo | elo;
    let seps_hi = chi | ehi;
    let starts_lo = (clo << 1) | 1;
    let starts_hi = (chi << 1) | (clo >> 63);
    let ends_lo = (seps_lo >> 1) | (seps_hi << 63);
    let ends_hi = seps_hi >> 1;
    let px_lo = pxor64(qlo);
    let px_hi = pxor64(qhi) ^ (0u64.wrapping_sub(qlo.count_ones() as u64 & 1));
    let bad_lo = (qlo & !(starts_lo | ends_lo)) | (qlo & starts_lo & ends_lo) | (px_lo & seps_lo);
    let bad_hi = (qhi & !(starts_hi | ends_hi)) | (qhi & starts_hi & ends_hi) | (px_hi & seps_hi);
    (bad_lo | bad_hi) != 0
}
