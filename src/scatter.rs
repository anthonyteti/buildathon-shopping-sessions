//! Partition scatter with software write-combining: records for each partition gather
//! in a 64-byte staging line, and full lines go to the partition's buffer with
//! streaming (non-temporal) stores on x86, so the scatter does not read every
//! destination cache line before writing it.

pub struct Scatter<T: Copy, const K: usize> {
    pub vecs: Vec<Vec<T>>,
    stage: Vec<[T; K]>,
    cnt: Vec<u8>,
}

impl<T: Copy + Default, const K: usize> Scatter<T, K> {
    pub fn new(parts: usize, reserve: usize) -> Self {
        assert!(std::mem::size_of::<[T; K]>() == 64);
        Scatter {
            vecs: (0..parts).map(|_| Vec::with_capacity(reserve.next_multiple_of(K))).collect(),
            stage: vec![[T::default(); K]; parts],
            cnt: vec![0; parts],
        }
    }

    #[inline(always)]
    pub fn push(&mut self, p: usize, v: T) {
        let c = unsafe { *self.cnt.get_unchecked(p) } as usize;
        unsafe { *self.stage.get_unchecked_mut(p).get_unchecked_mut(c) = v };
        if c + 1 == K {
            self.flush_line(p);
            unsafe { *self.cnt.get_unchecked_mut(p) = 0 };
        } else {
            unsafe { *self.cnt.get_unchecked_mut(p) = (c + 1) as u8 };
        }
    }

    #[inline(always)]
    fn flush_line(&mut self, p: usize) {
        let v = unsafe { self.vecs.get_unchecked_mut(p) };
        if v.capacity() - v.len() < K {
            v.reserve(K.max(v.len() / 2));
        }
        let len = v.len();
        let dst = unsafe { v.as_mut_ptr().add(len) } as *mut u8;
        let src = unsafe { self.stage.get_unchecked(p) }.as_ptr() as *const u8;
        unsafe { copy_line(dst, src) };
        unsafe { v.set_len(len + K) };
    }

    /// Moves the partial lines into the buffers and orders the streaming stores.
    pub fn finish(&mut self) {
        for p in 0..self.vecs.len() {
            let c = self.cnt[p] as usize;
            if c > 0 {
                let line = self.stage[p];
                self.vecs[p].extend_from_slice(&line[..c]);
                self.cnt[p] = 0;
            }
        }
        fence();
    }
}

#[inline(always)]
pub fn fence() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_sfence()
    }
}

#[inline(always)]
unsafe fn copy_line(dst: *mut u8, src: *const u8) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(src as *const __m256i);
        let b = _mm256_loadu_si256(src.add(32) as *const __m256i);
        if (dst as usize) & 31 == 0 {
            _mm256_stream_si256(dst as *mut __m256i, a);
            _mm256_stream_si256(dst.add(32) as *mut __m256i, b);
        } else {
            _mm256_storeu_si256(dst as *mut __m256i, a);
            _mm256_storeu_si256(dst.add(32) as *mut __m256i, b);
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    unsafe {
        std::ptr::copy_nonoverlapping(src, dst, 64)
    }
}
