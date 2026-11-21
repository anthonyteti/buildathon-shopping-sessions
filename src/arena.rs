//! Global allocator: allocations of 64 KiB and more come from one reserved region by
//! bumping a pointer, and are never returned (the process exits right after the output
//! is written). On Linux the region asks for transparent huge pages, which cuts page
//! faults, TLB misses and the cost of tearing the memory down at exit. Smaller
//! allocations, and everything if the reservation fails, use the system allocator.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

const BIG: usize = 64 << 10;
const RESERVE: usize = 1 << 37; // 128 GiB of address space, MAP_NORESERVE
const ALIGN: usize = 64;

static BASE: AtomicUsize = AtomicUsize::new(0); // 0 = not set up yet, 1 = unavailable
static NEXT: AtomicUsize = AtomicUsize::new(0);

pub struct Arena;

fn base() -> usize {
    let b = BASE.load(Relaxed);
    if b != 0 {
        return b;
    }
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            RESERVE,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        )
    };
    let nb = if p == libc::MAP_FAILED {
        1
    } else {
        #[cfg(target_os = "linux")]
        unsafe {
            libc::madvise(p, RESERVE, libc::MADV_HUGEPAGE);
        }
        // start on a 2 MiB boundary
        ((p as usize) + (2 << 20) - 1) & !((2 << 20) - 1)
    };
    match BASE.compare_exchange(0, nb, Relaxed, Relaxed) {
        Ok(_) => nb,
        Err(cur) => {
            if nb != 1 {
                unsafe { libc::munmap(p, RESERVE) };
            }
            cur
        }
    }
}

#[inline]
fn in_arena(p: *mut u8) -> bool {
    let b = BASE.load(Relaxed);
    b > 1 && (p as usize) >= b && (p as usize) < b + RESERVE - (2 << 20)
}

unsafe fn bump(size: usize) -> *mut u8 {
    let b = base();
    if b == 1 {
        return std::ptr::null_mut();
    }
    let size = (size + ALIGN - 1) & !(ALIGN - 1);
    let off = NEXT.fetch_add(size, Relaxed);
    if off + size > RESERVE - (2 << 20) {
        return std::ptr::null_mut();
    }
    (b + off) as *mut u8
}

unsafe impl GlobalAlloc for Arena {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= ALIGN {
            let p = unsafe { bump(l.size()) };
            if !p.is_null() {
                return p;
            }
        }
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= ALIGN {
            let p = unsafe { bump(l.size()) };
            if !p.is_null() {
                return p; // fresh anonymous memory is zero and is never reused
            }
        }
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if !in_arena(p) {
            unsafe { System.dealloc(p, l) }
        }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        let arena = in_arena(p);
        if arena && new_size <= l.size() {
            return p;
        }
        if arena || (new_size >= BIG && l.align() <= ALIGN) {
            let nl = unsafe { Layout::from_size_align_unchecked(new_size, l.align()) };
            let q = unsafe { self.alloc(nl) };
            if !q.is_null() {
                unsafe { std::ptr::copy_nonoverlapping(p, q, l.size().min(new_size)) };
                unsafe { self.dealloc(p, l) };
            }
            return q;
        }
        unsafe { System.realloc(p, l, new_size) }
    }
}
