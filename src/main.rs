//! Shopping Sessions pipeline: one native binary.
//!
//! Phases:
//!   0. manifest, products, campaigns; mmap every CSV and cut it into chunks at '\n'.
//!   1. links: parse in parallel, then union-find to map every linked ID to its person name.
//!   2. events and clicks: parse in parallel. Events are scattered by hash(event_id),
//!      clicks (already validated and resolved to their person) by hash(person).
//!   3. per event_id partition: dedup (smallest ingest_ts), keep the valid kept copies,
//!      scatter them by hash(person).
//!   4. per person partition: sort, sessionize, credit and assists, aggregate.
//!   5. write Parquet.
//!
//! IDs are packed into order-preserving u64 keys (up to 9 ASCII bytes, 7 bits each), so
//! equality and byte-wise order are exact integer operations. If any ID does not fit,
//! the whole run is redone with keys that are ranks in the sorted set of all IDs.

mod arena;
mod fastpath;
mod parquet_out;
mod scan;
mod scatter;

use memmap2::Mmap;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

#[global_allocator]
static GLOBAL: arena::Arena = arena::Arena;

pub const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;

// ---------------------------------------------------------------- hashing

#[derive(Default, Clone, Copy)]
pub struct Fx(u64);
impl Hasher for Fx {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.rotate_left(5) ^ b as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
        }
    }
    #[inline]
    fn write_usize(&mut self, n: usize) {
        self.0 = (self.0.rotate_left(5) ^ n as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
}
pub type FxMap<K, V> = HashMap<K, V, BuildHasherDefault<Fx>>;

#[inline(always)]
fn mix(k: u64) -> u64 {
    let x = k.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 29)
}

// ---------------------------------------------------------------- CSV dialect

/// Splits one line (without its '\n') into exactly N fields per the dialect:
/// every comma separates, a field may be wrapped in quotes, any other quote is malformed.
#[inline(always)]
fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let mut k = 0usize;
    let mut start = 0usize;
    for c in memchr::memchr_iter(b',', line) {
        if k + 1 >= N {
            return false;
        }
        out[k] = &line[start..c];
        k += 1;
        start = c + 1;
    }
    if k + 1 != N {
        return false;
    }
    out[k] = &line[start..];
    if memchr::memchr(b'"', line).is_none() {
        return true;
    }
    for f in out.iter_mut() {
        let v = *f;
        let n = v.len();
        if n >= 2 && v[0] == b'"' && v[n - 1] == b'"' {
            let inner = &v[1..n - 1];
            if memchr::memchr(b'"', inner).is_some() {
                return false;
            }
            *f = inner;
        } else if memchr::memchr(b'"', v).is_some() {
            return false;
        }
    }
    true
}

#[inline(always)]
fn for_lines<'a>(data: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    let mut pos = 0;
    for nl in memchr::memchr_iter(b'\n', data) {
        f(&data[pos..nl]);
        pos = nl + 1;
    }
    if pos < data.len() {
        f(&data[pos..]);
    }
}

// ---------------------------------------------------------------- timestamps

#[inline(always)]
fn d2(a: u8, b: u8) -> Option<i64> {
    let x = a.wrapping_sub(b'0');
    let y = b.wrapping_sub(b'0');
    if x < 10 && y < 10 { Some(x as i64 * 10 + y as i64) } else { None }
}

#[cfg(test)]
pub fn parse_ts_for_test(f: &[u8]) -> Option<i64> {
    parse_ts(f)
}

pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

/// f[0..19] = YYYY-MM-DDTHH:MM:SS -> epoch ms.
#[inline(always)]
pub fn civil_ms(f: &[u8]) -> Option<i64> {
    if f[4] != b'-' || f[7] != b'-' || f[10] != b'T' || f[13] != b':' || f[16] != b':' {
        return None;
    }
    let y = d2(f[0], f[1])? * 100 + d2(f[2], f[3])?;
    let mo = d2(f[5], f[6])?;
    let d = d2(f[8], f[9])?;
    let h = d2(f[11], f[12])?;
    let mi = d2(f[14], f[15])?;
    let s = d2(f[17], f[18])?;
    if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let dim = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo as usize - 1];
    if d > dim {
        return None;
    }
    Some(days_from_civil(y, mo, d) * DAY + ((h * 60 + mi) * 60 + s) * 1000)
}

/// F1 / F2 / F3 -> epoch ms, or None for a bad form. F3 saturates (huge = outside any window).
#[inline(always)]
pub fn parse_ts(f: &[u8]) -> Option<i64> {
    match f.len() {
        0 => None,
        20 if f[19] == b'Z' => civil_ms(f),
        25 if f[19] == b'+' || f[19] == b'-' => {
            if f[22] != b':' {
                return None;
            }
            let base = civil_ms(f)?;
            let oh = d2(f[20], f[21])?;
            let om = d2(f[23], f[24])?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 60 + om) * 60_000;
            Some(if f[19] == b'+' { base - off } else { base + off })
        }
        _ => {
            let mut v: u64 = 0;
            for &b in f {
                let x = b.wrapping_sub(b'0');
                if x > 9 {
                    return None;
                }
                v = v.saturating_mul(10).saturating_add(x as u64);
            }
            Some(if v > i64::MAX as u64 { i64::MAX } else { v as i64 })
        }
    }
}

/// Purchase price: ASCII digits only, value 1..=10,000,000.
#[inline(always)]
fn parse_price(f: &[u8]) -> Option<u32> {
    if f.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in f {
        let x = b.wrapping_sub(b'0');
        if x > 9 {
            return None;
        }
        v = v * 10 + x as u64;
        if v > 10_000_000 {
            v = 10_000_001; // saturate, keep checking the remaining bytes are digits
        }
    }
    if (1..=10_000_000).contains(&v) { Some(v as u32) } else { None }
}

// ---------------------------------------------------------------- ID keys

pub trait Packer: Sync {
    /// Non-empty ID -> non-zero key. Equality and order of keys equal those of the bytes.
    fn pack(&self, f: &[u8]) -> u64;
    /// Same as `pack`, but `f` must have at least 16 readable bytes from its start.
    #[inline(always)]
    unsafe fn pack_padded(&self, f: &[u8]) -> u64 {
        self.pack(f)
    }
    fn unpack(&self, k: u64, out: &mut Vec<u8>);
    fn failed(&self) -> bool;
}

/// Big-endian word of eight 7-bit bytes -> their 56 bits, most significant first.
#[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
#[inline(always)]
fn compress7(w: u64) -> u64 {
    unsafe { std::arch::x86_64::_pext_u64(w, 0x7f7f_7f7f_7f7f_7f7f) }
}

/// Big-endian word of eight 7-bit bytes -> their 56 bits, most significant first.
#[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
#[inline(always)]
fn compress7(w: u64) -> u64 {
    let x = (w & 0x007f_007f_007f_007f) | ((w & 0x7f00_7f00_7f00_7f00) >> 1);
    let x = (x & 0x0000_3fff_0000_3fff) | ((x & 0x3fff_0000_3fff_0000) >> 2);
    (x & 0x0fff_ffff) | ((x & 0x0fff_ffff_0000_0000) >> 4)
}

/// Up to 9 bytes, each 1..=127, 7 bits each, big-endian, zero-padded.
struct Pack7 {
    fail: AtomicBool,
}
impl Packer for Pack7 {
    #[inline(always)]
    fn pack(&self, f: &[u8]) -> u64 {
        let n = f.len();
        if n == 0 || n > 9 {
            if n > 9 {
                self.fail.store(true, Relaxed);
            }
            return if n == 0 { 0 } else { 1 };
        }
        let mut k = 0u64;
        let mut bad = false;
        for (j, &b) in f.iter().enumerate() {
            bad |= b == 0 || b >= 0x80;
            k |= ((b & 0x7f) as u64) << (56 - 7 * j);
        }
        if bad {
            self.fail.store(true, Relaxed);
            return 1;
        }
        k
    }
    #[inline(always)]
    unsafe fn pack_padded(&self, f: &[u8]) -> u64 {
        let n = f.len();
        if n == 0 {
            return 0;
        }
        if n > 9 {
            self.fail.store(true, Relaxed);
            return 1;
        }
        let raw = u64::from_le(unsafe { (f.as_ptr() as *const u64).read_unaligned() });
        let lo = n.min(8);
        #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
        let (w, mask) = {
            let w = unsafe { std::arch::x86_64::_bzhi_u64(raw, (8 * lo) as u32) };
            (w, unsafe { std::arch::x86_64::_bzhi_u64(u64::MAX, (8 * lo) as u32) })
        };
        #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
        let (w, mask) = {
            let mask = if lo == 8 { u64::MAX } else { (1u64 << (8 * lo)) - 1 };
            (raw & mask, mask)
        };
        let b8 = if n == 9 { (unsafe { *f.get_unchecked(8) }) as u64 } else { 0 };
        let probe = w | (!mask & 0x0101_0101_0101_0101);
        let high = w & 0x8080_8080_8080_8080 != 0;
        let zero = probe.wrapping_sub(0x0101_0101_0101_0101) & !probe & 0x8080_8080_8080_8080 != 0;
        let bad9 = n == 9 && (b8 == 0 || b8 >= 0x80);
        if high | zero | bad9 {
            self.fail.store(true, Relaxed);
            return 1;
        }
        (compress7(w.swap_bytes()) << 7) | b8
    }
    fn unpack(&self, k: u64, out: &mut Vec<u8>) {
        for j in 0..9 {
            let b = ((k >> (56 - 7 * j)) & 0x7f) as u8;
            if b == 0 {
                break;
            }
            out.push(b);
        }
    }
    fn failed(&self) -> bool {
        self.fail.load(Relaxed)
    }
}

/// Fallback: key = 1 + rank of the ID among all distinct IDs, sorted byte-wise.
struct RankPack {
    map: FxMap<Vec<u8>, u64>,
    strs: Vec<Vec<u8>>,
}
impl Packer for RankPack {
    fn pack(&self, f: &[u8]) -> u64 {
        if f.is_empty() {
            return 0;
        }
        *self.map.get(f).expect("ID missing from the rank table")
    }
    fn unpack(&self, k: u64, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.strs[(k - 1) as usize]);
    }
    fn failed(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------- person map

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0)
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags))
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = p;
}

/// Open addressing ID key -> person name key (key 0 = empty). Holds only IDs whose
/// name differs from themselves; every other ID is its own person.
pub struct PersonMap {
    t: Vec<[u64; 2]>,
    mask: usize,
    // one-hash filter (about 16 bits per entry): most IDs are their own person and
    // are answered here without touching the big table
    filt: Vec<u64>,
    fshift: u32,
}
impl PersonMap {
    fn new(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        let fbits = (n * 16).next_power_of_two().max(4096);
        PersonMap { t: vec![[0, 0]; cap], mask: cap - 1, filt: vec![0; fbits / 64], fshift: 64 - fbits.trailing_zeros() }
    }
    #[inline(always)]
    fn fpos(&self, k: u64) -> usize {
        (k.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> self.fshift) as usize
    }
    #[inline(always)]
    fn maybe(&self, k: u64) -> bool {
        let b = self.fpos(k);
        unsafe { *self.filt.get_unchecked(b >> 6) >> (b & 63) & 1 != 0 }
    }
    fn insert(&mut self, k: u64, v: u64) {
        let b = self.fpos(k);
        self.filt[b >> 6] |= 1 << (b & 63);
        let mut i = mix(k) as usize & self.mask;
        loop {
            if self.t[i][0] == 0 || self.t[i][0] == k {
                self.t[i] = [k, v];
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn prefetch_filter(&self, k: u64) {
        prefetch(self.filt.as_ptr().wrapping_add(self.fpos(k) >> 6));
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        if self.maybe(k) {
            prefetch(self.t.as_ptr().wrapping_add(mix(k) as usize & self.mask));
        }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> u64 {
        if !self.maybe(k) {
            return k;
        }
        let mut i = mix(k) as usize & self.mask;
        loop {
            let s = unsafe { *self.t.get_unchecked(i) };
            if s[0] == k {
                return s[1];
            }
            if s[0] == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Union-find over link endpoints. IDs get dense indices through a growable hash table;
/// every union keeps the root with the smaller key, so a root is its person's name.
struct Uf {
    t: Vec<(u64, u32)>,
    mask: usize,
    keyof: Vec<u64>,
    parent: Vec<u32>,
}
impl Uf {
    fn with_capacity(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(1024);
        Uf { t: vec![(0, 0); cap], mask: cap - 1, keyof: Vec::with_capacity(n), parent: Vec::with_capacity(n) }
    }
    fn grow(&mut self) {
        let cap = self.t.len() * 2;
        let old = std::mem::replace(&mut self.t, vec![(0, 0); cap]);
        self.mask = cap - 1;
        for (k, v) in old {
            if k != 0 {
                let mut i = mix(k) as usize & self.mask;
                while self.t[i].0 != 0 {
                    i = (i + 1) & self.mask;
                }
                self.t[i] = (k, v);
            }
        }
    }
    #[inline(always)]
    fn id(&mut self, k: u64) -> u32 {
        let mut i = mix(k) as usize & self.mask;
        loop {
            let s = self.t[i];
            if s.0 == k {
                return s.1;
            }
            if s.0 == 0 {
                let id = self.keyof.len() as u32;
                self.t[i] = (k, id);
                self.keyof.push(k);
                self.parent.push(id);
                if self.keyof.len() * 10 > self.t.len() * 7 {
                    self.grow();
                }
                return id;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn find(&mut self, mut x: u32) -> u32 {
        while self.parent[x as usize] != x {
            let gp = self.parent[self.parent[x as usize] as usize];
            self.parent[x as usize] = gp;
            x = gp;
        }
        x
    }
}

fn build_persons(pairs: &[Vec<(u64, u64)>]) -> PersonMap {
    let total: usize = pairs.iter().map(|v| v.len()).sum();
    let mut uf = Uf::with_capacity(total + total / 2);
    for v in pairs {
        for (k, &(a, b)) in v.iter().enumerate() {
            if k + 8 < v.len() {
                let (a2, b2) = v[k + 8];
                prefetch(uf.t.as_ptr().wrapping_add(mix(a2) as usize & uf.mask));
                prefetch(uf.t.as_ptr().wrapping_add(mix(b2) as usize & uf.mask));
            }
            let ia = uf.id(a);
            let ib = uf.id(b);
            let ra = uf.find(ia);
            let rb = uf.find(ib);
            if ra != rb {
                if uf.keyof[ra as usize] < uf.keyof[rb as usize] {
                    uf.parent[rb as usize] = ra;
                } else {
                    uf.parent[ra as usize] = rb;
                }
            }
        }
    }
    let n = uf.keyof.len() as u32;
    let mut changed = 0;
    for i in 0..n {
        if uf.find(i) != i {
            changed += 1;
        }
    }
    let mut pm = PersonMap::new(changed);
    for i in 0..n {
        let r = uf.find(i);
        if r != i {
            pm.insert(uf.keyof[i as usize], uf.keyof[r as usize]);
        }
    }
    pm
}

// ---------------------------------------------------------------- inputs

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                name.ends_with(".csv") && !name.starts_with('.') && p.is_file()
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

pub struct Input {
    map: Mmap,
    file: std::fs::File,
}
impl std::ops::Deref for Input {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.map
    }
}

fn map_files(paths: &[PathBuf]) -> Vec<Input> {
    let mut out = Vec::new();
    for p in paths {
        let f = std::fs::File::open(p).expect("open csv");
        let len = f.metadata().map(|m| m.len()).unwrap_or(0);
        if len == 0 {
            continue;
        }
        let m = unsafe { Mmap::map(&f) }.expect("mmap csv");
        out.push(Input { map: m, file: f });
    }
    out
}

/// Runs `f(data, start, end)` over one chunk: either straight on the mapping, or (with
/// `read`) on a copy read into `buf`, padded so every line takes the fast path.
#[inline(always)]
fn with_chunk(inp: &Input, c: &Chunk, read: bool, buf: &mut Vec<u8>, mut f: impl FnMut(&[u8], usize, usize)) {
    if !read {
        f(&inp.map, c.start, c.end);
        return;
    }
    // read the chunk in small blocks of whole lines into one reused buffer (stays in L2)
    use std::os::unix::fs::FileExt;
    const BLOCK: usize = 256 << 10;
    let mut pos = c.start;
    while pos < c.end {
        let mut want = BLOCK.min(c.end - pos);
        loop {
            if buf.len() < want + scan::FAST_SLACK {
                buf.resize(want + scan::FAST_SLACK, 0);
            }
            inp.file.read_exact_at(&mut buf[..want], pos as u64).expect("read csv");
            if pos + want == c.end {
                break;
            }
            match memchr::memrchr(b'\n', &buf[..want]) {
                Some(i) => {
                    want = i + 1;
                    break;
                }
                None => want = (want * 2).min(c.end - pos),
            }
        }
        // no stale bytes after the block (a last line without '\n' must not find one)
        buf[want..want + scan::FAST_SLACK].fill(0);
        f(&buf[..want + scan::FAST_SLACK], 0, want);
        pos += want;
    }
}

#[derive(Clone, Copy)]
struct Chunk {
    kind: u8, // 0 links, 1 events, 2 clicks
    file: u32,
    start: usize,
    end: usize,
}

fn make_chunks(maps: &[Input], kind: u8, target: usize, out: &mut Vec<Chunk>) {
    for (fi, m) in maps.iter().enumerate() {
        let d: &[u8] = m;
        let len = d.len();
        let mut start = match memchr::memchr(b'\n', d) {
            Some(i) => i + 1,
            None => len, // header only
        };
        while start < len {
            let mut end = (start + target).min(len);
            if end < len {
                end = match memchr::memchr(b'\n', &d[end..]) {
                    Some(i) => end + i + 1,
                    None => len,
                };
            }
            out.push(Chunk { kind, file: fi as u32, start, end });
            start = end;
        }
    }
}

/// Strip wrapping quotes and a trailing '\r' (products/campaigns are clean CSV).
fn clean_field(f: &[u8]) -> &[u8] {
    let f = if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' { &f[1..f.len() - 1] } else { f };
    f
}

/// Rows of a clean two-column CSV after its header (first row per key is kept by callers).
fn two_col_rows(data: &[u8]) -> Vec<(&[u8], &[u8])> {
    let mut out = Vec::new();
    for line in data.split(|&b| b == b'\n').skip(1) {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        let (a, b) = match memchr::memchr(b',', line) {
            Some(i) => (&line[..i], &line[i + 1..]),
            None => (line, &b""[..]),
        };
        out.push((clean_field(a), clean_field(b)));
    }
    out
}

fn manifest_ts(text: &str, key: &str) -> i64 {
    let k = format!("\"{key}\"");
    let i = text.find(&k).expect("manifest key") + k.len();
    let rest = &text[i..];
    let q1 = rest.find('"').expect("manifest value") + 1;
    let q2 = rest[q1..].find('"').expect("manifest value") + q1;
    parse_ts(rest[q1..q2].as_bytes()).expect("manifest timestamp")
}

pub struct Ctx {
    nthreads: usize,
    ws: i64,
    we: i64,
    cat_names: Vec<Vec<u8>>,
    unknown_cat: u16,
    camp_ids: Vec<Vec<u8>>,
    camp_channels: Vec<Vec<u8>>,
    links: Vec<Input>,
    events: Vec<Input>,
    clicks: Vec<Input>,
    use_read: bool,
    out_dir: PathBuf,
    check_split: bool,
    tstab: fastpath::TsTab,
    product_idx: fastpath::IdIndex,
    campaign_idx: fastpath::IdIndex,
}

// ---------------------------------------------------------------- parallel helper

/// Per-thread busy/finish times of the last parallel phase (ms), for the phase log.
static PHASE_STATS: std::sync::Mutex<Vec<(f64, f64)>> = std::sync::Mutex::new(Vec::new());

fn par_run<S: Send, I: Fn() -> S + Sync, F: Fn(&mut S, usize) + Sync>(nt: usize, n: usize, init: I, f: F) -> Vec<S> {
    let next = AtomicUsize::new(0);
    let t0 = Instant::now();
    let out: Vec<(S, f64, f64)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let start = t0.elapsed().as_secs_f64();
                    let mut st = init();
                    loop {
                        let i = next.fetch_add(1, Relaxed);
                        if i >= n {
                            break;
                        }
                        f(&mut st, i);
                    }
                    let end = t0.elapsed().as_secs_f64();
                    (st, (end - start) * 1e3, end * 1e3)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    *PHASE_STATS.lock().unwrap() = out.iter().map(|x| (x.1, x.2)).collect();
    out.into_iter().map(|x| x.0).collect()
}

// ---------------------------------------------------------------- records

/// One parsed event copy (32 bytes).
/// meta = ingest seconds (48 bits, offset by 2^47 from window start) << 16 | category.
/// tp   = (ts - window_start) ms << 26 | price << 2 | flags (1 = valid, 2 = purchase).
#[derive(Clone, Copy, Default)]
#[repr(C, align(32))]
struct Ev {
    eid: u64,
    uid: u64,
    meta: u64,
    tp: u64,
}

#[derive(Clone, Copy)]
struct Se {
    person: u64,
    ts: i64,
    price: u32,
    cat: u16,
    purchase: bool,
}

/// A surviving view or cart event (no price): 16 bytes.
#[derive(Clone, Copy, Default)]
#[repr(C, align(16))]
struct Sv {
    person: u64,
    ts: i64,
}

/// An event inside a grouped person slice (the person is the slice's): 16 bytes.
/// pc = price | category << 32 | purchase << 48.
#[derive(Clone, Copy)]
struct Ev2 {
    ts: i64,
    pc: u64,
}
impl Ev2 {
    #[inline(always)]
    fn of(e: &Se) -> Ev2 {
        Ev2 { ts: e.ts, pc: e.price as u64 | (e.cat as u64) << 32 | (e.purchase as u64) << 48 }
    }
    #[inline(always)]
    fn purchase(&self) -> bool {
        self.pc >> 48 != 0
    }
    #[inline(always)]
    fn price(&self) -> u32 {
        self.pc as u32
    }
    #[inline(always)]
    fn cat(&self) -> u16 {
        (self.pc >> 32) as u16
    }
}

#[derive(Clone, Copy, Default)]
#[repr(C, align(32))]
struct Ck {
    person: u64,
    ts: i64,
    cid: u64,
    camp: u16,
}

pub struct UdRow {
    pub person: u64,
    pub day: i32,
    pub sessions: i32,
    pub events: i32,
    pub revenue: i64,
    pub top: u16, // u16::MAX = null
}

#[derive(Clone, Copy)]
struct Parts {
    bits: u32,  // event_id partitions
    pbits: u32, // person partitions
}
impl Parts {
    #[inline(always)]
    fn n(self) -> usize {
        1 << self.bits
    }
    #[inline(always)]
    fn by_eid(self, k: u64) -> usize {
        (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - self.bits)) as usize
    }
    #[inline(always)]
    fn np(self) -> usize {
        1 << self.pbits
    }
    #[inline(always)]
    fn by_person(self, k: u64) -> usize {
        (k.wrapping_mul(0xC2B2_AE3D_27D4_EB4F) >> (64 - self.pbits)) as usize
    }
}

struct ParseState {
    ev: scatter::Scatter<Ev, 2>,
    ck: scatter::Scatter<Ck, 2>,
    buf: Vec<u8>,
}

struct DedupState {
    out: Vec<Vec<Sv>>,
    outp: Vec<Vec<Se>>,
    table: Vec<Ev>,
    surv: Vec<Ev>,
}

struct AggState {
    rows: Vec<UdRow>,
    camp: Vec<i64>, // [(camp) * ndays + day] * 4; slot 3 unused here (see asst)
    asst: Vec<u32>, // assisted sessions per (camp) * ndays + day: small, stays in cache
    catsum: Vec<i64>,
    touched: Vec<u16>,
    ccount: Vec<u32>,
    cpos: Vec<u32>,
    active: Vec<u16>,
    g: Group,
}

/// One person partition, grouped: events of local person l are evs[off[l]..off[l+1]],
/// sorted by ts; their clicks are cks[coff[l]..coff[l+1]], sorted by (ts, click_id).
#[derive(Default)]
struct Group {
    tab: Vec<(u64, u32)>,
    lid: Vec<u32>,
    off: Vec<u32>,
    pos: Vec<u32>,
    evs: Vec<Ev2>,
    pkey: Vec<u64>,
    coff: Vec<u32>,
    clid: Vec<u32>,
    cks: Vec<Ck>,
}

/// A record that can be grouped by person.
trait PersonRec: Copy {
    fn person(&self) -> u64;
    fn ev2(&self) -> Ev2;
}
impl PersonRec for Se {
    #[inline(always)]
    fn person(&self) -> u64 {
        self.person
    }
    #[inline(always)]
    fn ev2(&self) -> Ev2 {
        Ev2::of(self)
    }
}
impl PersonRec for Sv {
    #[inline(always)]
    fn person(&self) -> u64 {
        self.person
    }
    #[inline(always)]
    fn ev2(&self) -> Ev2 {
        Ev2 { ts: self.ts, pc: 0 }
    }
}

/// Assigns local person ids; false if the table filled up.
#[inline(always)]
fn assign_lids<'a, R: PersonRec + 'a>(g: &mut Group, mask: usize, limit: usize, parts: impl Iterator<Item = &'a [R]>) -> bool {
    for part in parts {
        for e in part {
            let person = e.person();
            let mut i = mix(person) as usize & mask;
            let l = loop {
                let s = unsafe { *g.tab.get_unchecked(i) };
                if s.0 == person {
                    break s.1;
                }
                if s.0 == 0 {
                    let l = g.off.len() as u32;
                    if l as usize >= limit {
                        return false;
                    }
                    g.tab[i] = (person, l);
                    g.off.push(0);
                    break l;
                }
                i = (i + 1) & mask;
            };
            g.off[l as usize] += 1;
            g.lid.push(l);
        }
    }
    true
}

#[inline(always)]
fn scatter_lids<'a, R: PersonRec + 'a>(g: &mut Group, k: &mut usize, parts: impl Iterator<Item = &'a [R]>) {
    for part in parts {
        for e in part {
            let l = g.lid[*k] as usize;
            g.evs[g.pos[l] as usize] = e.ev2();
            g.pkey[l] = e.person();
            g.pos[l] += 1;
            *k += 1;
        }
    }
}

fn group_partition<'a>(
    g: &mut Group,
    n: usize,
    plain_in: impl Iterator<Item = &'a [Sv]> + Clone,
    pur_in: impl Iterator<Item = &'a [Se]> + Clone,
    cks_in: impl Iterator<Item = &'a [Ck]> + Clone,
) {
    // local person ids; the table starts small (persons are far fewer than events)
    // and is rebuilt larger if it fills up
    let mut cap = (n / 4).next_power_of_two().max(64);
    let mask = loop {
        let mask = cap - 1;
        let limit = cap / 2;
        g.tab.clear();
        g.tab.resize(cap, (0, 0));
        g.lid.clear();
        g.off.clear();
        if assign_lids(g, mask, limit, plain_in.clone()) && assign_lids(g, mask, limit, pur_in.clone()) {
            break mask;
        }
        cap *= 4;
    };
    let nl = g.off.len();
    let mut acc = 0u32;
    for x in g.off.iter_mut() {
        let c = *x;
        *x = acc;
        acc += c;
    }
    g.off.push(acc);
    g.pos.clear();
    g.pos.extend_from_slice(&g.off[..nl]);
    g.evs.clear();
    g.evs.resize(n, Ev2 { ts: 0, pc: 0 });
    g.pkey.clear();
    g.pkey.resize(nl, 0);
    let mut k = 0;
    scatter_lids(g, &mut k, plain_in);
    scatter_lids(g, &mut k, pur_in);
    for l in 0..nl {
        let s = &mut g.evs[g.off[l] as usize..g.off[l + 1] as usize];
        if s.len() > 1 {
            s.sort_unstable_by_key(|e| e.ts);
        }
    }
    // clicks of persons with events in this partition
    g.coff.clear();
    g.coff.resize(nl + 1, 0);
    g.clid.clear();
    for part in cks_in.clone() {
        for c in part {
            let mut i = mix(c.person) as usize & mask;
            let l = loop {
                let s = g.tab[i];
                if s.0 == c.person {
                    break s.1;
                }
                if s.0 == 0 {
                    break u32::MAX;
                }
                i = (i + 1) & mask;
            };
            if l != u32::MAX {
                g.coff[l as usize] += 1;
            }
            g.clid.push(l);
        }
    }
    let mut acc = 0u32;
    for x in g.coff.iter_mut() {
        let c = *x;
        *x = acc;
        acc += c;
    }
    g.pos.clear();
    g.pos.extend_from_slice(&g.coff[..nl]);
    g.cks.clear();
    g.cks.resize(acc as usize, Ck { person: 0, ts: 0, cid: 0, camp: 0 });
    let mut k = 0;
    for part in cks_in {
        for c in part {
            let l = g.clid[k];
            k += 1;
            if l != u32::MAX {
                g.cks[g.pos[l as usize] as usize] = *c;
                g.pos[l as usize] += 1;
            }
        }
    }
    for l in 0..nl {
        let s = &mut g.cks[g.coff[l] as usize..g.coff[l + 1] as usize];
        if s.len() > 1 {
            s.sort_unstable_by_key(|c| (c.ts, c.cid));
        }
    }
}

// ---------------------------------------------------------------- the pipeline

fn cpu_ms() -> (f64, f64) {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as f64 * 1e3 + t.tv_usec as f64 / 1e3;
    (tv(ru.ru_utime), tv(ru.ru_stime))
}

#[cfg(target_os = "macos")]
fn instructions() -> f64 {
    let mut ri: libc::rusage_info_v4 = unsafe { std::mem::zeroed() };
    let r = unsafe {
        libc::proc_pid_rusage(libc::getpid(), libc::RUSAGE_INFO_V4, &mut ri as *mut _ as *mut libc::rusage_info_t)
    };
    if r == 0 { ri.ri_instructions as f64 / 1e9 } else { 0.0 }
}
#[cfg(not(target_os = "macos"))]
fn instructions() -> f64 {
    0.0
}

fn tlog(t0: &Instant, what: &str) {
    let stats = std::mem::take(&mut *PHASE_STATS.lock().unwrap());
    if !stats.is_empty() {
        let busy_min = stats.iter().map(|x| x.0).fold(f64::MAX, f64::min);
        let busy_max = stats.iter().map(|x| x.0).fold(0.0, f64::max);
        let end_min = stats.iter().map(|x| x.1).fold(f64::MAX, f64::min);
        let end_max = stats.iter().map(|x| x.1).fold(0.0, f64::max);
        eprintln!("    threads: busy {busy_min:.1}-{busy_max:.1} ms, finished {end_min:.1}-{end_max:.1} ms");
    }
    let (u, s) = cpu_ms();
    eprintln!(
        "[{:8.1} ms] [cpu {:8.1} ms] [sys {:7.1} ms] [ins {:6.2} G] {what}",
        t0.elapsed().as_secs_f64() * 1e3,
        u + s,
        s,
        instructions()
    );
}

/// Calls `f` with the fields of every well-formed line in file[start..end] (N fields).
/// Every field handed to `f` has at least 16 readable bytes from its start.
#[inline(always)]
fn scan_lines<const N: usize>(file: &[u8], start: usize, end: usize, check: bool, mut f: impl FnMut(&[&[u8]; N])) {
    let mut pos = start;
    let fast_end = file.len().saturating_sub(scan::FAST_SLACK);
    let mut buf: Vec<u8> = Vec::new();
    let mut out: [&[u8]; N] = [b""; N];
    while pos < end {
        if pos <= fast_end {
            // keep the input stream ahead of the scan
            prefetch(file.as_ptr().wrapping_add(pos + PF_DIST));
            prefetch(file.as_ptr().wrapping_add(pos + PF_DIST + 64));
            let (nl, cm, qm) = unsafe { scan::masks128(file.as_ptr().add(pos)) };
            if nl != 0 {
                let len = nl.trailing_zeros() as usize;
                let line = unsafe { file.get_unchecked(pos..pos + len) };
                let ok = scan::fields::<N>(line, cm, qm, &mut out);
                if check {
                    let mut o2: [&[u8]; N] = [b""; N];
                    let ok2 = split::<N>(line, &mut o2);
                    assert!(ok == ok2 && (!ok || out == o2), "splitters disagree on {:?}", String::from_utf8_lossy(line));
                }
                if ok {
                    f(&out);
                }
                pos += len + 1;
                continue;
            }
        }
        let e = memchr::memchr(b'\n', &file[pos..end]).map_or(end, |i| pos + i);
        buf.clear();
        buf.extend_from_slice(&file[pos..e]);
        buf.extend_from_slice(&[0u8; 16]);
        let line = &buf[..e - pos];
        let mut o: [&[u8]; N] = [b""; N];
        if split::<N>(line, &mut o) {
            f(&o);
        }
        pos = e + 1;
    }
}

const INGEST_OFF: i64 = 1 << 47;
const PF_DIST: usize = 1024;

/// Returns None when the packer reports an ID it cannot represent.
fn run<P: Packer>(ctx: &Ctx, pk: &P, t0: &Instant) -> Option<()> {
    let nt = ctx.nthreads;
    let (ws, we) = (ctx.ws, ctx.we);
    let ws_s = ws.div_euclid(1000);
    let check = ctx.check_split;
    assert!(we - ws < (1i64 << 38), "event window too long");

    // ---- links
    let mut lchunks = Vec::new();
    make_chunks(&ctx.links, 0, 4 << 20, &mut lchunks);
    lchunks.sort_by_key(|c| std::cmp::Reverse(c.end - c.start));
    let pairs: Vec<Vec<(u64, u64)>> = par_run(nt, lchunks.len(), Vec::new, |st: &mut Vec<(u64, u64)>, i| {
        let c = lchunks[i];
        scan_lines::<2>(&ctx.links[c.file as usize], c.start, c.end, check, |f| {
            if !f[0].is_empty() && !f[1].is_empty() {
                let a = unsafe { pk.pack_padded(f[0]) };
                let b = unsafe { pk.pack_padded(f[1]) };
                if a != b {
                    st.push((a, b));
                }
            }
        });
    });
    tlog(t0, "links parsed");

    // ---- events + clicks (one worker first builds the person map; clicks wait for it)
    let ev_bytes: usize = ctx.events.iter().map(|m| m.len()).sum();
    let ck_bytes: usize = ctx.clicks.iter().map(|m| m.len()).sum();
    let est_events = ev_bytes / 70 + 1;
    let max_bits: u32 = std::env::var("SS_EBITS").ok().and_then(|v| v.parse().ok()).unwrap_or(11);
    let mut bits = 8u32.min(max_bits);
    while bits < max_bits && (est_events >> bits) > 16_384 {
        bits += 1;
    }
    let pdelta: u32 = std::env::var("SS_PDELTA").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
    let parts = Parts { bits, pbits: bits.saturating_sub(pdelta).max(4) };
    let np = parts.n();
    let npp = parts.np();
    let ev_res = est_events * 6 / 5 / (nt * np) + 8;
    let ck_res = (ck_bytes / 45) * 6 / 5 / (nt * npp) + 8;
    let mut items = vec![Chunk { kind: 3, file: 0, start: 0, end: 0 }];
    let mut evc = Vec::new();
    make_chunks(&ctx.events, 1, 8 << 20, &mut evc);
    evc.sort_by_key(|c| std::cmp::Reverse(c.end - c.start));
    let mut ckc = Vec::new();
    make_chunks(&ctx.clicks, 2, 8 << 20, &mut ckc);
    ckc.sort_by_key(|c| std::cmp::Reverse(c.end - c.start));
    items.extend(evc);
    items.extend(ckc);
    let cws = ws - WEEK;
    let persons: std::sync::OnceLock<PersonMap> = std::sync::OnceLock::new();
    let parsed: Vec<ParseState> = par_run(
        nt,
        items.len(),
        || ParseState {
            ev: scatter::Scatter::new(np, ev_res),
            ck: scatter::Scatter::new(npp, ck_res),
            buf: Vec::new(),
        },
        |st, i| {
            let c = items[i];
            if c.kind == 3 {
                let _ = persons.set(build_persons(&pairs));
                tlog(t0, &format!("persons built ({} remapped IDs, table {} MB)", persons.get().unwrap().t.len() / 2, persons.get().unwrap().t.len() * 16 >> 20));
            } else if c.kind == 1 {
                let mut buf = std::mem::take(&mut st.buf);
                with_chunk(&ctx.events[c.file as usize], &c, ctx.use_read, &mut buf, |data, start, end| {
                scan_lines::<7>(data, start, end, check, |f| {
                    if f[0].is_empty() {
                        return;
                    }
                    let eid = unsafe { pk.pack_padded(f[0]) };
                    let ingest = ctx.tstab.ingest(f[3]);
                    let ing_s = (ingest.div_euclid(1000) - ws_s).clamp(-INGEST_OFF, INGEST_OFF - 1) + INGEST_OFF;
                    let mut rec = Ev { eid, uid: 0, meta: (ing_s as u64) << 16, tp: 0 };
                    'v: {
                        if f[1].is_empty() || f[5].is_empty() {
                            break 'v;
                        }
                        let ts = match ctx.tstab.parse(f[2]) {
                            Some(t) if t >= ws && t < we => t,
                            _ => break 'v,
                        };
                        // event_type: compare whole words (fields have 16 readable bytes)
                        let t = f[4];
                        let w = u64::from_le(unsafe { (t.as_ptr() as *const u64).read_unaligned() });
                        let purchase = if t.len() == 8 && w == u64::from_le_bytes(*b"purchase") {
                            true
                        } else if t.len() == 4
                            && ((w as u32) == u32::from_le_bytes(*b"view") || (w as u32) == u32::from_le_bytes(*b"cart"))
                        {
                            false
                        } else {
                            break 'v;
                        };
                        let mut price = 0u32;
                        if purchase {
                            match parse_price(f[6]) {
                                Some(p) => price = p,
                                None => break 'v,
                            }
                            rec.meta |= ctx.product_idx.get(f[5]).unwrap_or(ctx.unknown_cat) as u64;
                        }
                        if ingest.saturating_sub(ts) > DAY {
                            break 'v;
                        }
                        rec.uid = unsafe { pk.pack_padded(f[1]) };
                        rec.tp = ((ts - ws) as u64) << 26 | (price as u64) << 2 | 1 | if purchase { 2 } else { 0 };
                    }
                    st.ev.push(parts.by_eid(eid), rec);
                });
                });
                // order this thread's streaming stores before anyone reads the buffers
                scatter::fence();
                st.buf = buf;
            } else {
                let pm = loop {
                    if let Some(p) = persons.get() {
                        break p;
                    }
                    std::thread::yield_now();
                };
                const B: usize = 16;
                let mut pend = [(0u64, 0i64, 0u64, 0u16); B];
                let mut n = 0usize;
                let ck = &mut st.ck;
                let flush = |pend: &[(u64, i64, u64, u16)], ck: &mut scatter::Scatter<Ck, 2>| {
                    for &(uid, ts, cid, camp) in pend {
                        let person = pm.get(uid);
                        ck.push(parts.by_person(person), Ck { person, ts, cid, camp });
                    }
                };
                let mut buf = std::mem::take(&mut st.buf);
                with_chunk(&ctx.clicks[c.file as usize], &c, ctx.use_read, &mut buf, |data, start, end| {
                scan_lines::<4>(data, start, end, check, |f| {
                    if f[0].is_empty() || f[1].is_empty() {
                        return;
                    }
                    let Some(camp) = ctx.campaign_idx.get(f[2]) else { return };
                    let ts = match ctx.tstab.parse(f[3]) {
                        Some(t) if t >= cws && t < we => t,
                        _ => return,
                    };
                    let cid = unsafe { pk.pack_padded(f[0]) };
                    let uid = unsafe { pk.pack_padded(f[1]) };
                    pm.prefetch(uid);
                    pend[n] = (uid, ts, cid, camp);
                    n += 1;
                    if n == B {
                        flush(&pend, ck);
                        n = 0;
                    }
                });
                });
                st.buf = buf;
                flush(&pend[..n], ck);
                scatter::fence();
            }
        },
    );
    let mut parsed = parsed;
    for s in parsed.iter_mut() {
        s.ev.finish();
        s.ck.finish();
    }
    let parsed = parsed;
    tlog(t0, "events and clicks parsed");
    if pk.failed() {
        return None;
    }
    let persons = persons.get().expect("person map");

    // ---- dedup per event_id partition: keep the copy with the smallest ingest_ts
    let se_res = est_events * 6 / 5 / (nt * npp) + 8;
    let deduped: Vec<DedupState> = par_run(
        nt,
        np,
        || DedupState {
            out: (0..npp).map(|_| Vec::with_capacity(se_res)).collect(),
            outp: (0..npp).map(|_| Vec::with_capacity(se_res / 16 + 8)).collect(),
            table: Vec::new(),
            surv: Vec::new(),
        },
        |st, p| {
            let total: usize = parsed.iter().map(|s| s.ev.vecs[p].len()).sum();
            let cap = (total + total / 2).next_power_of_two().max(16);
            let mask = cap - 1;
            st.table.clear();
            st.table.resize(cap, Ev::default());
            let t = &mut st.table;
            // whole records in the table (it stays in L2): the kept copy per event_id
            for s in parsed.iter() {
                let v = &s.ev.vecs[p];
                for (k, e) in v.iter().enumerate() {
                    // prefetch the input stream and the table slot of a record a few ahead
                    prefetch(v.as_ptr().wrapping_add(k + 16));
                    if k + 8 < v.len() {
                        let a = unsafe { v.get_unchecked(k + 8) }.eid;
                        prefetch(t.as_ptr().wrapping_add(mix(a) as usize & mask));
                    }
                    let mut i = (mix(e.eid) as usize) & mask;
                    loop {
                        let x = unsafe { t.get_unchecked_mut(i) };
                        if x.eid == 0 {
                            *x = *e;
                            break;
                        }
                        if x.eid == e.eid {
                            if (e.meta >> 16) < (x.meta >> 16) {
                                *x = *e;
                            }
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
            st.surv.clear();
            for x in t.iter() {
                if x.tp & 1 != 0 {
                    st.surv.push(*x);
                }
            }
            // two-stage prefetch: the filter word far ahead, the table slot nearer
            const D1: usize = 16;
            const D2: usize = 6;
            let surv = &st.surv;
            for i in 0..surv.len() {
                if i + D1 < surv.len() {
                    persons.prefetch_filter(surv[i + D1].uid);
                }
                if i + D2 < surv.len() {
                    persons.prefetch(surv[i + D2].uid);
                }
                let e = surv[i];
                let person = persons.get(e.uid);
                let q = parts.by_person(person);
                let ts = ws + (e.tp >> 26) as i64;
                if e.tp & 2 == 0 {
                    st.out[q].push(Sv { person, ts });
                } else {
                    st.outp[q].push(Se {
                        person,
                        ts,
                        price: ((e.tp >> 2) & 0xff_ffff) as u32,
                        cat: (e.meta & 0xffff) as u16,
                        purchase: true,
                    });
                }
            }
        },
    );
    tlog(t0, "dedup done");

    // ---- sessions per person partition
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let ncamp = ctx.camp_ids.len();
    let organic = ncamp as u16;
    let ncat = ctx.cat_names.len();
    let mut order: Vec<(usize, usize)> =
        (0..npp).map(|q| (deduped.iter().map(|d| d.out[q].len() + d.outp[q].len()).sum::<usize>(), q)).collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
    let aggs: Vec<AggState> = par_run(
        nt,
        npp,
        || AggState {
            rows: Vec::new(),
            camp: vec![0; (ncamp + 1) * ndays * 4],
            asst: vec![0; (ncamp + 1) * ndays],
            catsum: vec![0; ncat],
            touched: Vec::new(),
            ccount: vec![0; ncamp],
            cpos: vec![0; ncamp],
            active: Vec::new(),
            g: Group::default(),
        },
        |st, oi| {
            let (n, q) = order[oi];
            let mut g = std::mem::take(&mut st.g);
            group_partition(&mut g, n, deduped.iter().map(|d| d.out[q].as_slice()), deduped.iter().map(|d| d.outp[q].as_slice()), parsed.iter().map(|s| s.ck.vecs[q].as_slice()));
            for l in 0..g.off.len() - 1 {
                let ev = &g.evs[g.off[l] as usize..g.off[l + 1] as usize];
                let person = g.pkey[l];
                let ck = &g.cks[g.coff[l] as usize..g.coff[l + 1] as usize];
                person_sessions(st, person, ev, ck, day0, ndays, organic);
            }
            st.g = g;
        },
    );
    tlog(t0, "sessions done");

    // ---- output
    let mut camp = vec![0i64; (ncamp + 1) * ndays * 4];
    for a in &aggs {
        for (x, y) in camp.iter_mut().zip(a.camp.iter()) {
            *x += *y;
        }
        for (i, &v) in a.asst.iter().enumerate() {
            camp[i * 4 + 3] += v as i64;
        }
    }
    let ud_dir = ctx.out_dir.join("user_daily");
    let cd_dir = ctx.out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud_dir).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd_dir).expect("mkdir campaign_daily");
    std::thread::scope(|s| {
        for (t, a) in aggs.iter().enumerate() {
            let path = ud_dir.join(format!("part-{t:03}.parquet"));
            let cat_names = &ctx.cat_names;
            s.spawn(move || parquet_out::write_user_daily(&path, &a.rows, pk, cat_names));
        }
        let path = cd_dir.join("part-000.parquet");
        let camp = &camp;
        s.spawn(move || {
            parquet_out::write_campaign_daily(&path, camp, ndays, day0, &ctx.camp_ids, &ctx.camp_channels)
        });
    });
    tlog(t0, "output written");
    if std::env::var_os("SS_DROP_TIMING").is_some() {
        drop(aggs);
        drop(deduped);
        drop(parsed);
        tlog(t0, "freed (timing only)");
    }
    Some(())
}

#[inline(always)]
fn cslot(camp: usize, day: usize, ndays: usize) -> usize {
    (camp * ndays + day) * 4
}

fn person_sessions(st: &mut AggState, person: u64, evs: &[Ev2], cks: &[Ck], day0: i64, ndays: usize, organic: u16) {
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day: i64 = i64::MIN;
    let (mut u_sess, mut u_events, mut u_rev) = (0i32, 0i32, 0i64);
    let mut k = 0;
    while k < evs.len() {
        let start = evs[k].ts;
        let day = start.div_euclid(DAY);
        if day != cur_day {
            if cur_day != i64::MIN {
                flush_ud(st, person, cur_day, u_sess, u_events, u_rev);
            }
            cur_day = day;
            u_sess = 0;
            u_events = 0;
            u_rev = 0;
        }
        let mut prev = start;
        let mut m = k;
        let (mut n, mut rev, mut purch) = (0i32, 0i64, false);
        while m < evs.len() && evs[m].ts - prev <= GAP {
            let e = evs[m];
            n += 1;
            if e.purchase() {
                rev += e.price() as i64;
                purch = true;
                let c = e.cat() as usize;
                if st.catsum[c] == 0 {
                    st.touched.push(e.cat());
                }
                st.catsum[c] += e.price() as i64;
            }
            prev = e.ts;
            m += 1;
        }
        u_sess += 1;
        u_events += n;
        u_rev += rev;

        // clicks with start - 7d <= ts <= start
        while hi < cks.len() && cks[hi].ts <= start {
            let c = cks[hi].camp as usize;
            st.ccount[c] += 1;
            if st.ccount[c] == 1 {
                st.cpos[c] = st.active.len() as u32;
                st.active.push(c as u16);
            }
            hi += 1;
        }
        while lo < hi && cks[lo].ts < start - WEEK {
            let c = cks[lo].camp as usize;
            st.ccount[c] -= 1;
            if st.ccount[c] == 0 {
                let p = st.cpos[c] as usize;
                let last = st.active.pop().unwrap();
                if last as usize != c {
                    st.active[p] = last;
                    st.cpos[last as usize] = p as u32;
                }
            }
            lo += 1;
        }
        let di = (day - day0) as usize;
        let credited = if hi > lo {
            let mt = cks[hi - 1].ts;
            let mut j = hi - 1;
            while j > lo && cks[j - 1].ts == mt {
                j -= 1;
            }
            for &a in &st.active {
                st.asst[a as usize * ndays + di] += 1;
            }
            cks[j].camp
        } else {
            organic
        };
        let s = cslot(credited as usize, di, ndays);
        st.camp[s] += 1;
        st.camp[s + 1] += purch as i64;
        st.camp[s + 2] += rev;
        k = m;
    }
    if cur_day != i64::MIN {
        flush_ud(st, person, cur_day, u_sess, u_events, u_rev);
    }
    for &a in &st.active {
        st.ccount[a as usize] = 0;
    }
    st.active.clear();
}

fn flush_ud(st: &mut AggState, person: u64, day: i64, sessions: i32, events: i32, revenue: i64) {
    let mut top = u16::MAX;
    let mut best = 0i64;
    for &c in &st.touched {
        let v = st.catsum[c as usize];
        if v > best || (v == best && c < top) {
            best = v;
            top = c;
        }
        st.catsum[c as usize] = 0;
    }
    st.touched.clear();
    st.rows.push(UdRow { person, day: day as i32, sessions, events, revenue, top });
}

/// Floor: read every byte of every input CSV on all threads (8 MB chunks), then exit.
fn bench_touch(ctx: &Ctx, t0: &Instant) {
    let mut chunks = Vec::new();
    make_chunks(&ctx.links, 0, 8 << 20, &mut chunks);
    make_chunks(&ctx.events, 1, 8 << 20, &mut chunks);
    make_chunks(&ctx.clicks, 2, 8 << 20, &mut chunks);
    chunks.sort_by_key(|c| std::cmp::Reverse(c.end - c.start));
    let sums = par_run(ctx.nthreads, chunks.len(), || 0u64, |acc, i| {
        let c = chunks[i];
        let m = match c.kind { 0 => &ctx.links, 1 => &ctx.events, _ => &ctx.clicks };
        let d = &m[c.file as usize][c.start..c.end];
        *acc += memchr::memchr_iter(b'\n', d).count() as u64;
    });
    tlog(t0, &format!("touched all inputs ({} lines)", sums.iter().sum::<u64>()));
}

/// Single-thread microbenchmarks over the event files (development only).
fn bench(ctx: &Ctx, mode: &str) {
    let t = Instant::now();
    let i0 = instructions();
    let mut n = 0u64;
    let mut acc = 0u64;
    let p7 = Pack7 { fail: AtomicBool::new(false) };
    for inp in &ctx.events {
        let d: &[u8] = inp;
        let start = memchr::memchr(b'\n', d).map_or(d.len(), |i| i + 1);
        match mode {
            "scan" => scan_lines::<7>(d, start, d.len(), false, |f| {
                n += 1;
                acc = acc.wrapping_add(f[2].len() as u64 + f[6].len() as u64);
            }),
            "decode" => scan_lines::<7>(d, start, d.len(), false, |f| {
                n += 1;
                let a = unsafe { p7.pack_padded(f[0]) } ^ unsafe { p7.pack_padded(f[1]) };
                let b = ctx.tstab.parse(f[2]).unwrap_or(0) ^ ctx.tstab.parse(f[3]).unwrap_or(0);
                acc = acc.wrapping_add(a ^ b as u64);
            }),
            "masks" => {
                let mut pos = start;
                let fe = d.len().saturating_sub(scan::FAST_SLACK);
                while pos < fe {
                    let (nl, cm, qm) = unsafe { scan::masks128(d.as_ptr().add(pos)) };
                    acc = acc.wrapping_add((cm ^ qm) as u64);
                    n += 1;
                    pos += if nl == 0 { 128 } else { nl.trailing_zeros() as usize + 1 };
                }
            }
            "memchr" => {
                for _ in memchr::memchr_iter(b'\n', &d[start..]) {
                    n += 1;
                }
            }
            "ts" => scan_lines::<7>(d, start, d.len(), false, |f| {
                n += 1;
                for _ in 0..10 {
                    acc = acc.wrapping_add(fastpath::ts_probe(&ctx.tstab, std::hint::black_box(f[3])).unwrap_or(0) as u64);
                }
            }),
            "pack" => scan_lines::<7>(d, start, d.len(), false, |f| {
                n += 1;
                for _ in 0..10 {
                    acc = acc.wrapping_add(unsafe { p7.pack_padded(std::hint::black_box(f[0])) });
                }
            }),
            "tsconst" => {
                if n == 0 {
                    let mut b = [0u8; 48];
                    b[..20].copy_from_slice(b"2026-10-03T23:58:10Z");
                    for _ in 0..10_000_000 {
                        n += 1;
                        acc = acc.wrapping_add(ctx.tstab.parse(std::hint::black_box(&b[..20])).unwrap_or(0) as u64);
                    }
                }
            }
            "tsnull" => {
                if n == 0 {
                    let mut b = [0u8; 48];
                    b[..20].copy_from_slice(b"2026-10-03T23:58:10Z");
                    for _ in 0..10_000_000 {
                        n += 1;
                        let f = std::hint::black_box(&b[..20]);
                        acc = acc.wrapping_add(f[3] as u64 + f.len() as u64);
                    }
                }
            }
            "tsin" => scan_lines::<7>(d, start, d.len(), false, |f| {
                n += 1;
                for _ in 0..10 {
                    acc = acc.wrapping_add(ctx.tstab.parse(std::hint::black_box(f[3])).unwrap_or(0) as u64);
                }
            }),
            "tsin2" => scan_lines::<7>(d, start, d.len(), false, |f| {
                n += 1;
                for _ in 0..10 {
                    acc = acc.wrapping_add(ctx.tstab.parse(std::hint::black_box(f[2])).unwrap_or(0) as u64);
                }
            }),
            "ingin" => scan_lines::<7>(d, start, d.len(), false, |f| {
                n += 1;
                for _ in 0..10 {
                    acc = acc.wrapping_add(ctx.tstab.ingest(std::hint::black_box(f[3])) as u64);
                }
            }),
            "packin" => scan_lines::<7>(d, start, d.len(), false, |f| {
                n += 1;
                for _ in 0..10 {
                    acc = acc.wrapping_add(unsafe { p7.pack_padded(std::hint::black_box(f[1])) });
                }
            }),
            "calib" => {
                if n == 0 {
                    for _ in 0..100_000_000 {
                        n += 1;
                        acc = std::hint::black_box(acc.wrapping_mul(3).wrapping_add(1));
                    }
                }
            }
            "tsconst_old" => {
                if n == 0 {
                    let mut b = [0u8; 48];
                    b[..20].copy_from_slice(b"2026-10-03T23:58:10Z");
                    for _ in 0..10_000_000 {
                        n += 1;
                        acc = acc.wrapping_add(parse_ts(std::hint::black_box(&b[..20])).unwrap_or(0) as u64);
                    }
                }
            }
            _ => panic!("unknown bench"),
        }
    }
    eprintln!(
        "bench {mode}: {n} lines, {:.1} ms, {:.0} instructions/line, {:.1} ns/line (acc {acc})",
        t.elapsed().as_secs_f64() * 1e3,
        (instructions() - i0) * 1e9 / n as f64,
        t.elapsed().as_secs_f64() * 1e9 / n as f64
    );
}

// ---------------------------------------------------------------- fallback key table

fn build_rank(ctx: &Ctx) -> RankPack {
    type Set<'a> = std::collections::HashSet<&'a [u8], BuildHasherDefault<Fx>>;
    fn add_from<'a>(set: &mut Set<'a>, maps: &'a [Input], n: usize) {
        for m in maps {
            let d: &'a [u8] = m;
            let start = memchr::memchr(b'\n', d).map(|i| i + 1).unwrap_or(d.len());
            for_lines(&d[start..], |line| {
                let mut f: [&[u8]; 7] = [b""; 7];
                let ok = match n {
                    2 => {
                        let mut g: [&[u8]; 2] = [b""; 2];
                        let ok = split(line, &mut g);
                        f[0] = g[0];
                        f[1] = g[1];
                        ok
                    }
                    4 => {
                        let mut g: [&[u8]; 4] = [b""; 4];
                        let ok = split(line, &mut g);
                        f[0] = g[0];
                        f[1] = g[1];
                        ok
                    }
                    _ => split(line, &mut f),
                };
                if ok {
                    for x in [f[0], f[1]] {
                        if !x.is_empty() {
                            set.insert(x);
                        }
                    }
                }
            });
        }
    }
    let mut set: Set = Default::default();
    add_from(&mut set, &ctx.links, 2);
    add_from(&mut set, &ctx.events, 7);
    add_from(&mut set, &ctx.clicks, 4);
    let mut strs: Vec<Vec<u8>> = set.into_iter().map(|s| s.to_vec()).collect();
    strs.sort_unstable();
    let mut map: FxMap<Vec<u8>, u64> = FxMap::default();
    for (i, s) in strs.iter().enumerate() {
        map.insert(s.clone(), i as u64 + 1);
    }
    RankPack { map, strs }
}

// ---------------------------------------------------------------- main

fn main() {
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().skip(1).filter(|a| !a.starts_with("--")).collect();
    let (data, out) = if args.len() >= 2 {
        (PathBuf::from(&args[args.len() - 2]), PathBuf::from(&args[args.len() - 1]))
    } else {
        (
            PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into())),
            PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into())),
        )
    };
    let nthreads = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));

    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = manifest_ts(&manifest, "window_start");
    let we = manifest_ts(&manifest, "window_end");

    // products: category names sorted byte-wise (plus "unknown"), so a smaller index wins ties
    let pdata = std::fs::read(data.join("products.csv")).unwrap_or_default();
    let prows = two_col_rows(&pdata);
    let mut cat_first: FxMap<&[u8], u16> = FxMap::default();
    for &(_, c) in &prows {
        let n = cat_first.len() as u16;
        cat_first.entry(c).or_insert(n);
    }
    let mut cat_names: Vec<Vec<u8>> = cat_first.keys().map(|c| c.to_vec()).collect();
    cat_names.push(b"unknown".to_vec());
    cat_names.sort_unstable();
    cat_names.dedup();
    let cat_idx: FxMap<&[u8], u16> = cat_names.iter().enumerate().map(|(i, c)| (c.as_slice(), i as u16)).collect();
    let unknown_cat = cat_idx[&b"unknown"[..]];
    let product_idx = fastpath::IdIndex::new(b"p", prows.iter().map(|&(p, c)| (p, cat_idx[c])));
    let cdata = std::fs::read(data.join("campaigns.csv")).unwrap_or_default();
    let mut seen: FxMap<&[u8], u16> = FxMap::default();
    let mut camp_ids = Vec::new();
    let mut camp_channels = Vec::new();
    for (c, ch) in two_col_rows(&cdata) {
        if seen.contains_key(c) {
            continue;
        }
        seen.insert(c, camp_ids.len() as u16);
        camp_ids.push(c.to_vec());
        camp_channels.push(ch.to_vec());
    }
    let campaign_idx = fastpath::IdIndex::new(b"c_", seen.iter().map(|(&c, &i)| (c, i)));
    let ctx = Ctx {
        nthreads,
        ws,
        we,
        cat_names,
        unknown_cat,
        camp_ids,
        camp_channels,
        links: map_files(&list_csv(&data.join("links"))),
        events: map_files(&list_csv(&data.join("events"))),
        clicks: map_files(&list_csv(&data.join("clicks"))),
        out_dir: out,
        check_split: std::env::var_os("SS_CHECK_SPLIT").is_some(),
        tstab: fastpath::TsTab::new(ws - WEEK, we),
        use_read: std::env::var_os("SS_READ").is_some(),
        product_idx,
        campaign_idx,
    };
    tlog(&t0, "inputs mapped");

    if let Ok(mode) = std::env::var("SS_BENCH") {
        if mode == "touch" {
            bench_touch(&ctx, &t0);
        } else {
            bench(&ctx, &mode);
        }
        std::process::exit(0);
    }
    let p7 = Pack7 { fail: AtomicBool::new(false) };
    // SS_FORCE_RANK=1 forces the fallback key path (used by the tests).
    let force_rank = std::env::var_os("SS_FORCE_RANK").is_some();
    if force_rank || run(&ctx, &p7, &t0).is_none() {
        eprintln!("an ID does not fit the packed key; rerunning with ranked keys");
        let rp = build_rank(&ctx);
        tlog(&t0, "rank table built");
        run(&ctx, &rp, &t0).expect("ranked run");
    }
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rng(s: &mut u64) -> u64 {
        *s ^= *s << 13;
        *s ^= *s >> 7;
        *s ^= *s << 17;
        *s
    }

    #[test]
    fn pack_padded_matches_pack() {
        let p = Pack7 { fail: AtomicBool::new(false) };
        let alphabet: &[u8] = b"\x00\x01\t ,\"azAZ09_\x7f\x80\xc3\xff";
        let mut s = 0x1234_5678_9abc_def1u64;
        for _ in 0..2_000_000 {
            let n = (rng(&mut s) % 12) as usize;
            let mut buf = [0u8; 32];
            for b in buf.iter_mut() {
                *b = alphabet[(rng(&mut s) % alphabet.len() as u64) as usize];
            }
            let f = &buf[..n];
            p.fail.store(false, Relaxed);
            let a = p.pack(f);
            let fa = p.failed();
            p.fail.store(false, Relaxed);
            let b = unsafe { p.pack_padded(f) };
            let fb = p.failed();
            assert_eq!(fa, fb, "{f:?}");
            if !fa {
                assert_eq!(a, b, "{f:?}");
            }
        }
    }

    #[test]
    fn fast_ts_matches_parse_ts() {
        let ws = 1_791_072_000_000i64; // 2026-10-04
        let tab = fastpath::TsTab::new(ws - WEEK, ws + 14 * DAY);
        let mut s = 0x5555_1234_9999_0001u64;
        let digits = b"0123456789";
        let pieces: [&[u8]; 10] = [b"2026-10-03T23:58:10Z", b"2028-02-29T00:00:00Z", b"2027-02-29T10:00:00Z", b"2026-09-30T23:59:59+05:30", b"2026-10-14T23:59:59-10:00", b"1791071890000", b"2026-12-31T23:59:59Z", b"0000-01-01T00:00:00Z", b"9999999999999", b"0000000000000"];
        for _ in 0..3_000_000 {
            let mut buf = [0u8; 48];
            let src = pieces[(rng(&mut s) % pieces.len() as u64) as usize];
            buf[..src.len()].copy_from_slice(src);
            let n = src.len();
            // mutate 0-2 bytes into digits, separators or junk
            for _ in 0..(rng(&mut s) % 3) {
                let i = (rng(&mut s) % n as u64) as usize;
                let r = rng(&mut s);
                buf[i] = match r % 4 { 0 | 1 => digits[(r >> 8) as usize % 10], 2 => { let j: &[u8] = b"-:TZ+ tz\xff/"; j[(r >> 8) as usize % j.len()] }, _ => (r >> 16) as u8 };
            }
            let f = &buf[..n];
            assert_eq!(tab.parse(f), parse_ts(f), "{:?}", String::from_utf8_lossy(f));
            if n == 20 && f[19] == b'Z' {
                if let Some(v) = parse_ts(f) {
                    assert_eq!(tab.ingest(f), v, "{:?}", String::from_utf8_lossy(f));
                }
            }
        }
    }

    fn check_fields<const N: usize>(alphabet: &[u8], maxlen: u64, iters: usize, seed: u64) {
        let mut s = seed;
        for _ in 0..iters {
            let n = (rng(&mut s) % maxlen) as usize;
            let mut buf = [b'x'; 300];
            for b in buf[..n].iter_mut() {
                *b = alphabet[(rng(&mut s) % alphabet.len() as u64) as usize];
            }
            buf[n] = b'\n';
            let (nl, cm, qm) = unsafe { scan::masks128(buf.as_ptr()) };
            if nl == 0 {
                continue;
            }
            let len = nl.trailing_zeros() as usize;
            let line = &buf[..len];
            let mut a: [&[u8]; N] = [b""; N];
            let mut b: [&[u8]; N] = [b""; N];
            let oa = scan::fields::<N>(line, cm, qm, &mut a);
            let ob = split::<N>(line, &mut b);
            assert_eq!(oa, ob, "{:?}", String::from_utf8_lossy(line));
            if oa {
                assert_eq!(a, b);
            }
        }
    }

    #[test]
    fn fast_fields_match_split() {
        check_fields::<4>(b"\"\",,,a1 \r\t\n", 40, 2_000_000, 0x9e37_79b9_7f4a_7c15);
        check_fields::<7>(b"\"\",,,,,a1 \r", 127, 2_000_000, 0x1234_9e37_79b9_7f4a);
        check_fields::<7>(b"\",aaaaaaaaaaaaaaaa", 127, 2_000_000, 0x7f4a_1234_9e37_79b9);
        check_fields::<2>(b"\"\",a\t", 12, 1_000_000, 0x55aa_1234_9e37_79b9);
    }
}
