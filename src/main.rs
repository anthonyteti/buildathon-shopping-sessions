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

mod parquet_out;

use memmap2::Mmap;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

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

pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

/// f[0..19] = YYYY-MM-DDTHH:MM:SS -> epoch ms.
#[inline(always)]
fn civil_ms(f: &[u8]) -> Option<i64> {
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
fn parse_ts(f: &[u8]) -> Option<i64> {
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
    fn unpack(&self, k: u64, out: &mut Vec<u8>);
    fn failed(&self) -> bool;
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

/// Open addressing u64 -> u64 (key 0 = empty). Holds only IDs whose person name differs.
struct PersonMap {
    keys: Vec<u64>,
    vals: Vec<u64>,
    mask: usize,
}
impl PersonMap {
    fn new(n: usize) -> Self {
        let cap = (n * 2).next_power_of_two().max(16);
        PersonMap { keys: vec![0; cap], vals: vec![0; cap], mask: cap - 1 }
    }
    fn insert(&mut self, k: u64, v: u64) {
        let mut i = mix(k) as usize & self.mask;
        loop {
            if self.keys[i] == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                return;
            }
            if self.keys[i] == k {
                self.vals[i] = v;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> u64 {
        let mut i = mix(k) as usize & self.mask;
        loop {
            let s = self.keys[i];
            if s == k {
                return self.vals[i];
            }
            if s == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
}

fn build_persons(pairs: &[Vec<(u64, u64)>]) -> PersonMap {
    let total: usize = pairs.iter().map(|v| v.len()).sum();
    let mut keys: Vec<u64> = Vec::with_capacity(total * 2);
    for v in pairs {
        for &(a, b) in v {
            keys.push(a);
            keys.push(b);
        }
    }
    keys.sort_unstable();
    keys.dedup();
    let n = keys.len();
    let mut idx: FxMap<u64, u32> = FxMap::default();
    idx.reserve(n);
    for (i, &k) in keys.iter().enumerate() {
        idx.insert(k, i as u32);
    }
    let mut parent: Vec<u32> = (0..n as u32).collect();
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let gp = p[p[x as usize] as usize];
            p[x as usize] = gp;
            x = gp;
        }
        x
    }
    for v in pairs {
        for &(a, b) in v {
            let ra = find(&mut parent, idx[&a]);
            let rb = find(&mut parent, idx[&b]);
            if ra < rb {
                parent[rb as usize] = ra;
            } else if rb < ra {
                parent[ra as usize] = rb;
            }
        }
    }
    let mut changed = 0;
    for i in 0..n as u32 {
        if find(&mut parent, i) != i {
            changed += 1;
        }
    }
    let mut pm = PersonMap::new(changed);
    for i in 0..n as u32 {
        let r = find(&mut parent, i);
        if r != i {
            // the root is the smallest index of its component = the byte-wise smallest ID
            pm.insert(keys[i as usize], keys[r as usize]);
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

fn map_files(paths: &[PathBuf]) -> Vec<Mmap> {
    let mut out = Vec::new();
    for p in paths {
        let f = std::fs::File::open(p).expect("open csv");
        let len = f.metadata().map(|m| m.len()).unwrap_or(0);
        if len == 0 {
            continue;
        }
        let m = unsafe { Mmap::map(&f) }.expect("mmap csv");
        out.push(m);
    }
    out
}

#[derive(Clone, Copy)]
struct Chunk {
    kind: u8, // 0 links, 1 events, 2 clicks
    file: u32,
    start: usize,
    end: usize,
}

fn make_chunks(maps: &[Mmap], kind: u8, target: usize, out: &mut Vec<Chunk>) {
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

fn read_two_col(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let data = std::fs::read(path).unwrap_or_default();
    let mut out = Vec::new();
    let mut first = true;
    for line in data.split(|&b| b == b'\n') {
        if first {
            first = false;
            continue;
        }
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.is_empty() {
            continue;
        }
        let mut it = line.splitn(2, |&b| b == b',');
        let a = clean_field(it.next().unwrap_or(b""));
        let b = clean_field(it.next().unwrap_or(b""));
        out.push((a.to_vec(), b.to_vec()));
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
    products: FxMap<Vec<u8>, u16>,
    cat_names: Vec<Vec<u8>>,
    unknown_cat: u16,
    campaigns: FxMap<Vec<u8>, u16>,
    camp_ids: Vec<Vec<u8>>,
    camp_channels: Vec<Vec<u8>>,
    links: Vec<Mmap>,
    events: Vec<Mmap>,
    clicks: Vec<Mmap>,
    out_dir: PathBuf,
}

// ---------------------------------------------------------------- parallel helper

fn par_run<S: Send, I: Fn() -> S + Sync, F: Fn(&mut S, usize) + Sync>(nt: usize, n: usize, init: I, f: F) -> Vec<S> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut st = init();
                    loop {
                        let i = next.fetch_add(1, Relaxed);
                        if i >= n {
                            break;
                        }
                        f(&mut st, i);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
struct Ev {
    eid: u64,
    ingest: i64,
    person: u64,
    ts: i64,
    price: u32,
    cat: u16,
    flags: u8, // 1 = valid, 2 = purchase
}

#[derive(Clone, Copy)]
struct Se {
    person: u64,
    ts: i64,
    price: u32,
    cat: u16,
    purchase: bool,
}

#[derive(Clone, Copy)]
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

const P1_BITS: u32 = 9;
const P2_BITS: u32 = 9;
const P1: usize = 1 << P1_BITS;
const P2: usize = 1 << P2_BITS;

#[inline(always)]
fn part1(k: u64) -> usize {
    (k.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - P1_BITS)) as usize
}
#[inline(always)]
fn part2(k: u64) -> usize {
    (k.wrapping_mul(0xC2B2_AE3D_27D4_EB4F) >> (64 - P2_BITS)) as usize
}

struct ParseState {
    ev: Vec<Vec<Ev>>,
    ck: Vec<Vec<Ck>>,
}

struct AggState {
    rows: Vec<UdRow>,
    camp: Vec<i64>, // [(camp) * ndays + day] * 4
    catsum: Vec<i64>,
    touched: Vec<u16>,
    ccount: Vec<u32>,
    cpos: Vec<u32>,
    active: Vec<u16>,
}

// ---------------------------------------------------------------- the pipeline

fn tlog(t0: &Instant, what: &str) {
    eprintln!("[{:8.1} ms] {what}", t0.elapsed().as_secs_f64() * 1e3);
}

/// Returns None when the packer reports an ID it cannot represent.
fn run<P: Packer>(ctx: &Ctx, pk: &P, t0: &Instant) -> Option<()> {
    let nt = ctx.nthreads;
    let (ws, we) = (ctx.ws, ctx.we);

    // ---- links
    let mut lchunks = Vec::new();
    make_chunks(&ctx.links, 0, 4 << 20, &mut lchunks);
    lchunks.sort_by_key(|c| std::cmp::Reverse(c.end - c.start));
    let pairs: Vec<Vec<(u64, u64)>> = par_run(nt, lchunks.len(), Vec::new, |st: &mut Vec<(u64, u64)>, i| {
        let c = lchunks[i];
        let d = &ctx.links[c.file as usize][c.start..c.end];
        let mut f: [&[u8]; 2] = [b""; 2];
        for_lines(d, |line| {
            if split(line, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
                let a = pk.pack(f[0]);
                let b = pk.pack(f[1]);
                if a != b {
                    st.push((a, b));
                }
            }
        });
    });
    tlog(t0, "links parsed");
    let persons = build_persons(&pairs);
    tlog(t0, "persons built");

    // ---- events + clicks
    let mut chunks = Vec::new();
    make_chunks(&ctx.events, 1, 8 << 20, &mut chunks);
    make_chunks(&ctx.clicks, 2, 8 << 20, &mut chunks);
    chunks.sort_by_key(|c| std::cmp::Reverse(c.end - c.start));
    let cws = ws - WEEK;
    let parsed: Vec<ParseState> = par_run(
        nt,
        chunks.len(),
        || ParseState { ev: (0..P1).map(|_| Vec::new()).collect(), ck: (0..P2).map(|_| Vec::new()).collect() },
        |st, i| {
            let c = chunks[i];
            if c.kind == 1 {
                let d = &ctx.events[c.file as usize][c.start..c.end];
                let mut f: [&[u8]; 7] = [b""; 7];
                for_lines(d, |line| {
                    if !split(line, &mut f) || f[0].is_empty() {
                        return;
                    }
                    let eid = pk.pack(f[0]);
                    let ingest = parse_ts(f[3]).unwrap_or(i64::MAX);
                    let mut rec = Ev { eid, ingest, person: 0, ts: 0, price: 0, cat: 0, flags: 0 };
                    'v: {
                        if f[1].is_empty() || f[5].is_empty() {
                            break 'v;
                        }
                        let ts = match parse_ts(f[2]) {
                            Some(t) if t >= ws && t < we => t,
                            _ => break 'v,
                        };
                        let purchase = match f[4] {
                            b"view" | b"cart" => false,
                            b"purchase" => true,
                            _ => break 'v,
                        };
                        if purchase {
                            match parse_price(f[6]) {
                                Some(p) => rec.price = p,
                                None => break 'v,
                            }
                            rec.cat = *ctx.products.get(f[5]).unwrap_or(&ctx.unknown_cat);
                        }
                        if ingest.saturating_sub(ts) > DAY {
                            break 'v;
                        }
                        rec.ts = ts;
                        rec.person = persons.get(pk.pack(f[1]));
                        rec.flags = 1 | if purchase { 2 } else { 0 };
                    }
                    st.ev[part1(eid)].push(rec);
                });
            } else {
                let d = &ctx.clicks[c.file as usize][c.start..c.end];
                let mut f: [&[u8]; 4] = [b""; 4];
                for_lines(d, |line| {
                    if !split(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                        return;
                    }
                    let Some(&camp) = ctx.campaigns.get(f[2]) else { return };
                    let ts = match parse_ts(f[3]) {
                        Some(t) if t >= cws && t < we => t,
                        _ => return,
                    };
                    let cid = pk.pack(f[0]);
                    let person = persons.get(pk.pack(f[1]));
                    st.ck[part2(person)].push(Ck { person, ts, cid, camp });
                });
            }
        },
    );
    tlog(t0, "events and clicks parsed");
    if pk.failed() {
        return None;
    }

    // ---- dedup per event_id partition
    let deduped: Vec<Vec<Vec<Se>>> = par_run(
        nt,
        P1,
        || (0..P2).map(|_| Vec::new()).collect::<Vec<Vec<Se>>>(),
        |out, p| {
            let total: usize = parsed.iter().map(|s| s.ev[p].len()).sum();
            let mut v: Vec<Ev> = Vec::with_capacity(total);
            for s in &parsed {
                v.extend_from_slice(&s.ev[p]);
            }
            v.sort_unstable_by_key(|e| (e.eid, e.ingest));
            let mut i = 0;
            while i < v.len() {
                let e = v[i];
                let mut j = i + 1;
                while j < v.len() && v[j].eid == e.eid {
                    j += 1;
                }
                if e.flags & 1 != 0 {
                    out[part2(e.person)].push(Se {
                        person: e.person,
                        ts: e.ts,
                        price: e.price,
                        cat: e.cat,
                        purchase: e.flags & 2 != 0,
                    });
                }
                i = j;
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
    let mut order: Vec<(usize, usize)> = (0..P2)
        .map(|q| (deduped.iter().map(|d| d[q].len()).sum::<usize>(), q))
        .collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
    let aggs: Vec<AggState> = par_run(
        nt,
        P2,
        || AggState {
            rows: Vec::new(),
            camp: vec![0; (ncamp + 1) * ndays * 4],
            catsum: vec![0; ncat],
            touched: Vec::new(),
            ccount: vec![0; ncamp],
            cpos: vec![0; ncamp],
            active: Vec::new(),
        },
        |st, oi| {
            let q = order[oi].1;
            let mut evs: Vec<Se> = Vec::with_capacity(order[oi].0);
            for d in &deduped {
                evs.extend_from_slice(&d[q]);
            }
            evs.sort_unstable_by_key(|e| (e.person, e.ts));
            let mut cks: Vec<Ck> = Vec::new();
            for s in &parsed {
                cks.extend_from_slice(&s.ck[q]);
            }
            cks.sort_unstable_by_key(|c| (c.person, c.ts, c.cid));
            let mut ci = 0;
            let mut i = 0;
            while i < evs.len() {
                let person = evs[i].person;
                let mut j = i + 1;
                while j < evs.len() && evs[j].person == person {
                    j += 1;
                }
                while ci < cks.len() && cks[ci].person < person {
                    ci += 1;
                }
                let c0 = ci;
                while ci < cks.len() && cks[ci].person == person {
                    ci += 1;
                }
                person_sessions(st, person, &evs[i..j], &cks[c0..ci], day0, ndays, organic);
                i = j;
            }
        },
    );
    tlog(t0, "sessions done");

    // ---- output
    let mut camp = vec![0i64; (ncamp + 1) * ndays * 4];
    for a in &aggs {
        for (x, y) in camp.iter_mut().zip(a.camp.iter()) {
            *x += *y;
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
    Some(())
}

#[inline(always)]
fn cslot(camp: usize, day: usize, ndays: usize) -> usize {
    (camp * ndays + day) * 4
}

fn person_sessions(st: &mut AggState, person: u64, evs: &[Se], cks: &[Ck], day0: i64, ndays: usize, organic: u16) {
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
            if e.purchase {
                rev += e.price as i64;
                purch = true;
                let c = e.cat as usize;
                if st.catsum[c] == 0 {
                    st.touched.push(e.cat);
                }
                st.catsum[c] += e.price as i64;
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
                st.camp[cslot(a as usize, di, ndays) + 3] += 1;
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

// ---------------------------------------------------------------- fallback key table

fn build_rank(ctx: &Ctx) -> RankPack {
    type Set<'a> = std::collections::HashSet<&'a [u8], BuildHasherDefault<Fx>>;
    fn add_from<'a>(set: &mut Set<'a>, maps: &'a [Mmap], n: usize) {
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

    let prows = read_two_col(&data.join("products.csv"));
    let mut cat_names: Vec<Vec<u8>> = prows.iter().map(|r| r.1.clone()).collect();
    cat_names.push(b"unknown".to_vec());
    cat_names.sort_unstable();
    cat_names.dedup();
    let cat_idx: FxMap<Vec<u8>, u16> = cat_names.iter().enumerate().map(|(i, c)| (c.clone(), i as u16)).collect();
    let unknown_cat = cat_idx[&b"unknown"[..]];
    let mut products: FxMap<Vec<u8>, u16> = FxMap::default();
    for (p, c) in prows {
        products.entry(p).or_insert(cat_idx[&c]);
    }
    let crows = read_two_col(&data.join("campaigns.csv"));
    let mut campaigns: FxMap<Vec<u8>, u16> = FxMap::default();
    let mut camp_ids = Vec::new();
    let mut camp_channels = Vec::new();
    for (c, ch) in crows {
        if campaigns.contains_key(&c) {
            continue;
        }
        campaigns.insert(c.clone(), camp_ids.len() as u16);
        camp_ids.push(c);
        camp_channels.push(ch);
    }

    let ctx = Ctx {
        nthreads,
        ws,
        we,
        products,
        cat_names,
        unknown_cat,
        campaigns,
        camp_ids,
        camp_channels,
        links: map_files(&list_csv(&data.join("links"))),
        events: map_files(&list_csv(&data.join("events"))),
        clicks: map_files(&list_csv(&data.join("clicks"))),
        out_dir: out,
    };
    tlog(&t0, "inputs mapped");

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
