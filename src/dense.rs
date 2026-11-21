//! Dense-index pipeline, tried first. It applies when the input uses the reference
//! generator's ID scheme, which it checks at run time:
//!
//!   event_id = "e" + base36(scramble(event_index, key_e))
//!   user_id  = "u" + base36(scramble(user_index * 512 + alias, key_u))
//!   click_id = "k" + base36(scramble(click_index, key_c))
//!
//! `scramble` is a fixed bijection on 40 bits and each key is 40 bits per dataset. The keys
//! are recovered from a sample of rows (meet in the middle); if that fails, this path
//! returns None before parsing and the caller runs the other pipelines.
//!
//! With the keys, every canonical ID decodes to its index, exactly and reversibly. Then:
//!   1. each parsed event copy goes to slot `event_index` of one dense array (scattered by
//!      index range); dedup is "keep the smaller ingest_ts in the slot", no hashing;
//!   2. a scan of the dense array cuts it into segments: maximal runs of kept valid events
//!      with the same user_index u (clicks likewise, by click_index);
//!   3. a person is handled in place when all of its IDs that decode name one u, that u
//!      forms a single event segment and a single click segment, and none of its rows has an
//!      ID outside the scheme. Its events and clicks are then exactly the records of that
//!      u's segments with its aliases. Everything else (shared-device persons, IDs that
//!      do not decode, u values in several segments, ...) goes to a general slow path that
//!      groups by person name and sorts, like the other pipelines.
//! None of this assumes the keys are "right": any key gives a bijection, and the segment
//! checks decide what is handled in place. Wrong keys only make the slow path big.
use super::*;
use crate::fused::CMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

const MASK40: u64 = (1 << 40) - 1;
const M1: u64 = 0x9E37_79B9_7F4B & MASK40;
const M2: u64 = 0xD6E8_FEB8_6659 & MASK40;
const fn inv64(a: u64) -> u64 {
    let mut x = a;
    let mut i = 0;
    while i < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        i += 1;
    }
    x
}
const M1I: u64 = inv64(M1) & MASK40;
const M2I: u64 = inv64(M2) & MASK40;
/// user_index * 512 + alias must be below this (u < 2^22) to be handled in place.
const XU_LIMIT: u64 = 1 << 31;
/// Flag in a record's user field: the user ID is outside the scheme; the low 31 bits
/// are thread (4 bits) and index (27 bits) into that thread's side table.
const EXO: u32 = 1 << 31;
const MULTI: u64 = u64::MAX;
const H8: u64 = 0x8080_8080_8080_8080;
const ONES: u64 = 0x0101_0101_0101_0101;
/// Slow-path person partitions.
const NSP: usize = 256;
#[inline(always)]
fn hp(p: u64) -> usize {
    (mix(p) >> 56) as usize
}
const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

#[inline(always)]
pub fn scramble(x: u64, k: u64) -> u64 {
    let mut m = (x.wrapping_mul(M1) ^ k) & MASK40;
    m ^= m >> 17;
    m.wrapping_mul(M2) & MASK40
}
/// Inverse of everything in `scramble` after the key: returns (x * M1) ^ key.
#[inline(always)]
fn unmix(v: u64) -> u64 {
    let m = v.wrapping_mul(M2I) & MASK40;
    m ^ (m >> 17) ^ (m >> 34)
}
#[inline(always)]
fn unkey(z: u64, k: u64) -> u64 {
    ((z ^ k) & MASK40).wrapping_mul(M1I) & MASK40
}

/// `prefix` then 1..=8 lowercase base-36 digits, no leading zero, value < 2^40 -> value.
/// Needs 16 readable bytes from `f`'s start.
#[inline(always)]
unsafe fn b36(f: &[u8], prefix: u8) -> Option<u64> {
    let n = f.len();
    if n < 2 || n > 9 || unsafe { *f.get_unchecked(0) } != prefix {
        return None;
    }
    let m = n - 1;
    let w = u64::from_le(unsafe { (f.as_ptr().add(1) as *const u64).read_unaligned() });
    // last digit in byte 0, first digit in byte m-1, zeros above
    let b = (w << (8 * (8 - m))).swap_bytes();
    let lm = if m == 8 { u64::MAX } else { (1u64 << (8 * m)) - 1 };
    if b & H8 != 0 {
        return None;
    }
    let ge30 = b.wrapping_add(0x50 * ONES) & H8;
    let ge3a = b.wrapping_add(0x46 * ONES) & H8;
    let ge61 = b.wrapping_add(0x1f * ONES) & H8;
    let ge7b = b.wrapping_add(0x05 * ONES) & H8;
    let ok = (ge30 & !ge3a) | (ge61 & !ge7b);
    if ok & lm != H8 & lm {
        return None;
    }
    let alpha = ge61 & lm;
    let d = b - (0x30 * ONES & lm) - (alpha >> 7) * 0x27;
    if m > 1 && (d >> (8 * (m - 1))) & 0xff == 0 {
        return None;
    }
    let p1 = (d & 0x00ff_00ff_00ff_00ff) + ((d >> 8) & 0x00ff_00ff_00ff_00ff) * 36;
    let p2 = (p1 & 0x0000_ffff_0000_ffff) + ((p1 >> 16) & 0x0000_ffff_0000_ffff) * 1296;
    let v = (p2 & 0xffff_ffff) + (p2 >> 32) * 1_679_616;
    if v > MASK40 { None } else { Some(v) }
}

/// The index of a canonical ID under `key`.
#[inline(always)]
unsafe fn xid(f: &[u8], prefix: u8, key: u64) -> Option<u64> {
    unsafe { b36(f, prefix) }.map(|v| unkey(unmix(v), key))
}

/// The packed (Pack7) key of the ID with index x: prefix + base36(scramble(x, key)).
#[inline(always)]
fn id_key(prefix: u8, x: u64, key: u64) -> u64 {
    let mut v = scramble(x, key);
    let mut digits = [0u8; 8];
    let mut n = 0;
    loop {
        digits[n] = DIGITS[(v % 36) as usize];
        n += 1;
        v /= 36;
        if v == 0 {
            break;
        }
    }
    let mut k = (prefix as u64) << 56;
    for j in 0..n {
        k |= (digits[n - 1 - j] as u64) << (56 - 7 * (j + 1));
    }
    k
}

/// The bytes of a Pack7 key.
fn key_bytes(k: u64, out: &mut [u8; 24]) -> usize {
    let mut n = 0;
    for j in 0..9 {
        let b = ((k >> (56 - 7 * j)) & 0x7f) as u8;
        if b == 0 {
            break;
        }
        out[n] = b;
        n += 1;
    }
    n
}

/// user_index * 512 + alias of a packed user ID key, or u32::MAX if outside the scheme.
fn xu_of_key(k: u64, ku: u64) -> u32 {
    let mut buf = [0u8; 24];
    let n = key_bytes(k, &mut buf);
    match unsafe { xid(&buf[..n], b'u', ku) } {
        Some(x) if x < XU_LIMIT => x as u32,
        _ => u32::MAX,
    }
}

/// A linked ID inside the scheme: its user index, alias and person name (bit 63: the
/// person spans more than one user index or has IDs outside the scheme).
#[derive(Clone, Copy, Default)]
struct Ul {
    u: u32,
    alias: u32,
    name: u64,
}

// ---------------------------------------------------------------- key recovery

/// A key k such that (z ^ k) * M1^-1 < 2^t for the pivot and at least 90% of `tests`,
/// with the low `low_bits` bits of k equal to `low`. Meet in the middle: 2^(t - low_bits)
/// candidates when t >= 20.
fn mitm(nt: usize, pivot: u64, tests: &[u64], t: u32, low_bits: u32, low: u64) -> Option<u64> {
    // split point: about 2^(t - low_bits) + 2^(sp - low_bits + 1) candidates
    let sp: u32 = (t / 2).max(low_bits + 2).min(20);
    let smask = (1u64 << sp) - 1;
    let hmask = (1u64 << (40 - sp)) - 1;
    let bound = 1u64 << t;
    let need = tests.len() * 9 / 10;
    let per = (1u64 << (t.saturating_sub(sp))) + 2;
    let nlow = 1u64 << (sp - low_bits);
    let blocks = 64u64.min(nlow);
    let found = AtomicU64::new(u64::MAX);
    par_run(nt.min(blocks as usize), blocks as usize, || (), |_, bi| {
        let (j0, j1) = (bi as u64 * nlow / blocks, (bi as u64 + 1) * nlow / blocks);
        for j in j0..j1 {
            if found.load(Relaxed) != u64::MAX {
                return;
            }
            let kl = low | (j << low_bits);
            let a = (((pivot ^ kl) & smask).wrapping_mul(M1I)) & MASK40;
            let neg = (MASK40 + 1 - a) & MASK40;
            let b0 = neg >> sp;
            for d in 0..per {
                let b = (b0 + d) & hmask;
                let kh = (b.wrapping_mul(M1) & hmask) ^ (pivot >> sp);
                let k = (kh << sp) | kl;
                if unkey(pivot, k) >= bound {
                    continue;
                }
                // a wrong key fails nearly every test: stop at the first few failures
                let allowed = tests.len() - need;
                let mut fails = 0;
                for &z in tests {
                    fails += (unkey(z, k) >= bound) as usize;
                    if fails > allowed {
                        break;
                    }
                }
                if fails <= allowed {
                    found.store(k, Relaxed);
                    return;
                }
            }
        }
    });
    let k = found.load(Relaxed);
    (k != u64::MAX).then_some(k)
}

/// Tries a few pivots and two bounds.
fn mitm_retry(nt: usize, zs: &[u64], t: u32, low_bits: u32, low: u64) -> Option<u64> {
    if zs.len() < 9 {
        return None;
    }
    for t in [t, t + 2] {
        for p in 0..3.min(zs.len()) {
            let tests: Vec<u64> = zs.iter().enumerate().filter(|&(i, _)| i != p).map(|(_, &z)| z).take(48).collect();
            if let Some(k) = mitm(nt, zs[p], &tests, t.min(40), low_bits, low) {
                return Some(k);
            }
        }
    }
    None
}

/// First lines of up to `files` of the largest files (after the header), split into N fields.
fn sample_rows<const N: usize>(maps: &[Input], files: usize, lines: usize, mut f: impl FnMut(&[&[u8]; N])) -> (usize, usize) {
    let mut order: Vec<usize> = (0..maps.len()).collect();
    order.sort_by_key(|&i| std::cmp::Reverse(maps[i].len()));
    let (mut nbytes, mut nlines) = (0usize, 0usize);
    for &i in order.iter().take(files) {
        let d: &[u8] = &maps[i];
        let mut pos = match memchr::memchr(b'\n', d) {
            Some(j) => j + 1,
            None => continue,
        };
        let mut k = 0;
        let mut buf = Vec::new();
        while pos < d.len() && k < lines {
            let e = memchr::memchr(b'\n', &d[pos..]).map_or(d.len(), |j| pos + j);
            buf.clear();
            buf.extend_from_slice(&d[pos..e]);
            let n = buf.len();
            buf.extend_from_slice(&[0u8; 16]);
            let mut o: [&[u8]; N] = [b""; N];
            if split::<N>(&buf[..n], &mut o) {
                f(&o);
            }
            nbytes += e + 1 - pos;
            nlines += 1;
            pos = e + 1;
            k += 1;
        }
    }
    (nbytes, nlines)
}

/// Copies a field into a padded buffer so `b36` may read 16 bytes.
fn padded(f: &[u8]) -> ([u8; 32], usize) {
    let mut b = [0u8; 32];
    let n = f.len().min(16);
    b[..n].copy_from_slice(&f[..n]);
    (b, if f.len() > 16 { 0 } else { n })
}

fn zfield(f: &[u8], prefix: u8) -> Option<u64> {
    let (b, n) = padded(f);
    unsafe { b36(&b[..n], prefix) }.map(unmix)
}

pub struct Keys {
    pub e: u64,
    pub u: u64,
    pub c: u64,
    /// Average bytes per line in the samples.
    pub ev_line: f64,
    pub ck_line: f64,
}

pub fn recover_keys(ctx: &Ctx, t0: &Instant) -> Option<Keys> {
    let nt = ctx.nthreads;
    let mut zu_all = Vec::new();
    let mut pairs = Vec::new(); // (z_e, z_u)
    let (eb, el) = sample_rows::<7>(&ctx.events, 6, 3000, |f| {
        let zu = zfield(f[1], b'u');
        if let Some(z) = zu {
            zu_all.push(z);
        }
        if let (Some(ze), Some(zu)) = (zfield(f[0], b'e'), zu) {
            pairs.push((ze, zu));
        }
    });
    tlog(t0, "keys: events sampled");
    if zu_all.len() < 1000 {
        return None;
    }
    let mut hist = vec![0u32; 512];
    for &z in &zu_all {
        hist[(z & 511) as usize] += 1;
    }
    let k9 = (0..512).max_by_key(|&i| hist[i]).unwrap() as u64;
    if (hist[k9 as usize] as usize) * 4 < zu_all.len() {
        return None;
    }
    let alias0: Vec<u64> = zu_all.iter().copied().filter(|z| z & 511 == k9).collect();
    let ev_bytes: usize = ctx.events.iter().map(|m| m.len()).sum();
    let ev_line = eb as f64 / el.max(1) as f64;
    let est_lines = (ev_bytes as f64 / ev_line.max(1.0)) as u64 + 1;
    let tu = (64 - (est_lines / 8 + 1).leading_zeros()) + 9;
    let ku = mitm_retry(nt, &alias0, tu, 9, k9)?;
    // events of user 0 (a whale in the generator: a few % of events, indexes below that)
    let u0: Vec<u64> = pairs.iter().filter(|&&(_, zu)| unkey(zu, ku) >> 9 == 0).map(|&(ze, _)| ze).collect();
    let te = 64 - ((est_lines as f64 * 0.04) as u64 + 1).leading_zeros();
    tlog(t0, "keys: user key");
    let ke = mitm_retry(nt, &u0, te, 0, 0)?;
    tlog(t0, "keys: event key");
    // clicks of user 0
    let mut cu0 = Vec::new();
    let (cb, cl) = sample_rows::<4>(&ctx.clicks, 6, 3000, |f| {
        if let (Some(zc), Some(zu)) = (zfield(f[0], b'k'), zfield(f[1], b'u')) {
            if unkey(zu, ku) >> 9 == 0 {
                cu0.push(zc);
            }
        }
    });
    let ck_bytes: usize = ctx.clicks.iter().map(|m| m.len()).sum();
    let ck_line = cb as f64 / cl.max(1) as f64;
    let kc = if cl == 0 {
        0
    } else {
        let est_c = (ck_bytes as f64 / ck_line.max(1.0)) as u64 + 1;
        let tc = 64 - ((est_c as f64 * 0.04) as u64 + 1).leading_zeros();
        mitm_retry(nt, &cu0, tc, 0, 0)?
    };
    Some(Keys { e: ke, u: ku, c: kc, ev_line, ck_line })
}

// ---------------------------------------------------------------- records

/// One event copy in the dense array (16 bytes).
/// w0 = ingest (biased u32) << 32 | ts_rel << 1 | 1 (present). After placement the kept
///      copy of a valid purchase carries price | cat << 24 in the high half instead.
/// w1 = local index (16) | valid << 16 | purchase << 17 | price-attached << 18 | user << 32
///      (user = user_index * 512 + alias, or EXO | side-table reference).
/// A purchase side record: w0 = ingest << 32 | local, w1 = price | cat << 24.
/// A click (valid ones only): w0 = ts_rel << 1 | 1 | camp << 32 | local << 48, w1 = user.
#[derive(Clone, Copy, Default)]
#[repr(C, align(16))]
struct R {
    w0: u64,
    w1: u64,
}

/// An event copy whose event_id is outside the scheme.
#[derive(Clone, Copy)]
struct Xe {
    eid: u64,
    ing: u32,
    valid: bool,
    ts: i64,
    uid: u64,
    price: u32,
    cat: u16,
    purchase: bool,
}

/// A valid click whose click_id is outside the scheme.
#[derive(Clone, Copy)]
struct Xc {
    cid: u64,
    uid: u64,
    ts: i64,
    camp: u16,
}

/// Dense layout of one ID space: partition p holds indexes [p << s, (p + 1) << s).
#[derive(Clone, Copy)]
struct Layout {
    s: u32,
    np: usize,
    cap: u64,
}
impl Layout {
    fn new(cap: u64) -> Option<Layout> {
        let bits = 64 - cap.max(1).leading_zeros();
        if bits > 27 {
            return None;
        }
        let s = bits.saturating_sub(11).max(8);
        let np = (cap as usize).div_ceil(1 << s);
        Some(Layout { s, np, cap })
    }
}

struct DState {
    tid: u32,
    er: scatter::Scatter<R, 4>,
    pr: scatter::Scatter<R, 4>,
    cr: scatter::Scatter<R, 4>,
    xe: Vec<Xe>,
    xc: Vec<Xc>,
    eside: Vec<u64>, // packed user IDs of events whose user is outside the scheme
    cside: Vec<u64>,
    buf: Vec<u8>,
    bad: bool,
}

/// A segment: kept valid records [start, end] (dense indexes) all with user index u.
#[derive(Clone, Copy)]
struct Seg {
    u: u32,
    start: u32,
    end: u32,
    n: u32,
}

/// Per-partition placement results.
#[derive(Default)]
struct PlaceOut {
    segs: Vec<(usize, usize, Vec<Seg>)>,  // (partition, kept records, segments) for events
    csegs: Vec<(usize, usize, Vec<Seg>)>, // for clicks
    slow_e: Vec<Se>,
    slow_c: Vec<Ck>,
    loc: Vec<R>, // scratch: one partition's slots (all zero between uses)
}

/// The kept records of every partition, compacted in index order into the memory of that
/// partition's parse buffers: chunks (pointer, length, partition) and their start positions
/// in the concatenation of all of them.
struct Comp {
    starts: Vec<u64>,
    chunks: Vec<(usize, u32, u32)>,
}
impl Comp {
    /// Builds the chunk table: partition p (in order) has `kept[p]` records spread over the
    /// buffers `bufs[p]` (pointer, capacity in records), filled in order.
    fn new(kept: &[usize], bufs: &[Vec<(usize, usize)>]) -> (Comp, Vec<u64>) {
        let mut c = Comp { starts: Vec::new(), chunks: Vec::new() };
        let mut first = vec![0u64; kept.len()];
        let mut at = 0u64;
        for (p, &m) in kept.iter().enumerate() {
            first[p] = at;
            let mut left = m;
            for &(ptr, cap) in &bufs[p] {
                if left == 0 {
                    break;
                }
                let n = left.min(cap);
                if n > 0 {
                    c.starts.push(at);
                    c.chunks.push((ptr, n as u32, p as u32));
                    at += n as u64;
                    left -= n;
                }
            }
            assert!(left == 0, "compaction overflow");
        }
        c.starts.push(at);
        (c, first)
    }
    /// Calls f(record, partition) for positions a..=b, in order.
    #[inline(always)]
    fn for_range(&self, a: u64, b: u64, mut f: impl FnMut(&R, u32)) {
        let mut k = self.starts.partition_point(|&x| x <= a) - 1;
        let mut pos = a;
        while pos <= b {
            let (ptr, n, p) = self.chunks[k];
            let off = (pos - self.starts[k]) as usize;
            let end = ((b + 1 - self.starts[k]) as usize).min(n as usize);
            let sl = unsafe { std::slice::from_raw_parts((ptr as *const R).add(off), end - off) };
            for r in sl {
                f(r, p);
            }
            pos += (end - off) as u64;
            k += 1;
        }
    }
}

#[derive(Default)]
struct Pass2 {
    sp_e: Vec<Vec<Se>>, // slow-path records by person partition (hp)
    sp_c: Vec<Vec<Ck>>,
    cache: Vec<(u64, bool)>, // (name, slow) per alias seen for the current user index
    aidx: Vec<(u32, u16)>,   // alias -> (stamp, cache index)
    stamp: u32,
    evs: Vec<Ev2>,
    eci: Vec<u16>,
    cks: Vec<Ck>,
    cci: Vec<u16>,
    evs2: Vec<Ev2>,
    cks2: Vec<Ck>,
    tmp_e: Vec<Ev2>,
    tmp_c: Vec<Ck>,
}

pub fn run_dense<P: Packer>(ctx: &Ctx, pk: &P, t0: &Instant) -> Option<()> {
    let nt = ctx.nthreads;
    let (ws, we) = (ctx.ws, ctx.we);
    let ws_s = ws.div_euclid(1000);
    let cws = ws - WEEK;
    let check = ctx.check_split;
    if nt > 16 || we - cws >= (1i64 << 31) || ctx.camp_ids.len() >= u16::MAX as usize - 1 || ctx.cat_names.len() > 255 {
        return None;
    }
    let keys = recover_keys(ctx, t0)?;
    let (ke, ku, kc) = (keys.e, keys.u, keys.c);
    tlog(t0, "keys recovered");

    // ---- links: on their own phase (SS_LINKS_PAR=1), or by one worker while the others
    // parse events and clicks (nothing in the parse needs persons)
    let links_par = std::env::var_os("SS_LINKS_PAR").is_some();
    let links_cell: std::sync::OnceLock<Option<(CMap, Vec<Ul>)>> = std::sync::OnceLock::new();
    if links_par {
        let _ = links_cell.set(build_links(ctx, pk, ku, check, nt));
        tlog(t0, "links joined");
    }

    // ---- parse events and clicks into dense-index partitions
    let ev_bytes: usize = ctx.events.iter().map(|m| m.len()).sum();
    let ck_bytes: usize = ctx.clicks.iter().map(|m| m.len()).sum();
    let est_ev = (ev_bytes as f64 / keys.ev_line.max(8.0)) as u64;
    let est_ck = (ck_bytes as f64 / keys.ck_line.max(8.0)) as u64;
    let le = Layout::new(est_ev + est_ev / 4 + 4096)?;
    let lc = Layout::new(est_ck + est_ck / 4 + 4096)?;
    let er_res = (est_ev as usize) * 11 / 10 / (nt * le.np) + 8;
    let cr_res = (est_ck as usize) * 11 / 10 / (nt * lc.np) + 8;
    let mut items = Vec::new();
    make_chunks(&ctx.events, 1, 8 << 20, &mut items);
    let mut ckc = Vec::new();
    make_chunks(&ctx.clicks, 2, 8 << 20, &mut ckc);
    items.extend(ckc);
    items.sort_by_key(|c| std::cmp::Reverse(c.end - c.start));
    if !links_par {
        items.insert(0, Chunk { kind: 3, file: 0, start: 0, end: 0 });
    }
    let next_tid = AtomicUsize::new(0);
    let parsed: Vec<DState> = par_run(
        nt,
        items.len(),
        || DState {
            tid: next_tid.fetch_add(1, Relaxed) as u32,
            er: scatter::Scatter::new(le.np, er_res),
            pr: scatter::Scatter::new(le.np, er_res / 16 + 8),
            cr: scatter::Scatter::new(lc.np, cr_res),
            xe: Vec::new(),
            xc: Vec::new(),
            eside: Vec::new(),
            cside: Vec::new(),
            buf: Vec::new(),
            bad: false,
        },
        |st, i| {
            let c = items[i];
            if c.kind == 3 {
                let _ = links_cell.set(build_links(ctx, pk, ku, check, 1));
                return;
            }
            let mut buf = std::mem::take(&mut st.buf);
            if c.kind == 1 {
                let mut sink = EvSink { ctx, pk, ke, ku, le, ws_s, st };
                with_chunk(&ctx.events[c.file as usize], &c, ctx.use_read, &mut buf, |data, start, end| {
                    scan7(data, start, end, check, &mut sink);
                });
            } else {
                let mut sink = CkSink { ctx, pk, kc, ku, lc, cws, st };
                with_chunk(&ctx.clicks[c.file as usize], &c, ctx.use_read, &mut buf, |data, start, end| {
                    scan4(data, start, end, check, &mut sink);
                });
            }
            scatter::fence();
            st.buf = buf;
        },
    );
    let mut parsed = parsed;
    for s in parsed.iter_mut() {
        s.er.finish();
        s.pr.finish();
        s.cr.finish();
    }
    // buffers per partition (pointer, length), for compaction in place after placement
    let ebufs: Vec<Vec<(usize, usize)>> =
        (0..le.np).map(|p| parsed.iter_mut().map(|s| (s.er.vecs[p].as_mut_ptr() as usize, s.er.vecs[p].len())).collect()).collect();
    let cbufs: Vec<Vec<(usize, usize)>> =
        (0..lc.np).map(|p| parsed.iter_mut().map(|s| (s.cr.vecs[p].as_mut_ptr() as usize, s.cr.vecs[p].len())).collect()).collect();
    let parsed = parsed;
    tlog(t0, "events and clicks parsed");
    if pk.failed() || parsed.iter().any(|s| s.bad) {
        return None;
    }
    let (cm, ul) = links_cell.into_inner().flatten()?;
    let mut by_tid: Vec<usize> = vec![0; parsed.len()];
    for (i, s) in parsed.iter().enumerate() {
        by_tid[s.tid as usize] = i;
    }
    let person_of = |uid: u64| -> u64 {
        let r = if cm.maybe(uid) { cm.get_linked(uid) } else { uid };
        // a root's word holds the per-person state (bit 63): its name is its own key
        if r >> 63 != 0 { uid } else { r }
    };
    let side = |xs: u32, ev: bool| -> u64 {
        let s = &parsed[by_tid[((xs >> 27) & 15) as usize]];
        let i = (xs & ((1 << 27) - 1)) as usize;
        if ev { s.eside[i] } else { s.cside[i] }
    };

    // ---- placement: per partition, dedup into a slot array (in cache), cut segments, and
    // write the kept records back, compacted, over the partition's parse buffers
    let nitems = le.np + lc.np;
    let slots = (1usize << le.s).max(1 << lc.s);
    let ebufs = &ebufs;
    let cbufs = &cbufs;
    let placed: Vec<PlaceOut> = par_run(
        nt,
        nitems,
        || PlaceOut { loc: vec![R::default(); slots], ..Default::default() },
        |out, it| {
            let ev = it < le.np;
            let p = if ev { it } else { it - le.np };
            let bufs = if ev { &ebufs[p] } else { &cbufs[p] };
            let total: usize = bufs.iter().map(|b| b.1).sum();
            if total == 0 {
                return;
            }
            let mut loc = std::mem::take(&mut out.loc);
            if ev {
                for &(ptr, n) in bufs {
                    for r in unsafe { std::slice::from_raw_parts(ptr as *const R, n) } {
                        let x = unsafe { loc.get_unchecked_mut((r.w1 & 0xffff) as usize) };
                        if x.w0 == 0 || (r.w0 >> 32) < (x.w0 >> 32) {
                            *x = *r;
                        }
                    }
                }
                for s in parsed.iter() {
                    for r in &s.pr.vecs[p] {
                        let x = unsafe { loc.get_unchecked_mut((r.w0 & 0xffff) as usize) };
                        if x.w0 != 0 && (x.w1 >> 16) & 7 == 3 && x.w0 >> 32 == r.w0 >> 32 {
                            x.w0 = (x.w0 & 0xffff_ffff) | (r.w1 << 32);
                            x.w1 |= 1 << 18;
                        }
                    }
                }
            } else {
                for &(ptr, n) in bufs {
                    for r in unsafe { std::slice::from_raw_parts(ptr as *const R, n) } {
                        let x = unsafe { loc.get_unchecked_mut((r.w0 >> 48) as usize) };
                        if x.w0 == 0 {
                            *x = *r;
                        }
                    }
                }
            }
            // kept records in index order -> segments, and compacted over the buffers
            let mut segs: Vec<Seg> = Vec::new();
            let (mut bi, mut bo) = (0usize, 0usize); // output buffer and offset
            let mut m = 0u32;
            let s_bits = if ev { le.s } else { lc.s };
            for i in 0..1usize << s_bits {
                let x = unsafe { *loc.get_unchecked(i) };
                if x.w0 == 0 {
                    continue;
                }
                unsafe { *loc.get_unchecked_mut(i) = R::default() };
                let uf = if ev {
                    if (x.w1 >> 16) & 1 == 0 {
                        continue; // the kept copy is invalid: the event is dropped
                    }
                    (x.w1 >> 32) as u32
                } else {
                    x.w1 as u32
                };
                if uf & EXO != 0 {
                    if ev {
                        out.slow_e.push(se_of(x, person_of(side(uf, true)), ws));
                    } else {
                        let ts = cws + ((x.w0 >> 1) & 0x7fff_ffff) as i64;
                        let cid = id_key(b'k', ((p << lc.s) + i) as u64, kc);
                        out.slow_c.push(Ck { person: person_of(side(uf, false)), ts, cid, camp: (x.w0 >> 32) as u16 });
                    }
                    continue;
                }
                while bo == bufs[bi].1 {
                    bi += 1;
                    bo = 0;
                }
                unsafe { (bufs[bi].0 as *mut R).add(bo).write(x) };
                bo += 1;
                let u = uf >> 9;
                match segs.last_mut() {
                    Some(g) if g.u == u => {
                        g.end = m;
                        g.n += 1;
                    }
                    _ => segs.push(Seg { u, start: m, end: m, n: 1 }),
                }
                m += 1;
            }
            if ev {
                out.segs.push((p, m as usize, segs));
            } else {
                out.csegs.push((p, m as usize, segs));
            }
            out.loc = loc;
        },
    );
    let mut ekept = vec![0usize; le.np];
    let mut ckept = vec![0usize; lc.np];
    for o in &placed {
        for x in &o.segs {
            ekept[x.0] = x.1;
        }
        for x in &o.csegs {
            ckept[x.0] = x.1;
        }
    }
    let (ecomp, efirst) = Comp::new(&ekept, ebufs);
    let (ccomp, cfirst) = Comp::new(&ckept, cbufs);
    tlog(t0, "placed");

    // ---- IDs outside the scheme: dedup them alone (they never equal an ID inside it);
    // their persons go to the slow path
    let mut slow_names: HashSet<u64, BuildHasherDefault<Fx>> = HashSet::default();
    let mut slow_e0: Vec<Se> = Vec::new();
    let mut slow_c0: Vec<Ck> = Vec::new();
    {
        let mut m: FxMap<u64, Xe> = FxMap::default();
        for s in &parsed {
            for x in &s.xe {
                match m.get_mut(&x.eid) {
                    Some(y) => {
                        if x.ing < y.ing {
                            *y = *x;
                        }
                    }
                    None => {
                        m.insert(x.eid, *x);
                    }
                }
            }
        }
        for x in m.values() {
            if x.valid {
                let person = person_of(x.uid);
                slow_names.insert(person);
                slow_e0.push(Se { person, ts: x.ts, price: x.price, cat: x.cat, purchase: x.purchase });
            }
        }
        let mut seen: HashSet<u64, BuildHasherDefault<Fx>> = HashSet::default();
        for s in &parsed {
            for x in &s.xc {
                if seen.insert(x.cid) {
                    let person = person_of(x.uid);
                    slow_names.insert(person);
                    slow_c0.push(Ck { person, ts: x.ts, cid: x.cid, camp: x.camp });
                }
            }
        }
    }

    // a person with a row whose user ID is outside the scheme is slow
    for o in &placed {
        for x in &o.slow_e {
            slow_names.insert(x.person);
        }
        for x in &o.slow_c {
            slow_names.insert(x.person);
        }
    }

    // ---- global segment lists; a user index in more than one segment is slow
    // positions become global (start of the partition in the compacted concatenation)
    let merge = |lists: Vec<&(usize, usize, Vec<Seg>)>, first: &[u64]| -> Vec<Seg> {
        let mut v: Vec<Seg> = Vec::new();
        for (p, _, l) in lists {
            let f = first[*p] as u32;
            for g in l {
                match v.last_mut() {
                    Some(h) if h.u == g.u => {
                        h.end = g.end + f;
                        h.n += g.n;
                    }
                    _ => v.push(Seg { u: g.u, start: g.start + f, end: g.end + f, n: g.n }),
                }
            }
        }
        v
    };
    let mut el: Vec<&(usize, usize, Vec<Seg>)> = placed.iter().flat_map(|o| o.segs.iter()).collect();
    el.sort_by_key(|x| x.0);
    let mut esegs = merge(el, &efirst);
    let mut cl: Vec<&(usize, usize, Vec<Seg>)> = placed.iter().flat_map(|o| o.csegs.iter()).collect();
    cl.sort_by_key(|x| x.0);
    let mut csegs = merge(cl, &cfirst);
    let ubits = 1usize << 22;
    let mut slow_u = vec![0u64; ubits / 64];
    let mut any_dup = false;
    for list in [&esegs, &csegs] {
        let mut seen = vec![0u64; ubits / 64];
        let mut prev = -1i64;
        let mut sorted = true;
        for g in list.iter() {
            let u = g.u as usize;
            if seen[u >> 6] >> (u & 63) & 1 != 0 {
                slow_u[u >> 6] |= 1 << (u & 63);
                any_dup = true;
            }
            seen[u >> 6] |= 1 << (u & 63);
            sorted &= (g.u as i64) > prev;
            prev = g.u as i64;
        }
        any_dup |= !sorted;
    }
    if any_dup {
        esegs.sort_by_key(|g| g.u);
        csegs.sort_by_key(|g| g.u);
    }
    tlog(t0, &format!("segments: {} event, {} click; slow: {} events, {} clicks, {} names", esegs.len(), csegs.len(),
        placed.iter().map(|o| o.slow_e.len()).sum::<usize>() + slow_e0.len(),
        placed.iter().map(|o| o.slow_c.len()).sum::<usize>() + slow_c0.len(), slow_names.len()));

    // ---- pass 2: per user index, sessions in place for fast persons
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let ncamp = ctx.camp_ids.len();
    let organic = ncamp as u16;
    let ncat = ctx.cat_names.len();
    let new_agg = || AggState {
        rows: Vec::new(),
        camp: vec![0; (ncamp + 1) * ndays * 4],
        asst: vec![0; (ncamp + 1) * ndays],
        catsum: vec![0; ncat],
        touched: Vec::new(),
        ccount: vec![0; ncamp],
        cpos: vec![0; ncamp],
        active: Vec::new(),
        g: Group::default(),
    };
    // units: runs of event segments with about the same number of events
    let total_ev: u64 = esegs.iter().map(|g| g.n as u64).sum();
    let nunits = (nt * 16).max(1);
    let per_unit = total_ev / nunits as u64 + 1;
    let mut ub: Vec<usize> = vec![0]; // first segment of each unit
    let mut acc = 0u64;
    for (i, g) in esegs.iter().enumerate() {
        if acc >= per_unit {
            ub.push(i);
            acc = 0;
        }
        acc += g.n as u64;
    }
    ub.push(esegs.len());
    let nu = ub.len() - 1;
    let ubound = |k: usize| -> u64 {
        if k == 0 {
            0
        } else if k >= nu {
            u64::MAX
        } else {
            esegs[ub[k]].u as u64
        }
    };
    let slow_u = &slow_u;
    let slow_names = &slow_names;
    let esegs = &esegs;
    let csegs = &csegs;
    let ecomp = &ecomp;
    let ccomp = &ccomp;
    let cs = lc.s;
    let ul = &ul;
    let p2: Vec<(AggState, Pass2)> = par_run(nt, nu, || (new_agg(), Pass2::default()), |(st, w), k| {
        let (lo, hi) = (ubound(k), ubound(k + 1));
        let mut j = csegs.partition_point(|g| (g.u as u64) < lo);
        let mut m = ul.partition_point(|e| (e.u as u64) < lo);
        let mut i = ub[k];
        let iend = ub[k + 1];
        loop {
            let ue = if i < iend { esegs[i].u as u64 } else { u64::MAX };
            let uc = if j < csegs.len() && (csegs[j].u as u64) < hi { csegs[j].u as u64 } else { u64::MAX };
            let u = ue.min(uc);
            if u == u64::MAX {
                break;
            }
            let eseg = if ue == u {
                i += 1;
                Some(esegs[i - 1])
            } else {
                None
            };
            let cseg = if uc == u {
                j += 1;
                Some(csegs[j - 1])
            } else {
                None
            };
            while m < ul.len() && (ul[m].u as u64) < u {
                m += 1;
            }
            let mut m2 = m;
            while m2 < ul.len() && ul[m2].u as u64 == u {
                m2 += 1;
            }
            do_user(st, w, u as u32, eseg, cseg, &ul[m..m2], ecomp, ccomp, cs, slow_u, slow_names, ku, kc, ws, cws, day0, ndays, organic);
        }
    });
    tlog(t0, &format!("sessions done (in place); slow path gets {} events, {} clicks; {} user indexes in several segments",
        p2.iter().flat_map(|x| x.1.sp_e.iter()).map(|v| v.len()).sum::<usize>(),
        p2.iter().flat_map(|x| x.1.sp_c.iter()).map(|v| v.len()).sum::<usize>(),
        slow_u.iter().map(|w| w.count_ones() as usize).sum::<usize>()));

    // ---- slow path: everything else, grouped by person name. Pass 2 already split its
    // records by person partition; the few from placement and the exotic IDs join here.
    let mut sp0_e: Vec<Vec<Se>> = (0..NSP).map(|_| Vec::new()).collect();
    let mut sp0_c: Vec<Vec<Ck>> = (0..NSP).map(|_| Vec::new()).collect();
    for (e, c) in std::iter::once((&slow_e0, &slow_c0)).chain(placed.iter().map(|o| (&o.slow_e, &o.slow_c))) {
        for x in e {
            sp0_e[hp(x.person)].push(*x);
        }
        for x in c {
            sp0_c[hp(x.person)].push(*x);
        }
    }
    let sp0_e = &sp0_e;
    let sp0_c = &sp0_c;
    let p2r = &p2;
    let part_e = move |q: usize| {
        p2r.iter().map(move |x| x.1.sp_e.get(q).map_or(&[][..], |v| v.as_slice())).chain(std::iter::once(sp0_e[q].as_slice()))
    };
    let part_c = move |q: usize| {
        p2r.iter().map(move |x| x.1.sp_c.get(q).map_or(&[][..], |v| v.as_slice())).chain(std::iter::once(sp0_c[q].as_slice()))
    };
    let mut sorder: Vec<(usize, usize)> = (0..NSP).map(|q| (part_e(q).map(|s| s.len()).sum::<usize>(), q)).collect();
    sorder.sort_unstable_by(|a, b| b.cmp(a));
    let sorder = &sorder;
    let slow_aggs: Vec<AggState> = par_run(nt, NSP, new_agg, |st, k| {
        let (n, q) = sorder[k];
        if n == 0 {
            return;
        }
        let mut g = std::mem::take(&mut st.g);
        group_partition(&mut g, n, std::iter::empty::<&[Sv]>(), part_e(q), part_c(q));
        for l in 0..g.off.len() - 1 {
            let ev = &g.evs[g.off[l] as usize..g.off[l + 1] as usize];
            let ck = &g.cks[g.coff[l] as usize..g.coff[l + 1] as usize];
            person_sessions(st, g.pkey[l], ev, ck, day0, ndays, organic);
        }
        st.g = g;
    });
    tlog(t0, "sessions done (slow path)");

    let mut aggs: Vec<AggState> = p2.into_iter().map(|x| x.0).collect();
    aggs.extend(slow_aggs);
    write_output(ctx, pk, &aggs, ndays, day0);
    tlog(t0, "output written");
    Some(())
}

/// Links: parse, union-find, the multi-user flags and the table of linked IDs by user
/// index, on `nt` threads.
fn build_links<P: Packer>(ctx: &Ctx, pk: &P, ku: u64, check: bool, nt: usize) -> Option<(CMap, Vec<Ul>)> {
    // ---- links + parallel union-find (as in the fused pipeline)
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
    let npairs: usize = pairs.iter().map(|v| v.len()).sum();
    let cm = CMap::new(npairs);
    let mut blocks = Vec::new();
    for (vi, v) in pairs.iter().enumerate() {
        let mut s = 0;
        while s < v.len() {
            let e = (s + 65536).min(v.len());
            blocks.push((vi, s, e));
            s = e;
        }
    }
    par_run(nt, blocks.len(), || (), |_, bi| {
        let (vi, s, e) = blocks[bi];
        let v = &pairs[vi][s..e];
        for (k, &(a, b)) in v.iter().enumerate() {
            if k + 8 < v.len() {
                let (a2, b2) = v[k + 8];
                cm.prefetch_slot(a2);
                cm.prefetch_slot(b2);
            }
            let ia = cm.slot(a);
            let ib = cm.slot(b);
            cm.union(ia, ib);
        }
    });
    let cap = cm.t.len();
    let nfin = (cap / 65536).max(1);
    par_run(nt, nfin, || 0usize, |n, i| {
        *n += cm.finish_range(i * cap / nfin, (i + 1) * cap / nfin);
    });
    // A person whose IDs inside the scheme do not all decode to one user index is handled
    // by the slow path. Per person, the user index seen so far is kept in its root slot's
    // name word (a root's name is its own key, so the word is free): bit 63 | u, or
    // MULTI. Readers of the map treat a value with bit 63 as "the name is the key".
    par_run(nt, nfin, || (), |_, i| {
        for s in i * cap / nfin..(i + 1) * cap / nfin {
            let k = cm.t[s][0].load(Relaxed);
            if k == 0 {
                continue;
            }
            let xu = xu_of_key(k, ku);
            if xu == u32::MAX {
                // outside the scheme: only matters if a row uses it (then that row's
                // person is put in the slow set after placement)
                continue;
            }
            let r = cm.find(s as u32) as usize;
            let enc = 1u64 << 63 | (xu >> 9) as u64;
            let w = &cm.t[r][1];
            let mut cur = w.load(Relaxed);
            loop {
                if cur == enc || cur == MULTI {
                    break;
                }
                if cur >> 63 == 0 {
                    match w.compare_exchange(cur, enc, Relaxed, Relaxed) {
                        Ok(_) => break,
                        Err(x) => cur = x,
                    }
                } else {
                    w.store(MULTI, Relaxed);
                    break;
                }
            }
        }
    });
    // linked IDs inside the scheme, with their person's name and flag, sorted by user index
    // (pass 2 walks this table in order: no hash lookups there)
    let ul_parts: Vec<Vec<Ul>> = par_run(nt, nfin, Vec::new, |out, i| {
        for s in i * cap / nfin..(i + 1) * cap / nfin {
            let k = cm.t[s][0].load(Relaxed);
            if k == 0 {
                continue;
            }
            let xu = xu_of_key(k, ku);
            if xu == u32::MAX {
                continue;
            }
            let r = cm.find(s as u32) as usize;
            let name = cm.t[r][0].load(Relaxed);
            let multi = cm.t[r][1].load(Relaxed) == MULTI;
            out.push(Ul { u: xu >> 9, alias: xu & 511, name: name | (multi as u64) << 63 });
        }
    });
    let mut ul: Vec<Ul> = Vec::with_capacity(ul_parts.iter().map(|v| v.len()).sum());
    for v in &ul_parts {
        ul.extend_from_slice(v);
    }
    if !ul.is_empty() {
        let mut tmp = Vec::new();
        radix_sort_u32(&mut ul, &mut tmp, |e| e.u);
    }
    Some((cm, ul))
}

#[inline(always)]
fn se_of(x: R, person: u64, ws: i64) -> Se {
    let purchase = (x.w1 >> 17) & 1 != 0;
    let pc = x.w0 >> 32;
    Se {
        person,
        ts: ws + ((x.w0 >> 1) & 0x7fff_ffff) as i64,
        price: if purchase { (pc & 0xff_ffff) as u32 } else { 0 },
        cat: if purchase { (pc >> 24) as u16 } else { 0 },
        purchase,
    }
}

/// All records of user index u: resolve each alias to its person once, then sessions in
/// place for fast persons and slow-path records for the rest.
#[allow(clippy::too_many_arguments)]
#[inline(never)]
fn do_user(
    st: &mut AggState,
    w: &mut Pass2,
    u: u32,
    eseg: Option<Seg>,
    cseg: Option<Seg>,
    links: &[Ul],
    ecomp: &Comp,
    ccomp: &Comp,
    cs: u32,
    slow_u: &[u64],
    slow_names: &HashSet<u64, BuildHasherDefault<Fx>>,
    ku: u64,
    kc: u64,
    ws: i64,
    cws: i64,
    day0: i64,
    ndays: usize,
    organic: u16,
) {
    let us = slow_u[(u >> 6) as usize] >> (u & 63) & 1 != 0;
    if w.aidx.is_empty() {
        w.aidx = vec![(0, 0); 512];
    }
    w.stamp = w.stamp.wrapping_add(1);
    if w.stamp == 0 {
        w.aidx.fill((0, 0));
        w.stamp = 1;
    }
    let stamp = w.stamp;
    w.cache.clear();
    w.evs.clear();
    w.eci.clear();
    w.cks.clear();
    w.cci.clear();
    let resolve = |w: &mut Pass2, alias: u32| -> u16 {
        let a = unsafe { *w.aidx.get_unchecked(alias as usize) };
        if a.0 == stamp {
            return a.1;
        }
        let (name, multi) = match links.iter().find(|e| e.alias == alias) {
            Some(e) => (e.name & !(1u64 << 63), e.name >> 63 != 0),
            None => (id_key(b'u', (u as u64) << 9 | alias as u64, ku), false),
        };
        let slow = us || multi || (!slow_names.is_empty() && slow_names.contains(&name));
        let ci = w.cache.len() as u16;
        w.cache.push((name, slow));
        w.aidx[alias as usize] = (stamp, ci);
        ci
    };
    let mut sorted = true;
    if let Some(g) = eseg {
        let mut prev = i64::MIN;
        let n = g.n as usize;
        w.evs.reserve(n);
        w.eci.reserve(n);
        let (pe, pi) = (w.evs.as_mut_ptr(), w.eci.as_mut_ptr());
        let mut k = 0usize;
        // compacted records: kept, valid, user inside the scheme
        ecomp.for_range(g.start as u64, g.end as u64, |x, _| {
            let uf = (x.w1 >> 32) as u32;
            let a = unsafe { *w.aidx.get_unchecked((uf & 511) as usize) };
            let ci = if a.0 == stamp { a.1 } else { resolve(w, uf & 511) };
            let purchase = (x.w1 >> 17) & 1 != 0;
            let pc = x.w0 >> 32;
            let ts = ws + ((x.w0 >> 1) & 0x7fff_ffff) as i64;
            sorted &= ts >= prev;
            prev = ts;
            let ev = Ev2 { ts, pc: if purchase { (pc & 0xff_ffff) | (pc >> 24) << 32 | 1 << 48 } else { 0 } };
            if k < n {
                unsafe {
                    pe.add(k).write(ev);
                    pi.add(k).write(ci);
                }
                k += 1;
            }
        });
        unsafe {
            w.evs.set_len(k);
            w.eci.set_len(k);
        }
    }
    if let Some(g) = cseg {
        let n = g.n as usize;
        w.cks.reserve(n);
        w.cci.reserve(n);
        ccomp.for_range(g.start as u64, g.end as u64, |x, p| {
            let uf = x.w1 as u32;
            let a = unsafe { *w.aidx.get_unchecked((uf & 511) as usize) };
            let ci = if a.0 == stamp { a.1 } else { resolve(w, uf & 511) };
            let ts = cws + ((x.w0 >> 1) & 0x7fff_ffff) as i64;
            let xc = ((p as u64) << cs) | (x.w0 >> 48);
            w.cks.push(Ck { person: 0, ts, cid: xc, camp: (x.w0 >> 32) as u16 });
            w.cci.push(ci);
        });
    }
    let nc = w.cache.len();
    if nc == 0 {
        return;
    }
    let name0 = w.cache[0].0;
    if w.cache.iter().all(|c| c.0 == name0) {
        let slow = w.cache[0].1;
        let mut evs = std::mem::take(&mut w.evs);
        let mut cks = std::mem::take(&mut w.cks);
        person_out(st, w, name0, slow, &mut evs, &mut cks, sorted, kc, day0, ndays, organic);
        w.evs = evs;
        w.cks = cks;
        return;
    }
    // several persons share this user index (a link split them): one at a time
    let mut evs2 = std::mem::take(&mut w.evs2);
    let mut cks2 = std::mem::take(&mut w.cks2);
    for pi in 0..nc {
        let (name, slow) = w.cache[pi];
        if (0..pi).any(|q| w.cache[q].0 == name) {
            continue; // done with an earlier alias of the same person
        }
        evs2.clear();
        cks2.clear();
        for (e, &ci) in w.evs.iter().zip(&w.eci) {
            if w.cache[ci as usize].0 == name {
                evs2.push(*e);
            }
        }
        for (c, &ci) in w.cks.iter().zip(&w.cci) {
            if w.cache[ci as usize].0 == name {
                cks2.push(*c);
            }
        }
        person_out(st, w, name, slow, &mut evs2, &mut cks2, sorted, kc, day0, ndays, organic);
    }
    w.evs2 = evs2;
    w.cks2 = cks2;
}

/// Clicks (cid = click index) by (ts, click_id bytes). Sorted on one u64 key per click
/// (ts relative to the first, click index); equal timestamps then by the packed click_id.
#[inline(always)]
fn sort_cks(cks: &mut [Ck], tmp: &mut Vec<Ck>, kc: u64) {
    let lo = cks.iter().map(|c| c.ts).min().unwrap();
    let hi = cks.iter().map(|c| c.ts).max().unwrap();
    if hi - lo < (1i64 << 36) && cks.len() <= 64 {
        // the key fits: 36 bits of ts, 27 bits of index
        for c in cks.iter_mut() {
            c.person = ((c.ts - lo) as u64) << 27 | c.cid;
        }
        insertion_sort(cks, |c| c.person);
    } else {
        sort_clicks(cks, tmp);
    }
    let mut a = 0;
    while a < cks.len() {
        let mut b = a + 1;
        while b < cks.len() && cks[b].ts == cks[a].ts {
            b += 1;
        }
        if b - a > 1 {
            for c in &mut cks[a..b] {
                c.cid = id_key(b'k', c.cid, kc);
            }
            insertion_sort(&mut cks[a..b], |c| c.cid);
        }
        a = b;
    }
}

/// One person's events (in index order) and clicks (cid = click index): sessions in place,
/// or slow-path records.
#[allow(clippy::too_many_arguments)]
#[inline(always)]
fn person_out(
    st: &mut AggState,
    w: &mut Pass2,
    name: u64,
    slow: bool,
    evs: &mut Vec<Ev2>,
    cks: &mut Vec<Ck>,
    sorted: bool,
    kc: u64,
    day0: i64,
    ndays: usize,
    organic: u16,
) {
    if slow {
        if w.sp_e.is_empty() {
            w.sp_e = (0..NSP).map(|_| Vec::new()).collect();
            w.sp_c = (0..NSP).map(|_| Vec::new()).collect();
        }
        let q = hp(name);
        for e in evs.iter() {
            w.sp_e[q].push(Se { person: name, ts: e.ts, price: e.price(), cat: e.cat(), purchase: e.purchase() });
        }
        for c in cks.iter() {
            w.sp_c[q].push(Ck { person: name, cid: id_key(b'k', c.cid, kc), ..*c });
        }
        return;
    }
    if evs.is_empty() {
        return;
    }
    if !sorted {
        sort_by_ts(evs, &mut w.tmp_e);
    }
    if cks.len() > 1 {
        sort_cks(cks, &mut w.tmp_c, kc);
    }
    person_sessions(st, name, evs, cks, day0, ndays, organic);
}

// ---------------------------------------------------------------- parse sinks

trait Sink<const N: usize> {
    fn line(&mut self, fs: &[(*const u8, usize); N], clean: bool);
}

struct EvSink<'a, P: Packer> {
    ctx: &'a Ctx,
    pk: &'a P,
    ke: u64,
    ku: u64,
    le: Layout,
    ws_s: i64,
    st: &'a mut DState,
}

impl<'a, P: Packer> Sink<7> for EvSink<'a, P> {
    #[inline(always)]
    fn line(&mut self, fs: &[(*const u8, usize); 7], clean: bool) {
        let f = |k: usize| unsafe { std::slice::from_raw_parts(fs[k].0, fs[k].1) };
        let ctx = self.ctx;
        let (ws, we) = (ctx.ws, ctx.we);
        let f0 = f(0);
        if f0.is_empty() {
            return;
        }
        let (ts_opt, ingest, ing_s) = ctx.tstab.ts_ingest(f(2), f(3));
        let ing_rel = ing_s - self.ws_s;
        if ing_rel < i32::MIN as i64 || ing_rel > i32::MAX as i64 {
            self.st.bad = true;
        }
        let ingb = (ing_rel as i32 as u32) ^ 0x8000_0000;
        let (mut valid, mut purchase, mut price, mut cat, mut ts) = (false, false, 0u32, 0u16, 0i64);
        let f1 = f(1);
        'v: {
            if f1.is_empty() || f(5).is_empty() {
                break 'v;
            }
            ts = match ts_opt {
                Some(t) if t >= ws && t < we => t,
                _ => break 'v,
            };
            let t = f(4);
            let w = u64::from_le(unsafe { (t.as_ptr() as *const u64).read_unaligned() });
            purchase = if t.len() == 8 && w == u64::from_le_bytes(*b"purchase") {
                true
            } else if t.len() == 4
                && ((w as u32) == u32::from_le_bytes(*b"view") || (w as u32) == u32::from_le_bytes(*b"cart"))
            {
                false
            } else {
                break 'v;
            };
            if purchase {
                match parse_price(f(6)) {
                    Some(p) => price = p,
                    None => break 'v,
                }
                cat = ctx.product_idx.get(f(5)).unwrap_or(ctx.unknown_cat);
            }
            if ingest.saturating_sub(ts) > DAY {
                break 'v;
            }
            valid = true;
        }
        let st = &mut *self.st;
        match unsafe { xid(f0, b'e', self.ke) } {
            Some(x) if x < self.le.cap => {
                let mut uf = 0u32;
                if valid {
                    uf = match unsafe { xid(f1, b'u', self.ku) } {
                        Some(xu) if xu < XU_LIMIT => xu as u32,
                        _ => {
                            let i = st.eside.len();
                            if i >= 1 << 27 {
                                st.bad = true;
                            }
                            st.eside.push(unsafe { self.pk.pack_clean(f1, clean) });
                            EXO | st.tid << 27 | i as u32 & ((1 << 27) - 1)
                        }
                    };
                }
                let local = x & ((1 << self.le.s) - 1);
                let part = (x >> self.le.s) as usize;
                let tsr = if valid { (ts - ws) as u64 } else { 0 };
                st.er.push(
                    part,
                    R {
                        w0: (ingb as u64) << 32 | tsr << 1 | 1,
                        w1: local | (valid as u64) << 16 | (purchase as u64) << 17 | (uf as u64) << 32,
                    },
                );
                if valid && purchase {
                    st.pr.push(part, R { w0: (ingb as u64) << 32 | local, w1: price as u64 | (cat as u64) << 24 });
                }
            }
            _ => {
                let eid = unsafe { self.pk.pack_clean(f0, clean) };
                let uid = if valid { unsafe { self.pk.pack_clean(f1, clean) } } else { 0 };
                st.xe.push(Xe { eid, ing: ingb, valid, ts, uid, price, cat, purchase });
            }
        }
    }
}

struct CkSink<'a, P: Packer> {
    ctx: &'a Ctx,
    pk: &'a P,
    kc: u64,
    ku: u64,
    lc: Layout,
    cws: i64,
    st: &'a mut DState,
}

impl<'a, P: Packer> Sink<4> for CkSink<'a, P> {
    #[inline(always)]
    fn line(&mut self, fs: &[(*const u8, usize); 4], clean: bool) {
        let f = |k: usize| unsafe { std::slice::from_raw_parts(fs[k].0, fs[k].1) };
        let ctx = self.ctx;
        let (f0, f1) = (f(0), f(1));
        if f0.is_empty() || f1.is_empty() {
            return;
        }
        let Some(camp) = ctx.campaign_idx.get(f(2)) else { return };
        let ts = match ctx.tstab.parse(f(3)) {
            Some(t) if t >= self.cws && t < ctx.we => t,
            _ => return,
        };
        let st = &mut *self.st;
        match unsafe { xid(f0, b'k', self.kc) } {
            Some(x) if x < self.lc.cap => {
                let uf = match unsafe { xid(f1, b'u', self.ku) } {
                    Some(xu) if xu < XU_LIMIT => xu as u32,
                    _ => {
                        let i = st.cside.len();
                        if i >= 1 << 27 {
                            st.bad = true;
                        }
                        st.cside.push(unsafe { self.pk.pack_clean(f1, clean) });
                        EXO | st.tid << 27 | i as u32 & ((1 << 27) - 1)
                    }
                };
                let local = x & ((1 << self.lc.s) - 1);
                st.cr.push(
                    (x >> self.lc.s) as usize,
                    R { w0: ((ts - self.cws) as u64) << 1 | 1 | (camp as u64) << 32 | local << 48, w1: uf as u64 },
                );
            }
            _ => {
                let cid = unsafe { self.pk.pack_clean(f0, clean) };
                let uid = unsafe { self.pk.pack_clean(f1, clean) };
                st.xc.push(Xc { cid, uid, ts, camp });
            }
        }
    }
}

/// Event lines of file[start..end] (same framing as the fused pipeline's scanner).
#[inline(always)]
fn scan7<S: Sink<7>>(file: &[u8], start: usize, end: usize, check: bool, sink: &mut S) {
    let mut pos = start;
    let fast_end = file.len().saturating_sub(scan::FAST_SLACK);
    let mut buf: Vec<u8> = Vec::new();
    let mut p = [0usize; 7];
    while pos < end {
        if pos <= fast_end {
            prefetch(file.as_ptr().wrapping_add(pos + PF_DIST));
            prefetch(file.as_ptr().wrapping_add(pos + PF_DIST + 64));
            let base = unsafe { file.as_ptr().add(pos) };
            let [nlo, nhi, clo, chi, qlo, qhi, blo, bhi] = unsafe { scan::masks96(base) };
            let mut clean = false;
            let (len, r) = if nlo == 0 && nhi != 0 {
                let len = 64 + nhi.trailing_zeros() as usize;
                clean = (blo | (bhi & ((1u64 << (len - 64)) - 1))) == 0;
                (len, scan::positions96::<7>(len, clo, chi, qlo, qhi, &mut p))
            } else if nlo != 0 {
                let len = nlo.trailing_zeros() as usize;
                let cm = clo as u128 | (chi as u128) << 64;
                let qm = qlo as u128 | (qhi as u128) << 64;
                (len, scan::positions::<7>(len, cm, qm, &mut p))
            } else {
                let (nl, cm, qm) = unsafe { scan::masks128(base) };
                if nl == 0 {
                    (usize::MAX, None)
                } else {
                    let len = nl.trailing_zeros() as usize;
                    (len, scan::positions::<7>(len, cm, qm, &mut p))
                }
            };
            if len != usize::MAX {
                if check {
                    let line = &file[pos..pos + len];
                    let mut o2: [&[u8]; 7] = [b""; 7];
                    let ok2 = split::<7>(line, &mut o2);
                    assert_eq!(r.is_some(), ok2, "splitters disagree on {:?}", String::from_utf8_lossy(line));
                    if let Some(q) = r {
                        for k in 0..7 {
                            assert_eq!(scan::field::<7>(base, &p, q, k), o2[k], "field {k} of {:?}", String::from_utf8_lossy(line));
                        }
                    }
                }
                if let Some(quoted) = r {
                    let mut fs = [(base, 0usize); 7];
                    let mut s = 0usize;
                    for k in 0..7 {
                        let e = p[k];
                        let qd = (unsafe { *base.add(s) } == b'"') as usize & quoted as usize;
                        fs[k] = (unsafe { base.add(s + qd) }, e - s - 2 * qd);
                        s = e + 1;
                    }
                    sink.line(&fs, clean);
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
        let mut o: [&[u8]; 7] = [b""; 7];
        if split::<7>(line, &mut o) {
            sink.line(&o.map(|x| (x.as_ptr(), x.len())), false);
        }
        pos = e + 1;
    }
}

/// Click lines of file[start..end] (same framing as the fused pipeline's scanner).
#[inline(always)]
fn scan4<S: Sink<4>>(file: &[u8], start: usize, end: usize, check: bool, sink: &mut S) {
    let mut pos = start;
    let fast_end = file.len().saturating_sub(scan::FAST_SLACK);
    let mut buf: Vec<u8> = Vec::new();
    let mut p = [0usize; 4];
    while pos < end {
        if pos <= fast_end {
            prefetch(file.as_ptr().wrapping_add(pos + PF_DIST));
            let base = unsafe { file.as_ptr().add(pos) };
            let [nlo, clo, qlo, blo] = unsafe { scan::masks64(base) };
            let mut clean = false;
            let (len, r) = if nlo != 0 {
                let len = nlo.trailing_zeros() as usize;
                clean = blo & ((1u64 << len) - 1) == 0;
                (len, scan::positions64::<4>(len, clo, qlo, &mut p))
            } else {
                let (nl, cm, qm) = unsafe { scan::masks128(base) };
                if nl == 0 {
                    (usize::MAX, None)
                } else {
                    let len = nl.trailing_zeros() as usize;
                    (len, scan::positions::<4>(len, cm, qm, &mut p))
                }
            };
            if len != usize::MAX {
                if check {
                    let line = &file[pos..pos + len];
                    let mut o2: [&[u8]; 4] = [b""; 4];
                    let ok2 = split::<4>(line, &mut o2);
                    assert_eq!(r.is_some(), ok2, "splitters disagree on {:?}", String::from_utf8_lossy(line));
                    if let Some(q) = r {
                        for k in 0..4 {
                            assert_eq!(scan::field::<4>(base, &p, q, k), o2[k], "field {k} of {:?}", String::from_utf8_lossy(line));
                        }
                    }
                }
                if let Some(quoted) = r {
                    let mut fs = [(base, 0usize); 4];
                    let mut s = 0usize;
                    for k in 0..4 {
                        let e = p[k];
                        let qd = (unsafe { *base.add(s) } == b'"') as usize & quoted as usize;
                        fs[k] = (unsafe { base.add(s + qd) }, e - s - 2 * qd);
                        s = e + 1;
                    }
                    sink.line(&fs, clean);
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
        let mut o: [&[u8]; 4] = [b""; 4];
        if split::<4>(line, &mut o) {
            sink.line(&o.map(|x| (x.as_ptr(), x.len())), false);
        }
        pos = e + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(prefix: u8, v: u64) -> Vec<u8> {
        let mut s = vec![prefix];
        let mut d = Vec::new();
        let mut x = v;
        loop {
            d.push(DIGITS[(x % 36) as usize]);
            x /= 36;
            if x == 0 {
                break;
            }
        }
        d.reverse();
        s.extend(d);
        s
    }

    #[test]
    fn b36_matches_reference() {
        let mut r = 0x1234_5678_9abc_def0u64;
        for i in 0..200_000u64 {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            let v = if i % 3 == 0 { r % 100 } else { r & MASK40 };
            let s = enc(b'u', v);
            let (b, n) = padded(&s);
            assert_eq!(unsafe { b36(&b[..n], b'u') }, Some(v), "{:?}", String::from_utf8_lossy(&s));
            // scramble round trip and packed key
            let k = r >> 20;
            let x = v;
            let id = enc(b'k', scramble(x, k));
            let (b, n) = padded(&id);
            assert_eq!(unsafe { xid(&b[..n], b'k', k) }, Some(x));
            assert_eq!(id_key(b'k', x, k), Pack7 { fail: AtomicBool::new(false) }.pack(&id));
        }
        for bad in [&b"u"[..], b"u0a", b"uA1", b"u12345678z", b"x12", b"u1-2", b"u\x80", b"u{", b"u/", b"u:", b"u`"] {
            let (b, n) = padded(bad);
            assert_eq!(unsafe { b36(&b[..n], b'u') }, None, "{:?}", String::from_utf8_lossy(bad));
        }
        let (b, n) = padded(b"u0");
        assert_eq!(unsafe { b36(&b[..n], b'u') }, Some(0));
        let (b, n) = padded(b"uzzzzzzzz");
        assert_eq!(unsafe { b36(&b[..n], b'u') }, None); // over 2^40
    }
}

