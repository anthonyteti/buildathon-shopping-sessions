//! The main pipeline (the one `run` in main.rs is the general fallback):
//!   1. links, then a lock-free parallel union-find over all link IDs;
//!   2. parse events and clicks, resolving each event's person right away: valid events go
//!      straight to their person partition (16-byte view/cart or 24-byte purchase records),
//!      and every parsed copy leaves a 16-byte (event_id, ingest, locator) record in an
//!      event_id partition;
//!   3. per event_id partition: find the kept copy of every event_id and mark every other
//!      copy dead (a bit per record);
//!   4. per person partition: group, sessionize and aggregate, skipping dead records.
//! Returns None (nothing written) when the input is outside this path's limits; the caller
//! then runs the general pipeline.
use super::*;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::sync::atomic::{AtomicU32, AtomicU64};

const NONE: u32 = u32::MAX;
const IDX_BITS: u32 = 18;
const PP_BITS: u32 = 9;

/// A vector of n all-zero values (atomics here). Large ones come from the arena, whose
/// fresh pages are zero, so nothing is written until first use (in parallel).
pub(crate) fn zeroed_vec<T>(n: usize) -> Vec<T> {
    let layout = std::alloc::Layout::array::<T>(n).unwrap();
    let p = unsafe { std::alloc::alloc_zeroed(layout) } as *mut T;
    assert!(!p.is_null());
    unsafe { Vec::from_raw_parts(p, n, n) }
}

/// Concurrent union-find over link IDs, then the ID -> person name map with a filter.
pub(crate) struct CMap {
    pub(crate) t: Vec<[AtomicU64; 2]>, // [key, name]
    pub(crate) mask: usize,
    parent: Vec<AtomicU32>, // parent slot + 1; 0 = root
    filt: Vec<AtomicU64>,
    fshift: u32,
}

impl CMap {
    pub(crate) fn new(pairs: usize) -> CMap {
        // at most 2 IDs per pair: the load stays under 80%
        let cap = (pairs * 5 / 2).next_power_of_two().max(1024);
        let fbits = (cap * 4).next_power_of_two().max(4096);
        CMap {
            t: zeroed_vec(cap),
            mask: cap - 1,
            parent: zeroed_vec(cap),
            filt: zeroed_vec(fbits / 64),
            fshift: 64 - fbits.trailing_zeros(),
        }
    }
    #[inline(always)]
    pub(crate) fn prefetch_slot(&self, k: u64) {
        prefetch(self.t.as_ptr().wrapping_add(mix(k) as usize & self.mask));
    }
    /// Slot of key k, inserting it if absent.
    #[inline(always)]
    pub(crate) fn slot(&self, k: u64) -> u32 {
        let mut i = mix(k) as usize & self.mask;
        loop {
            let cur = self.t[i][0].load(Acquire);
            if cur == k {
                return i as u32;
            }
            if cur == 0 {
                match self.t[i][0].compare_exchange(0, k, AcqRel, Acquire) {
                    Ok(_) => return i as u32,
                    Err(x) if x == k => return i as u32,
                    Err(_) => {}
                }
                continue;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    pub(crate) fn find(&self, mut x: u32) -> u32 {
        loop {
            let p = self.parent[x as usize].load(Acquire);
            if p == 0 {
                return x;
            }
            let p = p - 1;
            let gp = self.parent[p as usize].load(Acquire);
            if gp != 0 {
                let _ = self.parent[x as usize].compare_exchange(p + 1, gp, AcqRel, Relaxed);
                x = gp - 1;
            } else {
                return p;
            }
        }
    }
    /// Joins two slots; the root of every set is the slot with the smallest key.
    pub(crate) fn union(&self, a: u32, b: u32) {
        loop {
            let ra = self.find(a);
            let rb = self.find(b);
            if ra == rb {
                return;
            }
            let ka = self.t[ra as usize][0].load(Relaxed);
            let kb = self.t[rb as usize][0].load(Relaxed);
            let (child, root) = if ka < kb { (rb, ra) } else { (ra, rb) };
            if self.parent[child as usize].compare_exchange(0, root + 1, AcqRel, Relaxed).is_ok() {
                return;
            }
        }
    }
    /// Stores every slot's person name and the filter bits (after all unions); returns
    /// how many IDs have a name other than themselves.
    pub(crate) fn finish_range(&self, lo: usize, hi: usize) -> usize {
        let mut n = 0;
        for i in lo..hi {
            let k = self.t[i][0].load(Relaxed);
            if k == 0 {
                continue;
            }
            let r = self.find(i as u32);
            let name = self.t[r as usize][0].load(Relaxed);
            self.t[i][1].store(name, Relaxed);
            if name != k {
                let b = self.fpos(k);
                self.filt[b >> 6].fetch_or(1 << (b & 63), Relaxed);
                n += 1;
            }
        }
        n
    }
    #[inline(always)]
    fn fpos(&self, k: u64) -> usize {
        (k.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> self.fshift) as usize
    }
    #[inline(always)]
    pub(crate) fn maybe(&self, k: u64) -> bool {
        let b = self.fpos(k);
        unsafe { self.filt.get_unchecked(b >> 6) }.load(Relaxed) >> (b & 63) & 1 != 0
    }
    #[inline(always)]
    pub(crate) fn prefetch_filter(&self, k: u64) {
        prefetch(self.filt.as_ptr().wrapping_add(self.fpos(k) >> 6));
    }
    /// Person name of an ID that passed the filter.
    #[inline(always)]
    pub(crate) fn get_linked(&self, k: u64) -> u64 {
        let mut i = mix(k) as usize & self.mask;
        loop {
            let s = unsafe { self.t.get_unchecked(i) };
            let key = s[0].load(Relaxed);
            if key == k {
                return s[1].load(Relaxed);
            }
            if key == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// One parsed copy of an event: event_id, ingest seconds (relative, i32) and where its
/// person record is (thread 4 | kind 1 | partition 9 | index 18 bits), NONE if invalid.
#[derive(Clone, Copy, Default)]
#[repr(C, align(16))]
struct Er {
    eid: u64,
    x: u64,
}
impl Er {
    #[inline(always)]
    fn ingest(&self) -> i32 {
        (self.x >> 32) as u32 as i32
    }
    #[inline(always)]
    fn loc(&self) -> u32 {
        self.x as u32
    }
}

struct PState {
    tid: usize,
    er: scatter::Scatter<Er, 4>,
    sv: scatter::Scatter<Sv, 4>,
    se: Vec<Vec<Se>>,
    ck: scatter::Scatter<Ck, 2>,
    buf: Vec<u8>,
    bad: bool,
}

/// Dead-record bits of one thread's buffers: [kind][partition] -> bits.
struct Dead {
    sv: Vec<Vec<AtomicU64>>,
    se: Vec<Vec<AtomicU64>>,
}

#[inline(always)]
fn is_dead(bits: &[AtomicU64], i: usize) -> bool {
    unsafe { bits.get_unchecked(i >> 6) }.load(Relaxed) >> (i & 63) & 1 != 0
}

pub fn run_fused<P: Packer>(ctx: &Ctx, pk: &P, t0: &Instant) -> Option<()> {
    let nt = ctx.nthreads;
    let (ws, we) = (ctx.ws, ctx.we);
    let ws_s = ws.div_euclid(1000);
    let check = ctx.check_split;
    if nt > 16 || we - ws >= (1i64 << 40) {
        return None;
    }

    // ---- links + parallel union-find
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
    // unions in blocks of 64K pairs
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
        let lo = i * cap / nfin;
        let hi = (i + 1) * cap / nfin;
        *n += cm.finish_range(lo, hi);
    });
    tlog(t0, "links joined");

    // ---- parse events and clicks
    let ev_bytes: usize = ctx.events.iter().map(|m| m.len()).sum();
    let ck_bytes: usize = ctx.clicks.iter().map(|m| m.len()).sum();
    let est_events = ev_bytes / 70 + 1;
    let mut bits = 8u32;
    while bits < 11 && (est_events >> bits) > 16_384 {
        bits += 1;
    }
    let parts = Parts { bits, pbits: bits.saturating_sub(2).max(4).min(PP_BITS) };
    let np = parts.n();
    let npp = parts.np();
    let er_res = est_events * 6 / 5 / (nt * np) + 8;
    let sv_res = est_events * 6 / 5 / (nt * npp) + 8;
    let ck_res = (ck_bytes / 45) * 6 / 5 / (nt * npp) + 8;
    let mut items = Vec::new();
    let mut evc = Vec::new();
    make_chunks(&ctx.events, 1, 8 << 20, &mut evc);
    evc.sort_by_key(|c| std::cmp::Reverse(c.end - c.start));
    let mut ckc = Vec::new();
    make_chunks(&ctx.clicks, 2, 8 << 20, &mut ckc);
    ckc.sort_by_key(|c| std::cmp::Reverse(c.end - c.start));
    items.extend(evc);
    items.extend(ckc);
    let cws = ws - WEEK;
    let next_tid = AtomicUsize::new(0);
    let parsed: Vec<PState> = par_run(
        nt,
        items.len(),
        || PState {
            tid: next_tid.fetch_add(1, Relaxed),
            er: scatter::Scatter::new(np, er_res),
            sv: scatter::Scatter::new(npp, sv_res),
            se: (0..npp).map(|_| Vec::with_capacity(sv_res / 16 + 8)).collect(),
            ck: scatter::Scatter::new(npp, ck_res),
            buf: Vec::new(),
            bad: false,
        },
        |st, i| {
            let c = items[i];
            let tid = st.tid as u32;
            let mut buf = std::mem::take(&mut st.buf);
            if c.kind == 1 {
                let mut sink = EvSink {
                    ctx,
                    pk,
                    cm: &cm,
                    parts,
                    ws_s,
                    tid,
                    st,
                    pend: [(0u64, 0u64, 0u64, 0i64, 0u32, 0u16, false); 2 * PB],
                    linked: [false; 2 * PB],
                    cur: 0,
                    n: 0,
                    prev_n: 0,
                };
                with_chunk(&ctx.events[c.file as usize], &c, ctx.use_read, &mut buf, |data, start, end| {
                    scan_events2(data, start, end, check, &mut sink);
                });
                sink.flush();
            } else {
                let mut sink = CkSink {
                    ctx,
                    pk,
                    cm: &cm,
                    parts,
                    cws,
                    ck: &mut st.ck,
                    pend: [(0u64, 0i64, 0u64, 0u16); 2 * PB],
                    linked: [false; 2 * PB],
                    cur: 0,
                    n: 0,
                    prev_n: 0,
                };
                with_chunk(&ctx.clicks[c.file as usize], &c, ctx.use_read, &mut buf, |data, start, end| {
                    scan_clicks2(data, start, end, check, &mut sink);
                });
                sink.flush();
            }
            scatter::fence();
            st.buf = buf;
        },
    );
    let mut parsed = parsed;
    for s in parsed.iter_mut() {
        s.er.finish();
        s.sv.finish();
        s.ck.finish();
    }
    let parsed = parsed;
    tlog(t0, "events and clicks parsed");
    if pk.failed() || parsed.iter().any(|s| s.bad) {
        return None;
    }
    // thread id -> parse state
    let mut by_tid: Vec<usize> = vec![0; parsed.len()];
    for (i, s) in parsed.iter().enumerate() {
        by_tid[s.tid] = i;
    }
    let dead: Vec<Dead> = parsed
        .iter()
        .map(|s| Dead {
            sv: s.sv.vecs.iter().map(|v| (0..v.len() / 64 + 1).map(|_| AtomicU64::new(0)).collect()).collect(),
            se: s.se.iter().map(|v| (0..v.len() / 64 + 1).map(|_| AtomicU64::new(0)).collect()).collect(),
        })
        .collect();
    let kill = |loc: u32| {
        if loc == NONE {
            return;
        }
        let t = by_tid[(loc >> 28) as usize];
        let kind = (loc >> 27) & 1;
        let q = ((loc >> IDX_BITS) & ((1 << PP_BITS) - 1)) as usize;
        let idx = (loc & ((1 << IDX_BITS) - 1)) as usize;
        let bits = if kind == 0 { &dead[t].sv[q] } else { &dead[t].se[q] };
        bits[idx >> 6].fetch_or(1 << (idx & 63), Relaxed);
    };

    // ---- dedup per event_id partition: every copy but the kept one dies
    par_run(nt, np, Vec::<Er>::new, |table, p| {
        let total: usize = parsed.iter().map(|s| s.er.vecs[p].len()).sum();
        let cap = (total + total / 2).next_power_of_two().max(16);
        let mask = cap - 1;
        table.clear();
        table.resize(cap, Er::default());
        for s in parsed.iter() {
            let v = &s.er.vecs[p];
            for (k, e) in v.iter().enumerate() {
                prefetch(v.as_ptr().wrapping_add(k + 16));
                if k + 8 < v.len() {
                    let a = unsafe { v.get_unchecked(k + 8) }.eid;
                    prefetch(table.as_ptr().wrapping_add(mix(a) as usize & mask));
                }
                let mut i = (mix(e.eid) as usize) & mask;
                loop {
                    let x = unsafe { table.get_unchecked_mut(i) };
                    if x.eid == 0 {
                        *x = *e;
                        break;
                    }
                    if x.eid == e.eid {
                        if e.ingest() < x.ingest() {
                            kill(x.loc());
                            *x = *e;
                        } else {
                            // later, or the same ingest (identical copy): this one dies
                            kill(e.loc());
                        }
                        break;
                    }
                    i = (i + 1) & mask;
                }
            }
        }
    });
    tlog(t0, "dedup done");

    // ---- sessions per person partition
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let ncamp = ctx.camp_ids.len();
    let organic = ncamp as u16;
    let ncat = ctx.cat_names.len();
    let mut order: Vec<(usize, usize)> = (0..npp)
        .map(|q| (parsed.iter().map(|s| s.sv.vecs[q].len() + s.se[q].len()).sum::<usize>(), q))
        .collect();
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
            group_fused(&mut g, n, &parsed, &dead, q);
            for l in 0..g.off.len() - 1 {
                let ev = &g.evs[g.off[l] as usize..g.off[l + 1] as usize];
                let ck = &g.cks[g.coff[l] as usize..g.coff[l + 1] as usize];
                person_sessions(st, g.pkey[l], ev, ck, day0, ndays, organic);
            }
            st.g = g;
        },
    );
    tlog(t0, "sessions done");
    write_output(ctx, pk, &aggs, ndays, day0);
    tlog(t0, "output written");
    Some(())
}

/// Groups one person partition (skipping dead records) into `g` like `group_partition`.
fn group_fused(g: &mut Group, n: usize, parsed: &[PState], dead: &[Dead], q: usize) {
    let mut cap = (n / 4).next_power_of_two().max(64);
    let mask = loop {
        let mask = cap - 1;
        let limit = cap / 2;
        g.tab.clear();
        g.tab.resize(cap, (0, 0));
        g.lid.clear();
        g.off.clear();
        let mut ok = true;
        'all: for (s, d) in parsed.iter().zip(dead) {
            let sv = &s.sv.vecs[q];
            let dsv = &d.sv[q];
            for (i, e) in sv.iter().enumerate() {
                let l = if is_dead(dsv, i) { u32::MAX } else { lid_of(g, mask, limit, e.person) };
                if l == u32::MAX - 1 {
                    ok = false;
                    break 'all;
                }
                g.lid.push(l);
            }
            let se = &s.se[q];
            let dse = &d.se[q];
            for (i, e) in se.iter().enumerate() {
                let l = if is_dead(dse, i) { u32::MAX } else { lid_of(g, mask, limit, e.person) };
                if l == u32::MAX - 1 {
                    ok = false;
                    break 'all;
                }
                g.lid.push(l);
            }
        }
        if ok {
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
    g.evs.resize(acc as usize, Ev2 { ts: 0, pc: 0 });
    g.pkey.clear();
    g.pkey.resize(nl, 0);
    let mut k = 0;
    for s in parsed {
        for e in &s.sv.vecs[q] {
            let l = g.lid[k];
            k += 1;
            if l != u32::MAX {
                g.evs[g.pos[l as usize] as usize] = Ev2 { ts: e.ts, pc: 0 };
                g.pkey[l as usize] = e.person;
                g.pos[l as usize] += 1;
            }
        }
        for e in &s.se[q] {
            let l = g.lid[k];
            k += 1;
            if l != u32::MAX {
                g.evs[g.pos[l as usize] as usize] = Ev2::of(e);
                g.pkey[l as usize] = e.person;
                g.pos[l as usize] += 1;
            }
        }
    }
    for l in 0..nl {
        let s = &mut g.evs[g.off[l] as usize..g.off[l + 1] as usize];
        sort_by_ts(s, &mut g.tmp_e);
    }
    group_clicks(g, mask, parsed.iter().map(|s| s.ck.vecs[q].as_slice()));
}

/// Local id of a person (u32::MAX - 1 when the table is full).
#[inline(always)]
fn lid_of(g: &mut Group, mask: usize, limit: usize, person: u64) -> u32 {
    let mut i = mix(person) as usize & mask;
    let l = loop {
        let s = unsafe { *g.tab.get_unchecked(i) };
        if s.0 == person {
            break s.1;
        }
        if s.0 == 0 {
            let l = g.off.len() as u32;
            if l as usize >= limit {
                return u32::MAX - 1;
            }
            g.tab[i] = (person, l);
            g.off.push(0);
            break l;
        }
        i = (i + 1) & mask;
    };
    g.off[l as usize] += 1;
    l
}

const PB: usize = 16;
/// A valid event waiting for its person lookup: (eid, ingest, uid, ts, price, cat, purchase).
type Pe = (u64, u64, u64, i64, u32, u16, bool);

/// Decodes event lines and routes them: valid events wait in a small batch while their
/// person lookups are prefetched; every copy leaves a dedup record.
struct EvSink<'a, P: Packer> {
    ctx: &'a Ctx,
    pk: &'a P,
    cm: &'a CMap,
    parts: Parts,
    ws_s: i64,
    tid: u32,
    st: &'a mut PState,
    /// two halves of PB: the current one fills while the other one's lookups are in
    /// flight (its slots were prefetched when it filled)
    pend: [Pe; 2 * PB],
    linked: [bool; 2 * PB],
    cur: usize,
    n: usize,
    prev_n: usize,
}

impl<'a, P: Packer> EvSink<'a, P> {
    /// One well-formed line; `fs[k]` is field k (quotes stripped, 16 readable bytes).
    #[inline(always)]
    fn line(&mut self, fs: &[(*const u8, usize); 7], clean: bool) {
        let f = |k: usize| unsafe { std::slice::from_raw_parts(fs[k].0, fs[k].1) };
        let ctx = self.ctx;
        let pk = self.pk;
        let (ws, we) = (ctx.ws, ctx.we);
        let f0 = f(0);
        if f0.is_empty() {
            return;
        }
        let eid = unsafe { pk.pack_clean(f0, clean) };
        let (ts_opt, ingest, ing_s) = ctx.tstab.ts_ingest(f(2), f(3));
        let ing_rel = ing_s - self.ws_s;
        if ing_rel < i32::MIN as i64 || ing_rel > i32::MAX as i64 {
            self.st.bad = true;
        }
        let ing = ing_rel as i32 as u32 as u64;
        'v: {
            let f1 = f(1);
            let f5 = f(5);
            if f1.is_empty() || f5.is_empty() {
                break 'v;
            }
            let ts = match ts_opt {
                Some(t) if t >= ws && t < we => t,
                _ => break 'v,
            };
            let t = f(4);
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
            let mut cat = 0u16;
            if purchase {
                match parse_price(f(6)) {
                    Some(p) => price = p,
                    None => break 'v,
                }
                cat = ctx.product_idx.get(f5).unwrap_or(ctx.unknown_cat);
            }
            if ingest.saturating_sub(ts) > DAY {
                break 'v;
            }
            let uid = unsafe { pk.pack_clean(f1, clean) };
            self.cm.prefetch_filter(uid);
            unsafe { *self.pend.get_unchecked_mut(self.cur * PB + self.n) = (eid, ing, uid, ts, price, cat, purchase) };
            self.n += 1;
            if self.n == PB {
                self.rotate();
            }
            return;
        }
        // an invalid copy still counts for the dedup
        self.st.er.push(self.parts.by_eid(eid), Er { eid, x: ing << 32 | NONE as u64 });
    }

    /// The current half is full: prefetch its person slots, then resolve the other half.
    #[inline(never)]
    fn rotate(&mut self) {
        self.prefetch_half(self.cur, self.n);
        self.process(self.cur ^ 1, self.prev_n);
        self.prev_n = self.n;
        self.cur ^= 1;
        self.n = 0;
    }

    /// Resolves everything still pending.
    fn flush(&mut self) {
        self.prefetch_half(self.cur, self.n);
        self.process(self.cur ^ 1, self.prev_n);
        self.process(self.cur, self.n);
        self.prev_n = 0;
        self.n = 0;
    }

    #[inline(always)]
    fn prefetch_half(&mut self, h: usize, n: usize) {
        let cm = self.cm;
        for i in h * PB..h * PB + n {
            let uid = self.pend[i].2;
            let m = cm.maybe(uid);
            self.linked[i] = m;
            if m {
                cm.prefetch_slot(uid);
            }
        }
    }

    #[inline(always)]
    fn process(&mut self, h: usize, n: usize) {
        let (cm, parts, tid) = (self.cm, self.parts, self.tid);
        let st = &mut *self.st;
        for i in h * PB..h * PB + n {
            let (eid, ing, uid, ts, price, cat, purchase) = self.pend[i];
            let person = if self.linked[i] { cm.get_linked(uid) } else { uid };
            let q = parts.by_person(person);
            let (kind, idx) = if purchase {
                let idx = st.se[q].len();
                st.se[q].push(Se { person, ts, price, cat, purchase: true });
                (1u32, idx)
            } else {
                let idx = st.sv.len(q);
                st.sv.push(q, Sv { person, ts });
                (0u32, idx)
            };
            if idx >> IDX_BITS != 0 {
                st.bad = true;
            }
            let loc = tid << 28 | kind << 27 | (q as u32) << IDX_BITS | idx as u32 & ((1 << IDX_BITS) - 1);
            st.er.push(parts.by_eid(eid), Er { eid, x: ing << 32 | loc as u64 });
        }
    }
}

/// Event lines of file[start..end] into `sink`. Lines of 64..96 bytes (nearly all) take
/// `positions96`; shorter or longer ones the 128-byte window; the rest the scalar split.
/// With `check`, every fast-path line is also split by `split` and compared.
#[inline(always)]
fn scan_events2<P: Packer>(file: &[u8], start: usize, end: usize, check: bool, sink: &mut EvSink<P>) {
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
                    if check {
                        for k in 0..2 {
                            let fk = unsafe { std::slice::from_raw_parts(fs[k].0, fs[k].1) };
                            assert_eq!(unsafe { sink.pk.pack_clean(fk, clean) }, unsafe { sink.pk.pack_padded(fk) });
                        }
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

/// Click lines into person partitions, with the same two-half person lookup as `EvSink`.
struct CkSink<'a, P: Packer> {
    ctx: &'a Ctx,
    pk: &'a P,
    cm: &'a CMap,
    parts: Parts,
    cws: i64,
    ck: &'a mut scatter::Scatter<Ck, 2>,
    pend: [(u64, i64, u64, u16); 2 * PB],
    linked: [bool; 2 * PB],
    cur: usize,
    n: usize,
    prev_n: usize,
}

impl<'a, P: Packer> CkSink<'a, P> {
    /// One well-formed click line; `fs[k]` is field k (quotes stripped, 16 readable bytes).
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
        let cid = unsafe { self.pk.pack_clean(f0, clean) };
        let uid = unsafe { self.pk.pack_clean(f1, clean) };
        self.cm.prefetch_filter(uid);
        unsafe { *self.pend.get_unchecked_mut(self.cur * PB + self.n) = (uid, ts, cid, camp) };
        self.n += 1;
        if self.n == PB {
            self.rotate();
        }
    }

    #[inline(never)]
    fn rotate(&mut self) {
        self.prefetch_half(self.cur, self.n);
        self.process(self.cur ^ 1, self.prev_n);
        self.prev_n = self.n;
        self.cur ^= 1;
        self.n = 0;
    }

    fn flush(&mut self) {
        self.prefetch_half(self.cur, self.n);
        self.process(self.cur ^ 1, self.prev_n);
        self.process(self.cur, self.n);
        self.prev_n = 0;
        self.n = 0;
    }

    #[inline(always)]
    fn prefetch_half(&mut self, h: usize, n: usize) {
        let cm = self.cm;
        for i in h * PB..h * PB + n {
            let uid = self.pend[i].0;
            let m = cm.maybe(uid);
            self.linked[i] = m;
            if m {
                cm.prefetch_slot(uid);
            }
        }
    }

    #[inline(always)]
    fn process(&mut self, h: usize, n: usize) {
        let (cm, parts) = (self.cm, self.parts);
        for i in h * PB..h * PB + n {
            let (uid, ts, cid, camp) = self.pend[i];
            let person = if self.linked[i] { cm.get_linked(uid) } else { uid };
            self.ck.push(parts.by_person(person), Ck { person, ts, cid, camp });
        }
    }
}

/// Click lines of file[start..end] into `sink`: lines under 64 bytes (nearly all) are
/// split from two 32-byte masks; others take the 128-byte window or the scalar split.
#[inline(always)]
fn scan_clicks2<P: Packer>(file: &[u8], start: usize, end: usize, check: bool, sink: &mut CkSink<P>) {
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
                    if check {
                        for k in 0..2 {
                            let fk = unsafe { std::slice::from_raw_parts(fs[k].0, fs[k].1) };
                            assert_eq!(unsafe { sink.pk.pack_clean(fk, clean) }, unsafe { sink.pk.pack_padded(fk) });
                        }
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
