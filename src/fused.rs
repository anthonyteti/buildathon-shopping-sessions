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

/// Concurrent union-find over link IDs, then the ID -> person name map with a filter.
struct CMap {
    t: Vec<[AtomicU64; 2]>, // [key, name]
    mask: usize,
    parent: Vec<AtomicU32>, // parent slot + 1; 0 = root
    filt: Vec<AtomicU64>,
    fshift: u32,
}

impl CMap {
    fn new(pairs: usize) -> CMap {
        let cap = (pairs * 3).next_power_of_two().max(1024);
        let fbits = (cap * 4).next_power_of_two().max(4096);
        CMap {
            t: (0..cap).map(|_| [AtomicU64::new(0), AtomicU64::new(0)]).collect(),
            mask: cap - 1,
            parent: (0..cap).map(|_| AtomicU32::new(0)).collect(),
            filt: (0..fbits / 64).map(|_| AtomicU64::new(0)).collect(),
            fshift: 64 - fbits.trailing_zeros(),
        }
    }
    #[inline(always)]
    fn prefetch_slot(&self, k: u64) {
        prefetch(self.t.as_ptr().wrapping_add(mix(k) as usize & self.mask));
    }
    /// Slot of key k, inserting it if absent.
    #[inline(always)]
    fn slot(&self, k: u64) -> u32 {
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
    fn find(&self, mut x: u32) -> u32 {
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
    fn union(&self, a: u32, b: u32) {
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
    fn finish_range(&self, lo: usize, hi: usize) -> usize {
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
    fn maybe(&self, k: u64) -> bool {
        let b = self.fpos(k);
        unsafe { self.filt.get_unchecked(b >> 6) }.load(Relaxed) >> (b & 63) & 1 != 0
    }
    #[inline(always)]
    fn prefetch_filter(&self, k: u64) {
        prefetch(self.filt.as_ptr().wrapping_add(self.fpos(k) >> 6));
    }
    #[inline(always)]
    fn prefetch(&self, k: u64) {
        if self.maybe(k) {
            self.prefetch_slot(k);
        }
    }
    /// Person name of ID k.
    #[inline(always)]
    fn get(&self, k: u64) -> u64 {
        if !self.maybe(k) {
            return k;
        }
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
                // valid events wait in a small batch while their person lookups are prefetched
                const B: usize = 16;
                type Pe = (u64, u64, u64, i64, u32, u16, bool);
                let mut pend = [(0u64, 0u64, 0u64, 0i64, 0u32, 0u16, false); B];
                let mut n = 0usize;
                let flush = |pend: &[Pe], st: &mut PState| {
                    for p in pend {
                        cm.prefetch(p.2);
                    }
                    for &(eid, ing, uid, ts, price, cat, purchase) in pend {
                        let person = cm.get(uid);
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
                };
                with_chunk(&ctx.events[c.file as usize], &c, ctx.use_read, &mut buf, |data, start, end| {
                    scan_events(data, start, end, check, |fl| {
                        let f = |k: usize| fl.get(k);
                        let f0 = f(0);
                        if f0.is_empty() {
                            return;
                        }
                        let eid = unsafe { pk.pack_padded(f0) };
                        let ingest = ctx.tstab.ingest(f(3));
                        let ing_rel = ingest.div_euclid(1000) - ws_s;
                        if ing_rel < i32::MIN as i64 || ing_rel > i32::MAX as i64 {
                            st.bad = true;
                        }
                        let ing = ing_rel as i32 as u32 as u64;
                        'v: {
                            let f1 = f(1);
                            let f5 = f(5);
                            if f1.is_empty() || f5.is_empty() {
                                break 'v;
                            }
                            let ts = match ctx.tstab.parse(f(2)) {
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
                            let uid = unsafe { pk.pack_padded(f1) };
                            cm.prefetch_filter(uid);
                            pend[n] = (eid, ing, uid, ts, price, cat, purchase);
                            n += 1;
                            if n == B {
                                flush(&pend, st);
                                n = 0;
                            }
                            return;
                        }
                        // an invalid copy still counts for the dedup
                        st.er.push(parts.by_eid(eid), Er { eid, x: ing << 32 | NONE as u64 });
                    });
                });
                flush(&pend[..n], st);
            } else {
                const B: usize = 16;
                let mut pend = [(0u64, 0i64, 0u64, 0u16); B];
                let mut n = 0usize;
                let flush = |pend: &[(u64, i64, u64, u16)], ck: &mut scatter::Scatter<Ck, 2>| {
                    for &(uid, _, _, _) in pend {
                        cm.prefetch(uid);
                    }
                    for &(uid, ts, cid, camp) in pend {
                        let person = cm.get(uid);
                        ck.push(parts.by_person(person), Ck { person, ts, cid, camp });
                    }
                };
                let ck = &mut st.ck;
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
                        cm.prefetch_filter(uid);
                        pend[n] = (uid, ts, cid, camp);
                        n += 1;
                        if n == B {
                            flush(&pend, ck);
                            n = 0;
                        }
                    });
                });
                flush(&pend[..n], ck);
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
        if s.len() > 1 {
            s.sort_unstable_by_key(|e| e.ts);
        }
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
