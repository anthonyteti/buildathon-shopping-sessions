//! Fast decoders with exact fallbacks: SWAR timestamps (month table around the window)
//! and direct-index lookups for product and campaign IDs of the form `<prefix><n>`.
use crate::{DAY, FxMap, civil_ms, parse_ts};

pub struct TsTab {
    // "YYYY-MM-DD" of every date near the window, in a 256-slot table with one slot per
    // date (the multiplier is chosen so no two of these dates share a slot)
    slots: Vec<(u64, u16, i64)>, // (bytes 0..8, bytes 8..10, days since epoch); empty: (0, 0, 0)
    mult: u64,
}

const SLOT_BITS: u32 = 8;

impl TsTab {
    /// Covers every date in [from_ms - 10 days, to_ms + 10 days].
    pub fn new(from_ms: i64, to_ms: i64) -> TsTab {
        let d0 = from_ms.div_euclid(DAY) - 10;
        let d1 = to_ms.div_euclid(DAY) + 10;
        let mut dates = Vec::new();
        for d in d0..=d1 {
            let (y, m, dd) = civil_from_days(d);
            let s = format!("{y:04}-{m:02}-{dd:02}");
            if s.len() == 10 {
                let b = s.as_bytes();
                dates.push((u64::from_le_bytes(b[0..8].try_into().unwrap()), u16::from_le_bytes([b[8], b[9]]), d));
            }
        }
        let mut mult = 0x9E37_79B9_7F4A_7C15u64;
        for _ in 0..10_000 {
            let mut slots = vec![(0u64, 0u16, 0i64); 1 << SLOT_BITS];
            let mut ok = true;
            for &(a, dd, days) in &dates {
                let h = Self::hash(mult, a, dd);
                if slots[h].0 != 0 {
                    ok = false;
                    break;
                }
                slots[h] = (a, dd, days);
            }
            if ok {
                return TsTab { slots, mult };
            }
            mult = mult.wrapping_mul(0x5851_F42D_4C95_7F2D).wrapping_add(0x1405_7B7E_F767_814F) | 1;
        }
        // no collision-free multiplier (cannot happen for a normal window): table unused
        TsTab { slots: vec![(0, 0, 0); 1 << SLOT_BITS], mult }
    }

    #[inline(always)]
    fn hash(mult: u64, a: u64, d: u16) -> usize {
        ((a ^ ((d as u64) << 3)).wrapping_mul(mult) >> (64 - SLOT_BITS)) as usize
    }

    /// Days since epoch of the "YYYY-MM-DD" in f[0..10], if it is a date in the table.
    #[inline(always)]
    fn date(&self, f: &[u8]) -> Option<i64> {
        let a = u64::from_le_bytes(f[0..8].try_into().unwrap());
        let d = u16::from_le_bytes([f[8], f[9]]);
        let e = unsafe { *self.slots.get_unchecked(Self::hash(self.mult, a, d)) };
        if e.0 == a && e.1 == d { Some(e.2) } else { None }
    }

    /// YYYY-MM-DDTHH:MM:SS in f[0..19] -> epoch ms. `f` needs 19 bytes.
    /// Dates near the window come from the table; anything else goes to `civil_ms`.
    #[inline(always)]
    fn civil(&self, f: &[u8]) -> Option<i64> {
        let f: &[u8; 19] = f[..19].try_into().unwrap();
        let Some(days) = self.date(f) else { return civil_ms(f) };
        // "HH:MM:SS" at bytes 11..19
        let t = u64::from_le_bytes(f[11..19].try_into().unwrap());
        const DM: u64 = 0xFFFF_00FF_FF00_FFFF;
        let digits = (t & DM & 0xF0F0_F0F0_F0F0_F0F0) == (0x3030_3030_3030_3030 & DM)
            && (t.wrapping_add(0x0606_0606_0606_0606) & DM & 0xF0F0_F0F0_F0F0_F0F0) == (0x3030_3030_3030_3030 & DM);
        let seps = (t & 0x0000_FF00_00FF_0000) == 0x0000_3A00_003A_0000;
        if !(digits & seps & (f[10] == b'T')) {
            return None;
        }
        let v = t & 0x0F0F_0F0F_0F0F_0F0F;
        let p = v * 10 + (v >> 8);
        let (h, mi, s) = ((p & 0xFF) as i64, ((p >> 24) & 0xFF) as i64, ((p >> 48) & 0xFF) as i64);
        if h > 23 || mi > 59 || s > 59 {
            return None;
        }
        Some(days * DAY + ((h * 60 + mi) * 60 + s) * 1000)
    }

    /// ingest_ts, which the rules guarantee is a valid F1 timestamp: dates near the
    /// window are decoded without re-validating; anything else takes the full parse.
    #[inline(always)]
    pub fn ingest(&self, f: &[u8]) -> i64 {
        if f.len() == 20 {
            if let Some(days) = self.date(f) {
                let t = u64::from_le_bytes(f[11..19].try_into().unwrap());
                let v = t & 0x0F0F_0F0F_0F0F_0F0F;
                let p = v * 10 + (v >> 8);
                let (h, mi, s) = ((p & 0xFF) as i64, ((p >> 24) & 0xFF) as i64, ((p >> 48) & 0xFF) as i64);
                return days * DAY + ((h * 60 + mi) * 60 + s) * 1000;
            }
        }
        self.parse(f).unwrap_or(i64::MAX)
    }

    /// `ingest` in ms and its whole seconds (floor), without a division on the fast path.
    #[inline(always)]
    pub fn ingest_ms_s(&self, f: &[u8]) -> (i64, i64) {
        if f.len() == 20 {
            if let Some(days) = self.date(f) {
                let t = u64::from_le_bytes(f[11..19].try_into().unwrap());
                let v = t & 0x0F0F_0F0F_0F0F_0F0F;
                let p = v * 10 + (v >> 8);
                let (h, mi, s) = ((p & 0xFF) as i64, ((p >> 24) & 0xFF) as i64, ((p >> 48) & 0xFF) as i64);
                let secs = days * 86_400 + (h * 60 + mi) * 60 + s;
                return (secs * 1000, secs);
            }
        }
        let ms = self.parse(f).unwrap_or(i64::MAX);
        (ms, ms.div_euclid(1000))
    }

    /// Same result as `parse_ts`.
    #[inline(always)]
    pub fn parse(&self, f: &[u8]) -> Option<i64> {
        match f.len() {
            20 if f[19] == b'Z' => self.civil(f),
            25 if f[19] == b'+' || f[19] == b'-' => {
                if f[22] != b':' {
                    return None;
                }
                let x = f[20].wrapping_sub(b'0') as i64;
                let y = f[21].wrapping_sub(b'0') as i64;
                let z = f[23].wrapping_sub(b'0') as i64;
                let w = f[24].wrapping_sub(b'0') as i64;
                if x > 9 || y > 9 || z > 9 || w > 9 {
                    return None;
                }
                let (oh, om) = (x * 10 + y, z * 10 + w);
                if oh > 23 || om > 59 {
                    return None;
                }
                let base = self.civil(f)?;
                let off = (oh * 60 + om) * 60_000;
                Some(if f[19] == b'+' { base - off } else { base + off })
            }
            13 => {
                // epoch ms with exactly 13 digits: two overlapping 8-digit SWAR conversions
                let w0 = u64::from_le_bytes(f[0..8].try_into().unwrap());
                let w1 = u64::from_le_bytes(f[5..13].try_into().unwrap());
                if !(all_digits8(w0) & all_digits8(w1)) {
                    return None;
                }
                Some((swar8(w0) * 100_000 + swar8(w1) % 100_000) as i64)
            }
            _ => parse_ts(f),
        }
    }
}

#[inline(always)]
fn all_digits8(w: u64) -> bool {
    (w & 0xF0F0_F0F0_F0F0_F0F0) == 0x3030_3030_3030_3030
        && (w.wrapping_add(0x0606_0606_0606_0606) & 0xF0F0_F0F0_F0F0_F0F0) == 0x3030_3030_3030_3030
}

/// Eight ASCII digits (first byte most significant) -> their value.
#[inline(always)]
fn swar8(w: u64) -> u64 {
    let v = w & 0x0F0F_0F0F_0F0F_0F0F;
    let v = (v * 10 + (v >> 8)) & 0x00FF_00FF_00FF_00FF;
    let v = (v * 100 + (v >> 16)) & 0x0000_FFFF_0000_FFFF;
    (v * 10_000 + (v >> 32)) & 0xFFFF_FFFF
}

pub fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + if m <= 2 { 1 } else { 0 }, m, d)
}

/// ID -> small index. Direct array when every key is `<prefix><n>` with n in 1..=10^7
/// and no leading zero; otherwise a hash map.
pub struct IdIndex {
    prefix: Vec<u8>,
    direct: Option<Vec<u16>>, // index by n, u16::MAX = absent
    map: FxMap<Vec<u8>, u16>,
}

impl IdIndex {
    /// Entries are (id, index); the first entry for an id wins.
    pub fn new<'a>(prefix: &[u8], entries: impl Iterator<Item = (&'a [u8], u16)> + Clone) -> IdIndex {
        let mut max_n = 0usize;
        let mut ok = true;
        let mut any = false;
        for (k, _) in entries.clone() {
            any = true;
            match Self::number(prefix, k) {
                Some(n) => max_n = max_n.max(n),
                None => {
                    ok = false;
                    break;
                }
            }
        }
        if ok && any && max_n <= 10_000_000 {
            let mut v = vec![u16::MAX; max_n + 1];
            for (k, i) in entries {
                let n = Self::number(prefix, k).unwrap();
                if v[n] == u16::MAX {
                    v[n] = i;
                }
            }
            return IdIndex { prefix: prefix.to_vec(), direct: Some(v), map: FxMap::default() };
        }
        let mut map: FxMap<Vec<u8>, u16> = FxMap::default();
        for (k, i) in entries {
            map.entry(k.to_vec()).or_insert(i);
        }
        IdIndex { prefix: prefix.to_vec(), direct: None, map }
    }

    /// `<prefix>` then 1-8 ASCII digits without a leading zero.
    #[inline(always)]
    fn number(prefix: &[u8], f: &[u8]) -> Option<usize> {
        let rest = f.strip_prefix(prefix)?;
        if rest.is_empty() || rest.len() > 8 || rest[0] == b'0' {
            return None;
        }
        let mut n = 0usize;
        for &b in rest {
            let x = b.wrapping_sub(b'0');
            if x > 9 {
                return None;
            }
            n = n * 10 + x as usize;
        }
        Some(n)
    }

    #[inline(always)]
    pub fn get(&self, f: &[u8]) -> Option<u16> {
        match &self.direct {
            Some(v) => {
                let n = Self::number(&self.prefix, f)?;
                match v.get(n) {
                    Some(&i) if i != u16::MAX => Some(i),
                    _ => None,
                }
            }
            None => self.map.get(f).copied(),
        }
    }
}

#[inline(never)]
pub fn ts_probe(t: &TsTab, f: &[u8]) -> Option<i64> {
    t.parse(f)
}
