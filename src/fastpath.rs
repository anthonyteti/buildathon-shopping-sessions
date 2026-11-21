//! Fast decoders with exact fallbacks: SWAR timestamps (month table around the window)
//! and direct-index lookups for product and campaign IDs of the form `<prefix><n>`.
use crate::{DAY, FxMap, civil_ms, days_from_civil, parse_ts};

pub struct TsTab {
    prefix: Vec<u64>,        // "YYYY-MM-" of each month, little-endian
    months: Vec<(i64, i64)>, // (days since epoch of the 1st, days in month)
}

impl TsTab {
    /// Covers every month that touches [from_ms - 3 days, to_ms + 3 days].
    pub fn new(from_ms: i64, to_ms: i64) -> TsTab {
        let ym = |ms: i64| {
            let (y, m, _) = civil_from_days(ms.div_euclid(DAY));
            y * 12 + m - 1
        };
        let lo = ym(from_ms - 3 * DAY);
        let hi = ym(to_ms + 3 * DAY);
        let prefix = (lo..=hi)
            .map(|k| {
                let (y, m) = (k.div_euclid(12), k.rem_euclid(12) + 1);
                let s = format!("{y:04}-{m:02}-");
                if s.len() == 8 { u64::from_le_bytes(s.as_bytes().try_into().unwrap()) } else { 0 }
            })
            .collect();
        let months = (lo..=hi)
            .map(|k| {
                let (y, m) = (k.div_euclid(12), k.rem_euclid(12) + 1);
                let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
                let dim = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][(m - 1) as usize];
                (days_from_civil(y, m, 1), dim)
            })
            .collect();
        TsTab { prefix, months }
    }

    /// YYYY-MM-DDTHH:MM:SS in f[0..19] -> epoch ms. `f` needs 19 bytes.
    /// Months near the window are matched on their "YYYY-MM-" prefix; anything else
    /// goes to the general `civil_ms`.
    #[inline(always)]
    fn civil(&self, f: &[u8]) -> Option<i64> {
        let f: &[u8; 19] = f[..19].try_into().unwrap();
        let a = u64::from_le_bytes(f[0..8].try_into().unwrap());
        let mut j = usize::MAX;
        for (k, &p) in self.prefix.iter().enumerate() {
            if a == p {
                j = k;
            }
        }
        if j == usize::MAX {
            return civil_ms(f);
        }
        let d1 = f[8].wrapping_sub(b'0') as i64;
        let d2 = f[9].wrapping_sub(b'0') as i64;
        let day = d1 * 10 + d2;
        let (start, dim) = self.months[j];
        // "HH:MM:SS" at bytes 11..19
        let t = u64::from_le_bytes(f[11..19].try_into().unwrap());
        const DM: u64 = 0xFFFF_00FF_FF00_FFFF;
        let digits = (t & DM & 0xF0F0_F0F0_F0F0_F0F0) == (0x3030_3030_3030_3030 & DM)
            && (t.wrapping_add(0x0606_0606_0606_0606) & DM & 0xF0F0_F0F0_F0F0_F0F0) == (0x3030_3030_3030_3030 & DM);
        let seps = (t & 0x0000_FF00_00FF_0000) == 0x0000_3A00_003A_0000;
        if !(digits & seps & (f[10] == b'T') & (d1 <= 9) & (d2 <= 9) & (day >= 1) & (day <= dim)) {
            return None;
        }
        let v = t & 0x0F0F_0F0F_0F0F_0F0F;
        let p = v * 10 + (v >> 8);
        let (h, mi, s) = ((p & 0xFF) as i64, ((p >> 24) & 0xFF) as i64, ((p >> 48) & 0xFF) as i64);
        if h > 23 || mi > 59 || s > 59 {
            return None;
        }
        Some((start + day - 1) * DAY + ((h * 60 + mi) * 60 + s) * 1000)
    }

    /// ingest_ts, which the rules guarantee is a valid F1 timestamp: months near the
    /// window are decoded without re-validating; anything else takes the full parse.
    #[inline(always)]
    pub fn ingest(&self, f: &[u8]) -> i64 {
        if f.len() == 20 {
            let a = u64::from_le_bytes(f[0..8].try_into().unwrap());
            let mut j = usize::MAX;
            for (k, &p) in self.prefix.iter().enumerate() {
                if a == p {
                    j = k;
                }
            }
            if j != usize::MAX {
                let day = (f[8].wrapping_sub(b'0') as i64) * 10 + f[9].wrapping_sub(b'0') as i64;
                let t = u64::from_le_bytes(f[11..19].try_into().unwrap());
                let v = t & 0x0F0F_0F0F_0F0F_0F0F;
                let p = v * 10 + (v >> 8);
                let (h, mi, s) = ((p & 0xFF) as i64, ((p >> 24) & 0xFF) as i64, ((p >> 48) & 0xFF) as i64);
                return (self.months[j].0 + day - 1) * DAY + ((h * 60 + mi) * 60 + s) * 1000;
            }
        }
        self.parse(f).unwrap_or(i64::MAX)
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
