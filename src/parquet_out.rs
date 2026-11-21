//! Minimal Parquet writer: one row group, one PLAIN data page (v1) per column, no
//! compression, no statistics. Metadata is written with the Thrift compact protocol.
use crate::{Packer, UdRow};
use std::io::Write;
use std::path::Path;

// Parquet enums
const INT32: i32 = 1;
const INT64: i32 = 2;
const BYTE_ARRAY: i32 = 6;
const REQUIRED: i32 = 0;
const OPTIONAL: i32 = 1;
const CONV_UTF8: i32 = 0;
const CONV_DATE: i32 = 6;
const ENC_PLAIN: i32 = 0;
const ENC_RLE: i32 = 3;

// Thrift compact types
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

struct Tw {
    b: Vec<u8>,
    last: Vec<i16>,
}

impl Tw {
    fn new() -> Tw {
        Tw { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(v: i64) -> u64 {
        ((v << 1) ^ (v >> 63)) as u64
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let d = id - *last;
        *last = id;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.varint(Self::zz(id as i64));
        }
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.varint(Self::zz(v as i64));
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.varint(Self::zz(v));
    }
    fn bin(&mut self, id: i16, s: &[u8]) {
        self.field(id, T_BINARY);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.last.push(0);
    }
    /// A struct that is a list element (no field header).
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
}

#[derive(Clone, Copy)]
enum Logical {
    None,
    String,
    Date,
}

struct Column {
    name: &'static str,
    ptype: i32,
    optional: bool,
    logical: Logical,
    body: Vec<u8>, // page body: [definition levels] + PLAIN values
}

fn write_file(path: &Path, cols: &[Column], nrows: usize) {
    let mut out: Vec<u8> = Vec::with_capacity(cols.iter().map(|c| c.body.len() + 64).sum::<usize>() + 1024);
    out.extend_from_slice(b"PAR1");
    let mut chunks = Vec::new(); // (offset, size)
    if nrows > 0 {
        for c in cols {
            let mut h = Tw::new();
            h.i32(1, 0); // DATA_PAGE
            h.i32(2, c.body.len() as i32);
            h.i32(3, c.body.len() as i32);
            h.begin(5);
            h.i32(1, nrows as i32);
            h.i32(2, ENC_PLAIN);
            h.i32(3, ENC_RLE);
            h.i32(4, ENC_RLE);
            h.end();
            h.b.push(0);
            let off = out.len();
            out.extend_from_slice(&h.b);
            out.extend_from_slice(&c.body);
            chunks.push((off as i64, (out.len() - off) as i64));
        }
    }
    let mut m = Tw::new();
    m.i32(1, 1);
    m.list(2, T_STRUCT, cols.len() + 1);
    m.begin_elem();
    m.bin(4, b"schema");
    m.i32(5, cols.len() as i32);
    m.end();
    for c in cols {
        m.begin_elem();
        m.i32(1, c.ptype);
        m.i32(3, if c.optional { OPTIONAL } else { REQUIRED });
        m.bin(4, c.name.as_bytes());
        match c.logical {
            Logical::None => {}
            Logical::String => {
                m.i32(6, CONV_UTF8);
                m.begin(10);
                m.begin(1);
                m.end();
                m.end();
            }
            Logical::Date => {
                m.i32(6, CONV_DATE);
                m.begin(10);
                m.begin(6);
                m.end();
                m.end();
            }
        }
        m.end();
    }
    m.i64(3, nrows as i64);
    if nrows > 0 {
        m.list(4, T_STRUCT, 1);
        m.begin_elem();
        m.list(1, T_STRUCT, cols.len());
        for (c, &(off, size)) in cols.iter().zip(&chunks) {
            m.begin_elem();
            m.i64(2, off);
            m.begin(3);
            m.i32(1, c.ptype);
            m.list(2, T_I32, 2);
            m.varint(Tw::zz(ENC_PLAIN as i64));
            m.varint(Tw::zz(ENC_RLE as i64));
            m.list(3, T_BINARY, 1);
            m.varint(c.name.len() as u64);
            m.b.extend_from_slice(c.name.as_bytes());
            m.i32(4, 0); // UNCOMPRESSED
            m.i64(5, nrows as i64);
            m.i64(6, size);
            m.i64(7, size);
            m.i64(9, off);
            m.end();
            m.end();
        }
        m.i64(2, chunks.iter().map(|c| c.1).sum());
        m.i64(3, nrows as i64);
        m.end();
    } else {
        m.list(4, T_STRUCT, 0);
    }
    m.bin(6, b"shopping-sessions");
    m.b.push(0);
    out.extend_from_slice(&m.b);
    out.extend_from_slice(&(m.b.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    let mut f = std::fs::File::create(path).expect("create parquet file");
    f.write_all(&out).expect("write parquet file");
}

/// Definition levels (bit width 1) as one bit-packed run, with the v1 length prefix.
fn def_levels(defs: &[bool], body: &mut Vec<u8>) {
    let groups = defs.len().div_ceil(8);
    let mut run = Vec::with_capacity(groups + 10);
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        run.push((h as u8) | 0x80);
        h >>= 7;
    }
    run.push(h as u8);
    for g in defs.chunks(8) {
        let mut byte = 0u8;
        for (i, &d) in g.iter().enumerate() {
            byte |= (d as u8) << i;
        }
        run.push(byte);
    }
    body.extend_from_slice(&(run.len() as u32).to_le_bytes());
    body.extend_from_slice(&run);
}

fn put_str(body: &mut Vec<u8>, s: &[u8]) {
    body.extend_from_slice(&(s.len() as u32).to_le_bytes());
    body.extend_from_slice(s);
}

pub fn write_user_daily<P: Packer>(path: &Path, rows: &[UdRow], pk: &P, cat_names: &[Vec<u8>]) {
    let n = rows.len();
    let mut users = Vec::with_capacity(n * 13);
    let mut buf = Vec::with_capacity(16);
    for r in rows {
        buf.clear();
        pk.unpack(r.person, &mut buf);
        put_str(&mut users, &buf);
    }
    let mut day = Vec::with_capacity(n * 4);
    let mut sessions = Vec::with_capacity(n * 4);
    let mut events = Vec::with_capacity(n * 4);
    let mut revenue = Vec::with_capacity(n * 8);
    for r in rows {
        day.extend_from_slice(&r.day.to_le_bytes());
        sessions.extend_from_slice(&r.sessions.to_le_bytes());
        events.extend_from_slice(&r.events.to_le_bytes());
        revenue.extend_from_slice(&r.revenue.to_le_bytes());
    }
    let defs: Vec<bool> = rows.iter().map(|r| r.top != u16::MAX).collect();
    let mut top = Vec::with_capacity(n / 4 + 16);
    def_levels(&defs, &mut top);
    for r in rows {
        if r.top != u16::MAX {
            put_str(&mut top, &cat_names[r.top as usize]);
        }
    }
    let cols = [
        Column { name: "user_id", ptype: BYTE_ARRAY, optional: false, logical: Logical::String, body: users },
        Column { name: "day", ptype: INT32, optional: false, logical: Logical::Date, body: day },
        Column { name: "sessions", ptype: INT32, optional: false, logical: Logical::None, body: sessions },
        Column { name: "events", ptype: INT32, optional: false, logical: Logical::None, body: events },
        Column { name: "revenue_cents", ptype: INT64, optional: false, logical: Logical::None, body: revenue },
        Column { name: "top_category", ptype: BYTE_ARRAY, optional: true, logical: Logical::String, body: top },
    ];
    write_file(path, &cols, n);
}

pub fn write_campaign_daily(path: &Path, camp: &[i64], ndays: usize, day0: i64, ids: &[Vec<u8>], channels: &[Vec<u8>]) {
    let ncamp = ids.len();
    let (mut cid, mut ch, mut day) = (Vec::new(), Vec::new(), Vec::new());
    let (mut s, mut ps, mut rev, mut asst) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    let mut n = 0;
    for c in 0..=ncamp {
        let (id, chan): (&[u8], &[u8]) = if c == ncamp { (b"organic", b"organic") } else { (&ids[c], &channels[c]) };
        for d in 0..ndays {
            let k = (c * ndays + d) * 4;
            if camp[k] == 0 && camp[k + 3] == 0 {
                continue;
            }
            n += 1;
            put_str(&mut cid, id);
            put_str(&mut ch, chan);
            day.extend_from_slice(&((day0 + d as i64) as i32).to_le_bytes());
            s.extend_from_slice(&camp[k].to_le_bytes());
            ps.extend_from_slice(&camp[k + 1].to_le_bytes());
            rev.extend_from_slice(&camp[k + 2].to_le_bytes());
            asst.extend_from_slice(&camp[k + 3].to_le_bytes());
        }
    }
    let cols = [
        Column { name: "campaign_id", ptype: BYTE_ARRAY, optional: false, logical: Logical::String, body: cid },
        Column { name: "channel", ptype: BYTE_ARRAY, optional: false, logical: Logical::String, body: ch },
        Column { name: "day", ptype: INT32, optional: false, logical: Logical::Date, body: day },
        Column { name: "sessions", ptype: INT64, optional: false, logical: Logical::None, body: s },
        Column { name: "purchasing_sessions", ptype: INT64, optional: false, logical: Logical::None, body: ps },
        Column { name: "revenue_cents", ptype: INT64, optional: false, logical: Logical::None, body: rev },
        Column { name: "assisted_sessions", ptype: INT64, optional: false, logical: Logical::None, body: asst },
    ];
    write_file(path, &cols, n);
}
