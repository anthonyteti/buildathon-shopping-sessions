//! Parquet output: PLAIN encoding, uncompressed, no dictionary, no statistics, one row group.
use crate::{Packer, UdRow};
use parquet::basic::{Compression, Encoding};
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use std::path::Path;
use std::sync::Arc;

const UD_SCHEMA: &str = "message user_daily {
  required binary user_id (STRING);
  required int32 day (DATE);
  required int32 sessions;
  required int32 events;
  required int64 revenue_cents;
  optional binary top_category (STRING);
}";

const CD_SCHEMA: &str = "message campaign_daily {
  required binary campaign_id (STRING);
  required binary channel (STRING);
  required int32 day (DATE);
  required int64 sessions;
  required int64 purchasing_sessions;
  required int64 revenue_cents;
  required int64 assisted_sessions;
}";

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::PLAIN)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_data_page_size_limit(8 << 20)
            .set_write_batch_size(1 << 16)
            .build(),
    )
}

type W = SerializedFileWriter<std::io::BufWriter<std::fs::File>>;

fn open(path: &Path, schema: &str) -> W {
    let schema = Arc::new(parse_message_type(schema).unwrap());
    let file = std::fs::File::create(path).expect("create parquet file");
    SerializedFileWriter::new(std::io::BufWriter::with_capacity(4 << 20, file), schema, props()).unwrap()
}

macro_rules! col {
    ($rg:expr, $ty:ty, $vals:expr, $def:expr) => {{
        let mut c = $rg.next_column().unwrap().unwrap();
        c.typed::<$ty>().write_batch($vals, $def, None).unwrap();
        c.close().unwrap();
    }};
}

pub fn write_user_daily<P: Packer>(path: &Path, rows: &[UdRow], pk: &P, cat_names: &[Vec<u8>]) {
    let cats: Vec<ByteArray> = cat_names.iter().map(|c| ByteArray::from(c.clone())).collect();
    let mut users = Vec::with_capacity(rows.len());
    let mut day = Vec::with_capacity(rows.len());
    let mut sessions = Vec::with_capacity(rows.len());
    let mut events = Vec::with_capacity(rows.len());
    let mut revenue = Vec::with_capacity(rows.len());
    let mut top = Vec::new();
    let mut top_def = Vec::with_capacity(rows.len());
    let mut buf = Vec::with_capacity(16);
    for r in rows {
        buf.clear();
        pk.unpack(r.person, &mut buf);
        users.push(ByteArray::from(buf.clone()));
        day.push(r.day);
        sessions.push(r.sessions);
        events.push(r.events);
        revenue.push(r.revenue);
        if r.top == u16::MAX {
            top_def.push(0i16);
        } else {
            top_def.push(1);
            top.push(cats[r.top as usize].clone());
        }
    }
    let mut w = open(path, UD_SCHEMA);
    let mut rg = w.next_row_group().unwrap();
    col!(rg, ByteArrayType, &users, None);
    col!(rg, Int32Type, &day, None);
    col!(rg, Int32Type, &sessions, None);
    col!(rg, Int32Type, &events, None);
    col!(rg, Int64Type, &revenue, None);
    col!(rg, ByteArrayType, &top, Some(&top_def));
    rg.close().unwrap();
    w.close().unwrap();
}

pub fn write_campaign_daily(path: &Path, camp: &[i64], ndays: usize, day0: i64, ids: &[Vec<u8>], channels: &[Vec<u8>]) {
    let ncamp = ids.len();
    let organic = ByteArray::from(&b"organic"[..]);
    let mut cid = Vec::new();
    let mut ch = Vec::new();
    let mut day = Vec::new();
    let (mut s, mut ps, mut rev, mut asst) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for c in 0..=ncamp {
        let (id, chan) = if c == ncamp {
            (organic.clone(), organic.clone())
        } else {
            (ByteArray::from(ids[c].clone()), ByteArray::from(channels[c].clone()))
        };
        for d in 0..ndays {
            let k = (c * ndays + d) * 4;
            if camp[k] == 0 && camp[k + 3] == 0 {
                continue;
            }
            cid.push(id.clone());
            ch.push(chan.clone());
            day.push((day0 + d as i64) as i32);
            s.push(camp[k]);
            ps.push(camp[k + 1]);
            rev.push(camp[k + 2]);
            asst.push(camp[k + 3]);
        }
    }
    let mut w = open(path, CD_SCHEMA);
    let mut rg = w.next_row_group().unwrap();
    col!(rg, ByteArrayType, &cid, None);
    col!(rg, ByteArrayType, &ch, None);
    col!(rg, Int32Type, &day, None);
    col!(rg, Int64Type, &s, None);
    col!(rg, Int64Type, &ps, None);
    col!(rg, Int64Type, &rev, None);
    col!(rg, Int64Type, &asst, None);
    rg.close().unwrap();
    w.close().unwrap();
}
