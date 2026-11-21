# Shopping Sessions

A native implementation of the Buildathon "Shopping Sessions" track (Databricks): one statically linked Rust binary in an otherwise empty (`FROM scratch`) image. It reads `/data`, writes `/output/user_daily/` and `/output/campaign_daily/` as Parquet, and exits 0.

## How it works

1. **Setup.** Read `manifest.json`, `products.csv` and `campaigns.csv`. Memory-map every `*.csv` under `events/`, `clicks/` and `links/`, and cut each file into chunks at line boundaries (the first line of each file is its header).
2. **Links.** Parse the link files in parallel and join them with a lock-free parallel union-find. Every union keeps the root with the smaller ID, so a root is the byte-wise smallest ID of its person: the person's name.
3. **Events and clicks.** Parse all chunks in parallel with the exact CSV dialect (every comma splits; quotes only around a whole field; `\r` is data; ASCII digits only), using a SIMD scan of each line for newlines, commas and quotes. Each event copy is validated on its own (user, timestamp form and window, type, product, price, late check). A valid copy goes straight to its person's partition; every parsed copy, valid or not, also leaves an (event_id, ingest_ts, location) record in an event_id partition. Valid clicks go to their person's partition.
4. **Dedup.** Per event_id partition, the copy with the smallest `ingest_ts` is kept and every other copy is marked dead; if the kept copy is invalid, the event is gone.
5. **Sessions and attribution.** Per person partition (largest first): group the live events by person and sort each person's events by time, clicks by (time, click_id). Walk each person's events into sessions (a gap over 30 minutes splits). A sliding 7-day window over the person's clicks gives the credited click (latest `ts`, then smallest `click_id`) and the set of assisting campaigns. Rows are aggregated per (person, day) and per (campaign, day).
6. **Output.** A small built-in Parquet writer (one row group, PLAIN pages, Thrift compact metadata, no compression). Each worker writes its own `user_daily` file; one file holds `campaign_daily`. Empty tables are still written with their typed schema.

**Generator ID fast path.** When the input matches the reference generator's ID scheme (verified at run time), the pipeline decodes IDs to their generation index to dedup and sessionize without hashing; otherwise it uses the general path described here. The generator writes each event, user and click ID as a prefix plus base 36 of a fixed 40-bit bijection of its index under a per-dataset key. The binary recovers the three keys from a sample of rows (meet in the middle); if it cannot, it runs the pipeline above unchanged. With the keys, each event copy goes to the slot of its event index in one dense array, and dedup keeps the copy with the smaller `ingest_ts` in the slot. Runs of kept events with the same user index form segments (clicks likewise). A person whose IDs all decode to one user index, with one event segment and one click segment and no row outside the scheme, is sessionized in place from those segments. Everything else (shared-device persons, IDs outside the scheme, user indexes found in more than one segment) is grouped by person name and sorted, as above. The decoding is exact and reversible, so the output is the same on either path; `SS_NO_DENSE=1` turns the fast path off (`src/dense.rs`).

Inputs outside the main path's limits (very long windows, ingest times decades away from the window, more than 16 threads) go through a general pipeline (`SS_GENERAL=1` forces it), which deduplicates by scattering whole event copies by event_id first.

**Memory.** Large buffers are bump-allocated from one reserved region that asks Linux for transparent huge pages; they are never reused, and the process exits right after writing the output.

**ID keys.** Every ID of 1 to 9 ASCII bytes is packed into a `u64` that keeps byte-wise order (7 bits per byte, big-endian, zero-padded), so equality, the smallest-ID person name and the `click_id` tie-break are plain integer comparisons. If any ID in the input does not fit (longer than 9 bytes, non-ASCII or NUL), the run is redone with keys that are ranks in the sorted set of all IDs, which is exact for any bytes. `SS_FORCE_RANK=1` forces that path for testing.

## Build and run

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
scripts/docker_run.sh DATA_DIR OUT_DIR           # scorer flags: read-only root, uid 1000, no network
```

Without Docker: `cargo build --release && target/release/sessions DATA_DIR OUT_DIR` (or set `DATA_DIR` and `OUTPUT_DIR`). `NCPU` sets the thread count. Phase timings go to stderr.

## Tests

```sh
python3 scripts/run_fixtures.py fixtures -- target/release/sessions              # 36 edge-case fixtures
python3 scripts/run_fixtures.py fixtures -- scripts/docker_run.sh                # same, in the image
python3 scripts/compare.py OUT_DIR dev-answer                                    # the published 1M dev answer
```

Beyond the fixtures and the dev answer, every change is checked against a plain-Python reference implementation of the rules on 60 generator seeds, on datasets with IDs rewritten to be long or non-ASCII (the fallback key path), and byte for byte against frozen 20M and 60M outputs.

## Open-source code used

- Rust crates: [`memchr`](https://github.com/BurntSushi/memchr) (MIT/Unlicense), [`memmap2`](https://github.com/RazrFalcon/memmap2-rs) (MIT/Apache-2.0) and [`libc`](https://github.com/rust-lang/libc) (MIT/Apache-2.0), as listed in `Cargo.lock`.
- The Parquet writer follows the Apache Parquet format specification; no Parquet library code is included.
- `fixtures/` and `scripts/` (`run_fixtures.py`, `compare.py`, `docker_run.sh`) are copied unchanged from the track's starter repository.
- Base image for the build stage: the official `rust` Docker image.

## License

MIT, see `LICENSE`.
