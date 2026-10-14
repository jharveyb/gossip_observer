# Data Analysis

Once you have some gossip-observer data locally as Parquet files, you can compute various metrics
using standard SQL via DuckDB. You can also load the data as dataframes and use other data
science tooling such as Polars, Pandas, etc.

## Setup

Requires uv (or you manually set up local dependencies).

`uv sync`

The data on disk should match the structure used in the S3 bucket where data is published:

### Gossip data layout

**v1** layout:

```sh
ls $DATADIR

message_first_seen  message_hashes  messages  metadata  timings

tree $DATADIR/message_hashes/
../$DATADIR/message_hashes/
└── 2026
    ├── 02
    │   ├── 04
    │   │   └── message_hashes_2026-02-04.parquet
...
```

So, ./TABLE_NAME/YYYY/MM/DD/table_name_YYYY-MM-DD.parquet.

Newer data is published as **v2**, which is ~28% smaller and lives under its own prefix:

```sh
tree $DATADIR/v2/
../$DATADIR/v2/
└── table=timings
    └── year=2026
        └── month=03
            ├── timings_2026-03-07_2026-03-08.parquet
            ├── timings_2026-03-08_2026-03-09.parquet
...
```

So, `./v2/table=TABLE_NAME/year=YYYY/month=MM/table_name_START_END.parquet`, where
`START`/`END` are the half-open range the file covers.

Both layouts can coexist in the same directory; `parquet_queries.py` picks v2 per
day where it exists and falls back to v1, so a partially converted tree just works.
See "Reading the v2 files" below for the differences that matter to a reader.

### Packet capture data layout

```sh
tree $DATADIR
├── 20260804T155011Z
│   ├── collectors-2
│   │   ├── capture_meta.json
│   │   ├── collectors-2-20260804T155011Z_00001_20260804155028.pcapng
│   │   ├── ...
│   │   ├── sockmap.tsv
│   │   └── split
│   │       ├── 019c26b8-3125-798a-afbf-cf3f308089cd.pcapng
│   │       ├── ...
│   │       └── manifest.json
│   └── collectors-3-big
...
```

## Fetching data

Use any S3-compatible tool for downloads. The endpoint is <https://observer-data.jharveyb.xyz/>. Ask me for read-only credentials.

Right now the published data size is ~750 MB per day in v1, or **~540 MB per day in
v2** (compressed Parquet, all tables) — v2 should be ~28.1% smaller.

With [rclone](https://rclone.org/), you can use the config snippet in `infra/ansible/DATA_SHARING.md` for bucket access. To fetch the March data:

```bash
# Confirm our rclone config is working
rclone lsd observer-data:gossip-observer-data

# Should show multiple folders, 'timings', 'messages', etc.

# Create our target directories
mkdir -p timings/2026/03
mkdir -p metadata/2026/03
mkdir -p messages/2026/03
mkdir -p message_hashes/2026/03
mkdir -p message_first_seen/2026/03

# Copy the March data for each table
rclone copy observer-data:gossip-observer-data/timings/2026/03 ./timings/2026/03/ -P
rclone copy observer-data:gossip-observer-data/metadata/2026/03 ./metadata/2026/03/ -P
rclone copy observer-data:gossip-observer-data/messages/2026/03 ./messages/2026/03/ -P
rclone copy observer-data:gossip-observer-data/message_hashes/2026/03 ./message_hashes/2026/03/ -P
rclone copy observer-data:gossip-observer-data/message_first_seen/2026/03 ./message_first_seen/2026/03/ -P
```

## Collectors

`collectors.json` lists every collector: pubkey, chain backend, and the SCIDs of
the channels it owns. The origination queries in `parquet_queries.py` use it to
tell a collector's own gossip apart from gossip it relays or serves. After
opening or closing a channel, update it and check it against the data:

```sh
uv run python derive_collectors.py --data-dir $DATADIR   # fails if the SCIDs disagree with the exports
```

The same run writes `collectors.csv`, the version published alongside the data.

## Examples

```sh
# Compute various propagation delay metrics for the data from days
# 2026-02-07 to 2026-02-09; also generate charts.
uv run parquet_queries.py 2026-02-09 --days 3 --data-dir $DATADIR/ outer

# Same command, but with higher cutoffs for how many peers have to send a
# message to us for it to be considered in the queries.
uv run parquet_queries.py 2026-02-09 --days 3 --originated-cutoff 11 --peer-cutoff 300 --data-dir $DATA_DIR/ outer

# Compute some more basic statistics not related to propagation delay
# over that same dataset.
uv run parquet_queries.py 2026-02-09 --days 3 --peer-cutoff 300 --data-dir $DATA_DIR/ stats
```

## Reading the v2 files

v2 keeps every column v1 had except two, and narrows the types of the rest. If you
use `parquet_queries.py` you can ignore all of this — `load_range` normalises both
formats to one shape. This section is for reading the files directly.

### Column changes

| table | column | v1 | v2 |
|---|---|---|---|
| timings | `hash`, `inner_hash` | 8-byte little-endian `BLOB` | `UBIGINT` |
| timings | `collector`, `peer` | 66-char hex `VARCHAR` | 33-byte `BLOB` |
| timings | `orig_timestamp` | `TIMESTAMPTZ` (µs) | `UINTEGER` (epoch seconds) |
| timings | `row_inc`, `peer_hash` | present | **removed** |
| metadata | `hash`, `inner_hash`, `scid` | `BLOB` | `UBIGINT` |
| metadata | `orig_node` | hex `VARCHAR`, nullable | `BLOB`, nullable |
| message_hashes | `outer_hash`, `inner_hash` | `BLOB` | `UBIGINT` |
| message_first_seen | `hash` | `BLOB` | `UBIGINT` |
| messages | `hash` | `BLOB` | `UBIGINT` |
| messages | `raw` | unpadded base64url `VARCHAR` | `BLOB` |

Converting back, if you need v1's spelling:

```sql
lower(hex(peer))          -- BLOB(33)  -> 66-char hex pubkey
to_timestamp(orig_timestamp)  -- epoch seconds -> TIMESTAMPTZ
```

`net_timestamp` is unchanged and still microsecond-precision `TIMESTAMPTZ`.
`orig_timestamp` narrows to whole seconds, which is lossless for every value in the
archive — the exporter refuses to write v2 for a chunk containing a sub-second
value rather than truncating one silently.

### Querying

Glob **per table**. `table=` is a partition key, and the five tables have different
columns:

```sql
-- good
SELECT count(*) FROM read_parquet('v2/table=timings/**/*.parquet', hive_partitioning = true);

-- wrong, and quietly so
SELECT count(*) FROM read_parquet('v2/**/*.parquet');
```

The second one does not error. `count(*)` never projects a real column, so no
schema conflict surfaces and you get the row count of *all five tables added
together* — a number that means nothing. The mistake only announces itself once you
select an actual column:

```
Invalid Input Error: schema mismatch in glob: column "first_seen" was read from the
original file ".../message_first_seen_...parquet", but could not be found in file
".../timings_...parquet"
```

So a cross-table glob is not protected by a loud failure — scope it to one table.

With `hive_partitioning = true` you also get `table`, `year` and `month` as virtual
columns from the directory names. Two gotchas: `table` is a reserved word, so it
needs quoting (`"table"`), and `month` comes back as a zero-padded `VARCHAR`
(`'03'`) while `year` is a `BIGINT`.

If you are unioning v2 with v1 files, pass `hive_partitioning = false` — v1 paths
have no `key=value` directories, so those three virtual columns would exist on one
side of the union and not the other.

### Recovering `peer_hash`

v2 drops `peer_hash` because it carried no information: it is XXH3-64 of `peer`'s
**66-character hex string** (not of the 33 raw bytes), stored little-endian. To
recompute it:

```python
import xxhash

def peer_hash(peer_blob: bytes) -> int:      # v2 `peer` is BLOB(33)
    return xxhash.xxh3_64_intdigest(peer_blob.hex().encode())
```

Note that most uses do not need it: `peer_hash` is 1:1 with `peer`.

`messages.hash` is built the same way, but over the **base64url text** rather than
the decoded bytes — so verifying a message hash from a v2 `BLOB` means re-encoding
it first.
