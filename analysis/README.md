# Data Analysis

Once you have some gossip-observer data locally as Parquet files, you can compute various metrics
using standard SQL via DuckDB. You can also load the data as dataframes and use other data
science tooling such as Polars, Pandas, etc.

## Setup

Requires uv (or you manually set up local dependencies).

`uv sync`

The data on disk should match the structure used in the S3 bucket where data is published:

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

### Fetching data

Use any S3-compatible tool for downloads. The endpoint is <https://observer-data.jharveyb.xyz/>. Ask me for read-only credentials.

Right now the published data size is ~700 MB per day (as compressed Parquet, counting all tables). So ~20 GB per month.

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
