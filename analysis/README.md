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
