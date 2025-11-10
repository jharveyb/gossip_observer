COPY messages FROM 'data/mainnet/gossip_archives/dump_0926T195046/messages.parquet' (FORMAT 'parquet', COMPRESSION 'zstd', ROW_GROUP_SIZE 245760);
COPY metadata FROM 'data/mainnet/gossip_archives/dump_0926T195046/metadata.parquet' (FORMAT 'parquet', COMPRESSION 'zstd', ROW_GROUP_SIZE 245760);
COPY timings FROM 'data/mainnet/gossip_archives/dump_0926T195046/timings.parquet' (FORMAT 'parquet', COMPRESSION 'zstd', ROW_GROUP_SIZE 245760);
