-- Add our default 7-day compression policy for continuous aggregates that were
-- created with no compression enabled. We fetched the table names missing a
-- compression policy with the 'existing background jobs' diagnosis query.
ALTER MATERIALIZED VIEW ca_inner_msg_propagation_global_1h
SET
        (timescaledb.compress=true);

SELECT
        add_compression_policy ('ca_inner_msg_propagation_global_1h', compress_after=>INTERVAL '7 days');

ALTER MATERIALIZED VIEW ca_message_rate_10m
SET
        (timescaledb.compress=true);

SELECT
        add_compression_policy ('ca_message_rate_10m', compress_after=>INTERVAL '7 days');

ALTER MATERIALIZED VIEW ca_msg_propagation_global_1h
SET
        (timescaledb.compress=true);

SELECT
        add_compression_policy ('ca_msg_propagation_global_1h', compress_after=>INTERVAL '7 days');

ALTER MATERIALIZED VIEW ca_orig_node_activity_1h
SET
        (timescaledb.compress=true);

SELECT
        add_compression_policy ('ca_orig_node_activity_1h', compress_after=>INTERVAL '7 days');

ALTER MATERIALIZED VIEW ca_outbound_origin_1h
SET
        (timescaledb.compress=true);

SELECT
        add_compression_policy ('ca_outbound_origin_1h', compress_after=>INTERVAL '7 days');

ALTER MATERIALIZED VIEW ca_peer_count_10m
SET
        (timescaledb.compress=true);

SELECT
        add_compression_policy ('ca_peer_count_10m', compress_after=>INTERVAL '7 days');

ALTER MATERIALIZED VIEW ca_scid_activity_1h
SET
        (timescaledb.compress=true);

SELECT
        add_compression_policy ('ca_scid_activity_1h', compress_after=>INTERVAL '7 days');