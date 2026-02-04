-- Add migration script here
SELECT
        set_chunk_time_interval ('timings', INTERVAL '24 hours')
        -- TODO: should we drop timings.inner_hash and metadata.inner_hash?
        -- Can save some space but may slow down queries a bit