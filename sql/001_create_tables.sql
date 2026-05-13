CREATE DATABASE IF NOT EXISTS {database};

CREATE TABLE IF NOT EXISTS {database}.raw_events
(
    received_at DateTime64(6, 'UTC'),
    endpoint LowCardinality(String),
    channel LowCardinality(String),
    product_code LowCardinality(String),
    event_type LowCardinality(String),
    collector_instance_id LowCardinality(String),
    payload_hash String,
    payload_json String,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (event_type, product_code, channel, payload_hash);

CREATE TABLE IF NOT EXISTS {database}.raw_executions
(
    received_at DateTime64(6, 'UTC'),
    product_code LowCardinality(String),
    channel LowCardinality(String),
    collector_instance_id LowCardinality(String),
    payload_hash String,
    payload_json String,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (product_code, channel, payload_hash);

CREATE TABLE IF NOT EXISTS {database}.raw_tickers
(
    received_at DateTime64(6, 'UTC'),
    product_code LowCardinality(String),
    channel LowCardinality(String),
    collector_instance_id LowCardinality(String),
    payload_hash String,
    payload_json String,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (product_code, channel, payload_hash);

CREATE TABLE IF NOT EXISTS {database}.raw_board_snapshots
(
    received_at DateTime64(6, 'UTC'),
    product_code LowCardinality(String),
    channel LowCardinality(String),
    collector_instance_id LowCardinality(String),
    payload_hash String,
    payload_json String,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (product_code, channel, payload_hash);

CREATE TABLE IF NOT EXISTS {database}.raw_board_deltas
(
    received_at DateTime64(6, 'UTC'),
    product_code LowCardinality(String),
    channel LowCardinality(String),
    collector_instance_id LowCardinality(String),
    payload_hash String,
    payload_json String,
    inserted_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (product_code, channel, payload_hash);
