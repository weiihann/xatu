-- ### Related tables to State Indexer ###
--canonical_execution_nonce_reads
--canonical_execution_nonce_diffs
--canonical_execution_balance_diffs
--canonical_execution_balance_reads
--canonical_execution_storage_diffs
--canonical_execution_storage_reads
--canonical_execution_contracts

-- ### ACCOUNT STATE ###
-- accounts_state_local stores the latest access records for each account (local table)
CREATE TABLE accounts_state_local on cluster '{cluster}' (
    address            String,
    last_access_block  UInt64
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    last_access_block
) PARTITION BY intDiv(last_access_block, 5000000)
ORDER BY (address);

CREATE TABLE accounts_state on cluster '{cluster}' AS accounts_state_local
ENGINE = Distributed('{cluster}', default, accounts_state_local, rand());

CREATE MATERIALIZED VIEW mv_nonce_reads_to_accounts_state_local on cluster '{cluster}'
TO accounts_state_local AS
SELECT
    lower(address) as address,
    max(block_number) AS last_access_block
FROM canonical_execution_nonce_reads
GROUP BY address;

CREATE MATERIALIZED VIEW mv_nonce_diffs_to_accounts_state_local on cluster '{cluster}'
TO accounts_state_local AS
SELECT
    lower(address) as address,
    max(block_number) AS last_access_block
FROM canonical_execution_nonce_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_balance_diffs_to_accounts_state_local on cluster '{cluster}'
TO accounts_state_local AS
SELECT
    lower(address) as address,
    max(block_number) AS last_access_block
FROM canonical_execution_balance_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_balance_reads_to_accounts_state_local on cluster '{cluster}'
TO accounts_state_local AS
SELECT
    lower(address) as address,
    max(block_number) AS last_access_block
FROM canonical_execution_balance_reads
GROUP BY address;

CREATE MATERIALIZED VIEW mv_storage_diffs_to_accounts_state_local on cluster '{cluster}'
TO accounts_state_local AS
SELECT
    lower(address) as address,
    max(block_number) AS last_access_block
FROM canonical_execution_storage_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_storage_reads_to_accounts_state_local on cluster '{cluster}'
TO accounts_state_local AS
SELECT
    lower(contract_address) as address,
    max(block_number) AS last_access_block
FROM canonical_execution_storage_reads
GROUP BY contract_address;

CREATE MATERIALIZED VIEW mv_contracts_to_accounts_state_local on cluster '{cluster}'
TO accounts_state_local AS
SELECT
    lower(contract_address) as address,
    max(block_number) AS last_access_block
FROM canonical_execution_contracts
GROUP BY contract_address;

-- ### STORAGE STATE ###

CREATE TABLE storage_state_local on cluster '{cluster}' (
    address            String,
    slot_key           String,
    last_access_block  UInt64
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    last_access_block
) PARTITION BY intDiv(last_access_block, 5000000)
ORDER BY (address, slot_key);

CREATE TABLE storage_state on cluster '{cluster}' AS storage_state_local
ENGINE = Distributed('{cluster}', default, storage_state_local, rand());

CREATE MATERIALIZED VIEW mv_storage_diffs_to_storage_state_local on cluster '{cluster}'
TO storage_state_local AS
SELECT
    lower(address) as address,
    slot AS slot_key,
    max(block_number) AS last_access_block
FROM canonical_execution_storage_diffs
GROUP BY address, slot;

CREATE MATERIALIZED VIEW mv_storage_reads_to_storage_state_local on cluster '{cluster}'
TO storage_state_local AS
SELECT
    lower(contract_address) as address,
    slot AS slot_key,
    max(block_number) AS last_access_block
FROM canonical_execution_storage_reads
GROUP BY contract_address, slot;

-- ### CONTRACT STORAGE COUNT AGG ###
CREATE TABLE contract_storage_count_agg_local on cluster '{cluster}' (
    address     String,
    total_slots AggregateFunction(uniq, String)
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
ORDER BY (address);

CREATE TABLE contract_storage_count_agg on cluster '{cluster}' AS contract_storage_count_agg_local
ENGINE = Distributed('{cluster}', default, contract_storage_count_agg_local, rand());

CREATE MATERIALIZED VIEW mv_storage_diffs_to_contract_storage_count_agg_local on cluster '{cluster}'
TO contract_storage_count_agg_local AS
SELECT
    lower(address) as address,
    uniqState(slot) AS total_slots
FROM canonical_execution_storage_diffs
GROUP BY address;

-- ### ACCOUNT ACCESS COUNT AGG ###
CREATE TABLE account_access_count_agg_local on cluster '{cluster}' (
    address       String,
    is_contract   AggregateFunction(max, UInt8),
    read_count    AggregateFunction(count, UInt64),
    write_count   AggregateFunction(count, UInt64)
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
ORDER BY (address);

CREATE TABLE account_access_count_agg on cluster '{cluster}' AS account_access_count_agg_local
ENGINE = Distributed('{cluster}', default, account_access_count_agg_local, rand());

CREATE MATERIALIZED VIEW mv_nonce_reads_to_account_access_count_agg_local on cluster '{cluster}'
TO account_access_count_agg_local AS
SELECT
    lower(address) as address,
    maxState(toUInt8(false)) AS is_contract,
    countState(block_number) AS read_count
FROM canonical_execution_nonce_reads
GROUP BY address;

CREATE MATERIALIZED VIEW mv_nonce_diffs_to_account_access_count_agg_local on cluster '{cluster}'
TO account_access_count_agg_local AS
SELECT
    lower(address) as address,
    maxState(toUInt8(false)) AS is_contract,
    countState(block_number) AS write_count
FROM canonical_execution_nonce_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_balance_diffs_to_account_access_count_agg_local on cluster '{cluster}'
TO account_access_count_agg_local AS
SELECT
    lower(address) as address,
    maxState(toUInt8(false)) AS is_contract,
    countState(block_number) AS write_count
FROM canonical_execution_balance_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_balance_reads_to_account_access_count_agg_local on cluster '{cluster}'
TO account_access_count_agg_local AS
SELECT
    lower(address) as address,
    maxState(toUInt8(false)) AS is_contract,
    countState(block_number) AS read_count
FROM canonical_execution_balance_reads
GROUP BY address;

CREATE MATERIALIZED VIEW mv_storage_diffs_to_account_access_count_agg_local on cluster '{cluster}'
TO account_access_count_agg_local AS
SELECT
    lower(address) as address,
    maxState(toUInt8(true)) AS is_contract,
    countState(block_number) AS write_count
FROM canonical_execution_storage_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_storage_reads_to_account_access_count_agg_local on cluster '{cluster}'
TO account_access_count_agg_local AS
SELECT
    lower(contract_address) as address,
    maxState(toUInt8(true)) AS is_contract,
    countState(block_number) AS read_count
FROM canonical_execution_storage_reads
GROUP BY address;

CREATE MATERIALIZED VIEW mv_contracts_to_account_access_count_agg_local on cluster '{cluster}'
TO account_access_count_agg_local AS
SELECT
    lower(contract_address) as address,
    maxState(toUInt8(true)) AS is_contract
FROM canonical_execution_contracts
GROUP BY contract_address;

-- ### STORAGE ACCESS COUNT AGG ###
CREATE TABLE storage_access_count_agg_local on cluster '{cluster}' (
    address       String,
    slot_key      String,
    read_count    AggregateFunction(count, UInt64),
    write_count   AggregateFunction(count, UInt64)
) ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
ORDER BY (address, slot_key);

CREATE TABLE storage_access_count_agg on cluster '{cluster}' AS storage_access_count_agg_local
ENGINE = Distributed('{cluster}', default, storage_access_count_agg_local, rand());

CREATE MATERIALIZED VIEW mv_storage_diffs_to_storage_access_count_agg_local on cluster '{cluster}'
TO storage_access_count_agg_local AS
SELECT
    lower(address) as address,
    slot AS slot_key,
    countState(block_number) AS write_count
FROM canonical_execution_storage_diffs
GROUP BY address, slot;

CREATE MATERIALIZED VIEW mv_storage_reads_to_storage_access_count_agg_local on cluster '{cluster}'
TO storage_access_count_agg_local AS
SELECT
    lower(contract_address) as address,
    slot as slot_key,
    countState(block_number) AS read_count
FROM canonical_execution_storage_reads
GROUP BY address, slot;