-- ### Related tables to State Indexer ###
--canonical_execution_nonce_reads
--canonical_execution_nonce_diffs
--canonical_execution_balance_diffs
--canonical_execution_balance_reads
--canonical_execution_storage_diffs
--canonical_execution_storage_reads
--canonical_execution_contracts

-- ### DEBUG ###
-- ### ACCOUNT LAST ACCESS ###
-- default.accounts_last_access_local stores the latest access records for each account (local table)
CREATE TABLE default.accounts_last_access_local on cluster '{cluster}' (
    address            String,
    last_access_block  UInt64
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    last_access_block
)
ORDER BY (address);

CREATE TABLE default.accounts_last_access on cluster '{cluster}' AS default.accounts_last_access_local
ENGINE = Distributed('{cluster}', default, accounts_last_access_local, cityHash64(address));

CREATE MATERIALIZED VIEW mv_nonce_reads_to_accounts_last_access_local on cluster '{cluster}'
TO default.accounts_last_access_local AS
SELECT
    lower(address) as address,
    max(block_number) AS last_access_block
FROM default.canonical_execution_nonce_reads
GROUP BY address;

CREATE MATERIALIZED VIEW mv_nonce_diffs_to_accounts_last_access_local on cluster '{cluster}'
TO default.accounts_last_access_local AS
SELECT
    lower(address) as address,
    max(block_number) AS last_access_block
FROM default.canonical_execution_nonce_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_balance_diffs_to_accounts_last_access_local on cluster '{cluster}'
TO default.accounts_last_access_local AS
SELECT
    lower(address) as address,
    max(block_number) AS last_access_block
FROM default.canonical_execution_balance_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_balance_reads_to_accounts_last_access_local on cluster '{cluster}'
TO default.accounts_last_access_local AS
SELECT
    lower(address) as address,
    max(block_number) AS last_access_block
FROM default.canonical_execution_balance_reads
GROUP BY address;

CREATE MATERIALIZED VIEW mv_storage_diffs_to_accounts_last_access_local on cluster '{cluster}'
TO default.accounts_last_access_local AS
SELECT
    lower(address) as address,
    max(block_number) AS last_access_block
FROM default.canonical_execution_storage_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_storage_reads_to_accounts_last_access_local on cluster '{cluster}'
TO default.accounts_last_access_local AS
SELECT
    lower(contract_address) as address,
    max(block_number) AS last_access_block
FROM default.canonical_execution_storage_reads
GROUP BY contract_address;

CREATE MATERIALIZED VIEW mv_contracts_to_accounts_last_access_local on cluster '{cluster}'
TO default.accounts_last_access_local AS
SELECT
    lower(contract_address) as address,
    max(block_number) AS last_access_block
FROM default.canonical_execution_contracts
GROUP BY contract_address;

-- ### STORAGE LAST ACCESS ###
CREATE TABLE default.storage_last_access_local on cluster '{cluster}' (
    address            String,
    slot_key           String,
    last_access_block  UInt64,
    is_deleted         Bool,
    version            UInt128
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
    '{replica}',
    version
)
ORDER BY (address, slot_key);

CREATE TABLE default.storage_last_access on cluster '{cluster}' AS default.storage_last_access_local
ENGINE = Distributed('{cluster}', default, storage_last_access_local, cityHash64(address, slot_key));

CREATE MATERIALIZED VIEW mv_storage_diffs_to_storage_last_access_local on cluster '{cluster}'
TO default.storage_last_access_local AS
SELECT
    lower(address) as address,
    slot AS slot_key,
    max(block_number) AS last_access_block,
    argMax(to_value, (block_number, transaction_index, internal_index)) = '0x0000000000000000000000000000000000000000000000000000000000000000' AS is_deleted,
    -- Makes sure that we always get the latest version of the storage slot
    (bitShiftLeft(toUInt128(max(block_number)), 64) + bitShiftLeft(toUInt128(argMax(transaction_index, (block_number, transaction_index))), 32) + toUInt128(argMax(internal_index, (block_number, transaction_index, internal_index)))) AS version
FROM default.canonical_execution_storage_diffs
GROUP BY address, slot;

CREATE MATERIALIZED VIEW mv_storage_reads_to_storage_last_access_local on cluster '{cluster}'
TO default.storage_last_access_local AS
SELECT
    lower(contract_address) as address,
    slot AS slot_key,
    max(block_number) AS last_access_block,
    argMax(value, (block_number, transaction_index, internal_index)) = '0x0000000000000000000000000000000000000000000000000000000000000000' AS is_deleted,
    -- We do not use internal index here because internal indices are not unique, but it is purely for ordering in the same table.
    -- That is, same combination of (block_number, transaction_index, internal_index) can appear multiple times with storage_diffs.
    -- Therefore, in storage_reads, it will overwrite the latest record if and only if there isn't a storage_diffs record in the same block.
    -- This ensures that we will always keep the latest last_access_block and is_deleted for the same slot if there is a storage_diffs record in the same block.
    -- And if there's no storage_diffs record, then the storage_reads will overwrite the latest record.
    (bitShiftLeft(toUInt128(max(block_number)), 64)) AS version
FROM default.canonical_execution_storage_reads
GROUP BY contract_address, slot;

-- ### ACCOUNT FIRST ACCESS ###
CREATE TABLE default.accounts_first_access_local on cluster '{cluster}' (
  address              String,
  first_access_block   UInt64,
  version               Int64           -- == -toInt64(first_access_block)
) ENGINE = ReplicatedReplacingMergeTree(
  '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
  '{replica}',
  version
)
ORDER BY (address);

CREATE TABLE default.accounts_first_access on cluster '{cluster}' AS default.accounts_first_access_local
  ENGINE = Distributed('{cluster}', default, accounts_first_access_local, cityHash64(address));

CREATE MATERIALIZED VIEW mv_nonce_reads_to_accounts_first_access_local on cluster '{cluster}'
TO default.accounts_first_access_local AS
SELECT
    lower(address) as address,
    min(block_number) AS first_access_block,
    -toInt64(min(block_number)) AS version
FROM default.canonical_execution_nonce_reads
GROUP BY address;

CREATE MATERIALIZED VIEW mv_nonce_diffs_to_accounts_first_access_local on cluster '{cluster}'
TO default.accounts_first_access_local AS
SELECT
    lower(address) as address,
    min(block_number) AS first_access_block,
    -toInt64(min(block_number)) AS version
FROM default.canonical_execution_nonce_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_balance_diffs_to_accounts_first_access_local on cluster '{cluster}'
TO default.accounts_first_access_local AS
SELECT
    lower(address) as address,
    min(block_number) AS first_access_block,
    -toInt64(min(block_number)) AS version
FROM default.canonical_execution_balance_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_balance_reads_to_accounts_first_access_local on cluster '{cluster}'
TO default.accounts_first_access_local AS
SELECT
    lower(address) as address,
    min(block_number) AS first_access_block,
    -toInt64(min(block_number)) AS version
FROM default.canonical_execution_balance_reads
GROUP BY address;

CREATE MATERIALIZED VIEW mv_storage_diffs_to_accounts_first_access_local on cluster '{cluster}'
TO default.accounts_first_access_local AS
SELECT
    lower(address) as address,
    min(block_number) AS first_access_block,
    -toInt64(min(block_number)) AS version
FROM default.canonical_execution_storage_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_storage_reads_to_accounts_first_access_local on cluster '{cluster}'
TO default.accounts_first_access_local AS
SELECT
    lower(contract_address) as address,
    min(block_number) AS first_access_block,
    -toInt64(min(block_number)) AS version
FROM default.canonical_execution_storage_reads
GROUP BY contract_address;

CREATE MATERIALIZED VIEW mv_contracts_to_accounts_first_access_local on cluster '{cluster}'
TO default.accounts_first_access_local AS
SELECT
    lower(contract_address) as address,
    min(block_number) AS first_access_block,
    -toInt64(min(block_number)) AS version
FROM default.canonical_execution_contracts
GROUP BY contract_address;

-- ### STORAGE FIRST ACCESS ###
CREATE TABLE default.storage_first_access_local on cluster '{cluster}' (
    address              String,
    slot                 String,
    first_access_block   UInt64,
    version               Int64           -- == -toInt64(first_access_block)
) ENGINE = ReplicatedReplacingMergeTree(
  '/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}',
  '{replica}',
  version
)
ORDER BY (address, slot);

CREATE TABLE default.storage_first_access on cluster '{cluster}' AS default.storage_first_access_local
  ENGINE = Distributed('{cluster}', default, storage_first_access_local, cityHash64(address, slot));

CREATE MATERIALIZED VIEW mv_storage_reads_to_storage_first_access_local on cluster '{cluster}'
TO default.storage_first_access_local AS
SELECT
    lower(contract_address) as address,
    slot AS slot,
    min(block_number) AS first_access_block,
    -toInt64(min(block_number)) AS version
FROM default.canonical_execution_storage_reads
GROUP BY contract_address, slot;

CREATE MATERIALIZED VIEW mv_storage_diffs_to_storage_first_access_local on cluster '{cluster}'
TO default.storage_first_access_local AS
SELECT
    lower(address) as address,
    slot AS slot,
    min(block_number) AS first_access_block,
    -toInt64(min(block_number)) AS version
FROM default.canonical_execution_storage_diffs
GROUP BY address, slot;