-- ### Related tables to State Indexer ###
--canonical_execution_nonce_reads
--canonical_execution_nonce_diffs
--canonical_execution_balance_diffs
--canonical_execution_balance_reads
--canonical_execution_storage_diffs
--canonical_execution_storage_reads
--canonical_execution_contracts

-- ### ACCOUNT STATE ###

-- accounts_state store the latest access records for each account
CREATE TABLE accounts_state (
    address            String,
    is_contract        AggregateFunction(max, UInt8), -- true if any source says it's a contract
    last_read_block    AggregateFunction(max, UInt64), -- max of all read blocks
    last_write_block   AggregateFunction(max, UInt64), -- max of all write blocks
    last_access_block  AggregateFunction(max, UInt64)  -- max of all access blocks
) ENGINE = AggregatingMergeTree()
ORDER BY (address)

-- nonce_reads update the last_read_block and last_access_block for each account
CREATE MATERIALIZED VIEW mv_nonce_reads_to_accounts_state
TO accounts_state AS
SELECT
    address,
    maxState(toUInt8(false)) AS is_contract,
    maxState(block_number) AS last_read_block,
    maxState(toUInt64(0)) AS last_write_block,
    maxState(block_number) AS last_access_block
FROM canonical_execution_nonce_reads
GROUP BY address;

-- nonce_diffs update the last_write_block and last_access_block for each account
CREATE MATERIALIZED VIEW mv_nonce_diffs_to_accounts_state
TO accounts_state AS
SELECT
    address,
    maxState(toUInt8(false)) AS is_contract,
    maxState(toUInt64(0)) AS last_read_block,
    maxState(block_number) AS last_write_block,
    maxState(block_number) AS last_access_block
FROM canonical_execution_nonce_diffs
GROUP BY address;

-- balance_diffs update the last_write_block and last_access_block for each account
CREATE MATERIALIZED VIEW mv_balance_diffs_to_accounts_state
TO accounts_state AS
SELECT
    address,
    maxState(toUInt8(false)) AS is_contract,
    maxState(toUInt64(0)) AS last_read_block,
    maxState(block_number) AS last_write_block,
    maxState(block_number) AS last_access_block
FROM canonical_execution_balance_diffs
GROUP BY address;

-- balance_reads update the last_read_block and last_access_block for each account
CREATE MATERIALIZED VIEW mv_balance_reads_to_accounts_state
TO accounts_state AS
SELECT
    address,
    maxState(toUInt8(false)) AS is_contract,
    maxState(block_number) AS last_read_block,
    maxState(toUInt64(0)) AS last_write_block,
    maxState(block_number) AS last_access_block
FROM canonical_execution_balance_reads
GROUP BY address;

-- storage_diffs update the last_access_block for each account
CREATE MATERIALIZED VIEW mv_storage_diffs_to_accounts_state
TO accounts_state AS
SELECT
    address,
    maxState(toUInt8(true)) AS is_contract, -- storage diffs are always for contracts
    maxState(toUInt64(0)) AS last_read_block,
    maxState(toUInt64(0)) AS last_write_block,
    maxState(toUInt64(block_number)) AS last_access_block
FROM canonical_execution_storage_diffs
GROUP BY address;

-- storage_reads update the last_access_block for each account
CREATE MATERIALIZED VIEW mv_storage_reads_to_accounts_state
TO accounts_state AS
SELECT
    contract_address AS address,
    maxState(toUInt8(true)) AS is_contract,
    maxState(toUInt64(0)) AS last_read_block,
    maxState(toUInt64(0)) AS last_write_block,
    maxState(toUInt64(block_number)) AS last_access_block
FROM canonical_execution_storage_reads
GROUP BY contract_address;

-- contracts mark the account as a contract
CREATE MATERIALIZED VIEW mv_contracts_to_accounts_state
TO accounts_state AS
SELECT
    contract_address AS address,
    maxState(toUInt8(true)) AS is_contract,
    maxState(toUInt64(0)) AS last_read_block,
    maxState(toUInt64(0)) AS last_write_block,
    maxState(toUInt64(block_number)) AS last_access_block
FROM canonical_execution_contracts
GROUP BY contract_address;

-- ### STORAGE STATE ###

CREATE TABLE storage_state (
    address            FixedString(20),
    slot_key           FixedString(32),
    last_read_block    AggregateFunction(max, UInt64),
    last_write_block   AggregateFunction(max, UInt64),
    last_access_block  AggregateFunction(max, UInt64) -- max(last_read_block, last_write_block)
) ENGINE = AggregatingMergeTree()
PARTITION BY intDiv(any(last_access_block), 1000000)
ORDER BY (address, slot_key)
SETTINGS index_granularity = 8192;

-- For storage diffs: aggregate last_write_block and last_access_block
CREATE MATERIALIZED VIEW mv_storage_diffs_to_storage_state
TO storage_state AS
SELECT
    address,
    slot AS slot_key,
    maxState(0) AS last_read_block, -- no read operation in diffs
    maxState(block_number) AS last_write_block,
    maxState(block_number) AS last_access_block
FROM canonical_execution_storage_diffs
GROUP BY address, slot;

-- For storage reads: aggregate last_read_block and last_access_block
CREATE MATERIALIZED VIEW mv_storage_reads_to_storage_state
TO storage_state AS
SELECT
    contract_address AS address,
    slot AS slot_key,
    maxState(block_number) AS last_read_block,
    maxState(0) AS last_write_block, -- no write operation in reads
    maxState(block_number) AS last_access_block
FROM canonical_execution_storage_reads
GROUP BY contract_address, slot;

-- ### ACCOUNT ACCESS COUNT AGG ###

CREATE TABLE account_access_count_agg (
    address       FixedString(20),
    is_contract_state   AggregateFunction(argMax, UInt8, UInt64),
    read_count          AggregateFunction(count, UInt64),
    write_count         AggregateFunction(count, UInt64),
) 
ENGINE = AggregatingMergeTree()
ORDER BY (address);

CREATE TABLE storage_access_count_agg (
    address       FixedString(20),
    slot_key      FixedString(32),
    read_count          AggregateFunction(count, UInt64),
    write_count         AggregateFunction(count, UInt64),
) ENGINE = AggregatingMergeTree()
ORDER BY (address, slot_key);

CREATE TABLE accounts_block_summary (
    block_number       UInt64,
    eoa_read_count     UInt64,
    eoa_write_count    UInt64,
    contract_read_count UInt64,
    contract_write_count UInt64
) ENGINE = SummingMergeTree()
ORDER BY (block_number);

CREATE TABLE storage_block_summary (
    block_number     UInt64,
    storage_read_count       UInt64,
    storage_write_count      UInt64
) ENGINE = SummingMergeTree()
ORDER BY (block_number);

CREATE TABLE contract_storage_count_agg (
    address     FixedString(20),
    total_slots AggregateFunction(uniq, FixedString(32))
) ENGINE = AggregatingMergeTree()
ORDER BY (address);



-- Secondary Indexes
ALTER TABLE accounts_state
    ADD INDEX idx_acc_last_access   any(last_access_block) TYPE minmax   GRANULARITY 4,
    ADD INDEX idx_acc_address       address           TYPE bloom_filter GRANULARITY 4;

ALTER TABLE storage_state
    ADD INDEX idx_st_last_access    any(last_access_block) TYPE minmax   GRANULARITY 4,
    ADD INDEX idx_st_address        address           TYPE bloom_filter GRANULARITY 4;