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

-- storage_state store the latest access records for each storage slot
CREATE TABLE storage_state (
    address            String,
    slot_key           String,
    last_read_block    AggregateFunction(max, UInt64),
    last_write_block   AggregateFunction(max, UInt64),
    last_access_block  AggregateFunction(max, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY (address, slot_key)

-- storage_diffs update the last_write_block and last_access_block for each storage slot
CREATE MATERIALIZED VIEW mv_storage_diffs_to_storage_state
TO storage_state AS
SELECT
    address,
    slot AS slot_key,
    maxState(toUInt64(0)) AS last_read_block,
    maxState(toUInt64(block_number)) AS last_write_block,
    maxState(toUInt64(block_number)) AS last_access_block
FROM canonical_execution_storage_diffs
GROUP BY address, slot;

-- storage_reads update the last_read_block and last_access_block for each storage slot
CREATE MATERIALIZED VIEW mv_storage_reads_to_storage_state
TO storage_state AS
SELECT
    contract_address AS address,
    slot AS slot_key,
    maxState(toUInt64(block_number)) AS last_read_block,
    maxState(toUInt64(0)) AS last_write_block,
    maxState(toUInt64(block_number)) AS last_access_block
FROM canonical_execution_storage_reads
GROUP BY contract_address, slot;

-- ### CONTRACT STORAGE COUNT AGG ###
-- contract_storage_count_agg store the total number of unique storage slots for each contract
CREATE TABLE contract_storage_count_agg (
    address     String,
    total_slots AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree()
ORDER BY (address);

CREATE MATERIALIZED VIEW mv_storage_diffs_to_contract_storage_count_agg
TO contract_storage_count_agg AS
SELECT
    address,
    uniqState(slot) AS total_slots
FROM canonical_execution_storage_diffs
GROUP BY address;

-- ### ACCOUNT ACCESS COUNT AGG ###

-- account_access_count_agg store the read and write count for each account
CREATE TABLE account_access_count_agg (
    address       String,
    is_contract   AggregateFunction(max, UInt8),
    read_count    AggregateFunction(count, UInt64),
    write_count   AggregateFunction(count, UInt64)
) 
ENGINE = AggregatingMergeTree()
ORDER BY (address);

CREATE MATERIALIZED VIEW mv_nonce_reads_to_account_access_count_agg
TO account_access_count_agg AS
SELECT
    address,
    maxState(toUInt8(false)) AS is_contract,
    countState(block_number) AS read_count
FROM canonical_execution_nonce_reads
GROUP BY address;

CREATE MATERIALIZED VIEW mv_nonce_diffs_to_account_access_count_agg
TO account_access_count_agg AS
SELECT
    address,
    maxState(toUInt8(false)) AS is_contract,
    countState(block_number) AS write_count
FROM canonical_execution_nonce_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_balance_diffs_to_account_access_count_agg
TO account_access_count_agg AS
SELECT
    address,
    maxState(toUInt8(false)) AS is_contract,
    countState(block_number) AS write_count
FROM canonical_execution_balance_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_balance_reads_to_account_access_count_agg
TO account_access_count_agg AS
SELECT
    address,
    maxState(toUInt8(false)) AS is_contract,
    countState(block_number) AS read_count
FROM canonical_execution_balance_reads
GROUP BY address;

CREATE MATERIALIZED VIEW mv_storage_diffs_to_account_access_count_agg
TO account_access_count_agg AS
SELECT
    address,
    maxState(toUInt8(true)) AS is_contract,
    countState(block_number) AS write_count
FROM canonical_execution_storage_diffs
GROUP BY address;

CREATE MATERIALIZED VIEW mv_storage_reads_to_account_access_count_agg
TO account_access_count_agg AS
SELECT
    contract_address AS address,
    maxState(toUInt8(true)) AS is_contract,
    countState(block_number) AS read_count
FROM canonical_execution_storage_reads
GROUP BY address;

CREATE MATERIALIZED VIEW mv_contracts_to_account_access_count_agg
TO account_access_count_agg AS
SELECT
    contract_address as address,
    maxState(toUInt8(true)) AS is_contract
FROM canonical_execution_contracts
GROUP BY contract_address;

-- ### STORAGE ACCESS COUNT AGG ###

CREATE TABLE storage_access_count_agg (
    address       String,
    slot_key      String,
    read_count          AggregateFunction(count, UInt64),
    write_count         AggregateFunction(count, UInt64)
) ENGINE = AggregatingMergeTree()
ORDER BY (address, slot_key);

CREATE MATERIALIZED VIEW mv_storage_diffs_to_storage_access_count_agg
TO storage_access_count_agg AS
SELECT
    address,
    slot AS slot_key,
    countState(block_number) AS write_count
FROM canonical_execution_storage_diffs
GROUP BY address, slot;

CREATE MATERIALIZED VIEW mv_storage_reads_to_storage_access_count_agg
TO storage_access_count_agg AS
SELECT
    contract_address as address,
    slot as slot_key,
    countState(block_number) AS read_count
FROM canonical_execution_storage_reads
GROUP BY address, slot;