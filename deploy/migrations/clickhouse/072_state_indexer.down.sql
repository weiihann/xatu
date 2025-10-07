-- ### DOWN MIGRATION for 072_state_indexer.up.sql ###
-- Drop all materialized views first (to avoid dependency errors), then tables.

DROP TABLE IF EXISTS mv_storage_reads_to_storage_last_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_storage_diffs_to_storage_last_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_contracts_to_accounts_last_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_storage_reads_to_accounts_last_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_storage_diffs_to_accounts_last_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_balance_reads_to_accounts_last_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_balance_diffs_to_accounts_last_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_nonce_diffs_to_accounts_last_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_nonce_reads_to_accounts_last_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_nonce_reads_to_accounts_first_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_nonce_diffs_to_accounts_first_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_balance_diffs_to_accounts_first_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_balance_reads_to_accounts_first_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_storage_diffs_to_accounts_first_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_storage_reads_to_accounts_first_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_contracts_to_accounts_first_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_storage_reads_to_slot_first_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS mv_storage_diffs_to_slot_first_access_local on cluster '{cluster}' SYNC;

-- Drop TABLES
DROP TABLE IF EXISTS default.accounts_first_access on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS default.accounts_first_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS default.storage_first_access on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS default.storage_first_access_local on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS default.accounts_last_access on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS default.accounts_last_access_local on cluster '{cluster}' SYNC; 
DROP TABLE IF EXISTS default.storage_last_access on cluster '{cluster}' SYNC;
DROP TABLE IF EXISTS default.storage_last_access_local on cluster '{cluster}' SYNC;