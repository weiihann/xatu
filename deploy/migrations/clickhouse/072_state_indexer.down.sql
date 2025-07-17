-- ### DOWN MIGRATION for 072_state_indexer.up.sql ###
-- Drop all materialized views first (to avoid dependency errors), then tables.

-- Drop MATERIALIZED VIEWS (in reverse order of creation)
DROP TABLE IF EXISTS mv_storage_reads_to_storage_access_count_agg;
DROP TABLE IF EXISTS mv_storage_diffs_to_storage_access_count_agg;
DROP TABLE IF EXISTS mv_contracts_to_account_access_count_agg;
DROP TABLE IF EXISTS mv_storage_reads_to_account_access_count_agg;
DROP TABLE IF EXISTS mv_storage_diffs_to_account_access_count_agg;
DROP TABLE IF EXISTS mv_balance_reads_to_account_access_count_agg;
DROP TABLE IF EXISTS mv_balance_diffs_to_account_access_count_agg;
DROP TABLE IF EXISTS mv_nonce_diffs_to_account_access_count_agg;
DROP TABLE IF EXISTS mv_nonce_reads_to_account_access_count_agg;
DROP TABLE IF EXISTS mv_storage_diffs_to_contract_storage_count_agg;
DROP TABLE IF EXISTS mv_storage_reads_to_storage_state;
DROP TABLE IF EXISTS mv_storage_diffs_to_storage_state;
DROP TABLE IF EXISTS mv_contracts_to_accounts_state;
DROP TABLE IF EXISTS mv_storage_reads_to_accounts_state;
DROP TABLE IF EXISTS mv_storage_diffs_to_accounts_state;
DROP TABLE IF EXISTS mv_balance_reads_to_accounts_state;
DROP TABLE IF EXISTS mv_balance_diffs_to_accounts_state;
DROP TABLE IF EXISTS mv_nonce_diffs_to_accounts_state;
DROP TABLE IF EXISTS mv_nonce_reads_to_accounts_state;

-- Drop TABLES
DROP TABLE IF EXISTS storage_access_count_agg;
DROP TABLE IF EXISTS account_access_count_agg;
DROP TABLE IF EXISTS contract_storage_count_agg;
DROP TABLE IF EXISTS storage_state;
DROP TABLE IF EXISTS accounts_state; 