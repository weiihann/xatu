### Information
`accounts_state`
- Time Taken: ~13 minutes
```
   ┌─table────────────────┬─uncompressed─┬─compressed─┬─total_rows─┐
1. │ accounts_state_local │ 15.51 GiB    │ 12.94 GiB  │  326459123 │
   └──────────────────────┴──────────────┴────────────┴────────────┘
   ┌─table────────────────┬─uncompressed─┬─compressed─┬─total_rows─┐
1. │ accounts_state_local │ 15.48 GiB    │ 12.92 GiB  │  325952640 │
   └──────────────────────┴──────────────┴────────────┴────────────┘
```
- Size:
  - Uncompressed: ~31GB
  - Compressed: ~26GB
  - Total Rows: ~652 Million

`storage_state`
- Time Taken: ~35 minutes
```
   ┌─table───────────────┬─uncompressed─┬─compressed─┬─total_rows─┐
1. │ storage_state_local │ 111.76 GiB   │ 53.47 GiB  │ 1016974651 │
   └─────────────────────┴──────────────┴────────────┴────────────┘
   ┌─table───────────────┬─uncompressed─┬─compressed─┬─total_rows─┐
1. │ storage_state_local │ 111.65 GiB   │ 53.38 GiB  │ 1015917543 │
   └─────────────────────┴──────────────┴────────────┴────────────┘
```
- Size:
  - Uncompressed: ~223GB
  - Compressed: ~107GB
  - Total Rows: ~2 Billion

`contract_storage_count_agg`
- Time Taken: ~5 minutes
```
   ┌─table────────────────────────────┬─uncompressed─┬─compressed─┬─total_rows─┐
1. │ contract_storage_count_agg_local │ 2.27 GiB     │ 2.15 GiB   │   12863393 │
   └──────────────────────────────────┴──────────────┴────────────┴────────────┘
   ┌─table────────────────────────────┬─uncompressed─┬─compressed─┬─total_rows─┐
1. │ contract_storage_count_agg_local │ 2.29 GiB     │ 2.17 GiB   │   12899401 │
   └──────────────────────────────────┴──────────────┴────────────┴────────────┘
```
- Size:
  - Uncompressed: ~2.27GB
  - Compressed: ~2.29GB
  - Total Rows: ~25M

`account_access_count_sum`
- Time Taken: ~33 minutes
```
   ┌─table──────────────────────────┬─uncompressed─┬─compressed─┬─total_rows─┐
1. │ account_access_count_sum_local │ 11.60 GiB    │ 8.41 GiB   │  211340982 │
   └────────────────────────────────┴──────────────┴────────────┴────────────┘
   ┌─table──────────────────────────┬─uncompressed─┬─compressed─┬─total_rows─┐
1. │ account_access_count_sum_local │ 11.95 GiB    │ 8.69 GiB   │  218347889 │
   └────────────────────────────────┴──────────────┴────────────┴────────────┘
```
- Size:
  - Uncompressed: ~23.55GB
  - Compressed: ~17.1GB
  - Total Rows: ~430 Million

`storage_access_count_sum`
- Time Taken: ~41 minutes
```
   ┌─table──────────────────────────┬─uncompressed─┬─compressed─┬─total_rows─┐
1. │ storage_access_count_sum_local │ 101.16 GiB   │ 45.85 GiB  │  920490344 │
   └────────────────────────────────┴──────────────┴────────────┴────────────┘
   ┌─table──────────────────────────┬─uncompressed─┬─compressed─┬─total_rows─┐
1. │ storage_access_count_sum_local │ 101.21 GiB   │ 45.79 GiB  │  920910899 │
   └────────────────────────────────┴──────────────┴────────────┴────────────┘
```
- Size:
  - Uncompressed: ~202.4GB
  - Compressed: ~91.6GB
  - Total Rows: ~1.8 Billion