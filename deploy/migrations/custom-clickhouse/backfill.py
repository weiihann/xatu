from fill_account_last_access import (
    mv_nonce_reads_to_accounts_last_access_local,
    mv_nonce_diffs_to_accounts_last_access_local,
    mv_balance_diffs_to_accounts_last_access_local,
    mv_balance_reads_to_accounts_last_access_local,
    mv_storage_diffs_to_accounts_last_access_local,
    mv_storage_reads_to_accounts_last_access_local,
    mv_contracts_to_accounts_last_access_local,
)
from fill_storage_last_access import (
    mv_storage_diffs_to_storage_last_access_local,
    mv_storage_reads_to_storage_last_access_local,
)
from fill_account_first_access import (
    mv_nonce_reads_to_accounts_first_access_local,
    mv_nonce_diffs_to_accounts_first_access_local,
    mv_balance_diffs_to_accounts_first_access_local,
    mv_balance_reads_to_accounts_first_access_local,
    mv_storage_diffs_to_accounts_first_access_local,
    mv_storage_reads_to_accounts_first_access_local,
    mv_contracts_to_accounts_first_access_local,
)
from fill_storage_first_access import (
    mv_storage_diffs_to_storage_first_access_local,
    mv_storage_reads_to_storage_first_access_local,
)

import os
import time
import re
from typing import List, Callable, Dict, Tuple
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()  # Loads variables from .env into the environment

username = os.getenv('XATU_CLICKHOUSE_USERNAME')
password = os.getenv('XATU_CLICKHOUSE_PASSWORD')
url = os.getenv('XATU_CLICKHOUSE_URL')
protocol = os.getenv('XATU_CLICKHOUSE_PROTOCOL')

db_url = f"clickhouse+http://{username}:{password}@{url}/default?protocol={protocol}"
engine = create_engine(db_url)

# Define fill configurations with their functions
FILL_CONFIGURATIONS = {
    'account_last_access': [
        mv_nonce_reads_to_accounts_last_access_local,
        mv_nonce_diffs_to_accounts_last_access_local,
        mv_balance_diffs_to_accounts_last_access_local,
        mv_balance_reads_to_accounts_last_access_local,
        mv_storage_diffs_to_accounts_last_access_local,
        mv_storage_reads_to_accounts_last_access_local,
        mv_contracts_to_accounts_last_access_local,
    ],
    'storage_last_access': [
        mv_storage_diffs_to_storage_last_access_local,
        mv_storage_reads_to_storage_last_access_local,
    ],
    'account_first_access': [
        mv_nonce_reads_to_accounts_first_access_local,
        mv_nonce_diffs_to_accounts_first_access_local,
        mv_balance_diffs_to_accounts_first_access_local,
        mv_balance_reads_to_accounts_first_access_local,
        mv_storage_diffs_to_accounts_first_access_local,
        mv_storage_reads_to_accounts_first_access_local,
        mv_contracts_to_accounts_first_access_local,
    ],
    'storage_first_access': [
        mv_storage_diffs_to_storage_first_access_local,
        mv_storage_reads_to_storage_first_access_local,
    ]
}

def extract_source_table_from_sql(sql: str) -> str:
    """Extract the source table name from a SQL query."""
    # Pattern to match "FROM default.table_name" or "FROM table_name"
    pattern = r'FROM\s+(?:default\.)?(\w+)'
    match = re.search(pattern, sql, re.IGNORECASE)
    if match:
        return match.group(1)
    raise ValueError(f"Could not extract source table from SQL: {sql}")

def get_source_table_for_function(func: Callable) -> str:
    """Get the source table name for a given function by examining its SQL."""
    # Generate a sample SQL to extract the table name
    sample_sql = func(0, 1)
    return extract_source_table_from_sql(sample_sql)

def get_max_block_number(table_name: str) -> int:
    """Get the maximum block number from a given table."""
    with engine.begin() as conn:
        result = conn.execute(text(f"SELECT MAX(block_number) FROM default.{table_name}"))
        max_block = result.scalar()
        return max_block if max_block is not None else 0

def build_table_max_blocks_cache(functions: List[Callable], end_block: int = None) -> Dict[str, int]:
    """Build a cache of max block numbers for all source tables used by the functions."""
    table_cache = {}
    
    for func in functions:
        try:
            table_name = get_source_table_for_function(func)
            if table_name not in table_cache:
                max_block = get_max_block_number(table_name)
                # Use end_block if specified and max_block exceeds it
                if end_block is not None and max_block > end_block:
                    max_block = end_block
                    print(f"Table {table_name}: max block = {max_block} (limited by end_block)")
                else:
                    print(f"Table {table_name}: max block = {max_block}")
                table_cache[table_name] = max_block
        except Exception as e:
            print(f"Warning: Could not get source table for function {func.__name__}: {e}")
            continue
    
    return table_cache

def execute_function_with_table_blocks(func: Callable, table_cache: Dict[str, int], start_block: int, step: int) -> int:
    """Execute a function for all blocks in its source table, return number of blocks processed."""
    try:
        table_name = get_source_table_for_function(func)
        max_block = table_cache.get(table_name, 0)
        
        if max_block <= start_block:
            print(f"  [{func.__name__}] No blocks to process (max_block: {max_block}, start_block: {start_block})")
            return 0
        
        total_blocks = max_block - start_block + 1
        blocks_processed = 0
        
        print(f"  [{func.__name__}] Processing {total_blocks} blocks from table {table_name}")
        
        with engine.begin() as conn:
            for lower in range(start_block, max_block + 1, step):
                upper = min(lower + step - 1, max_block)
                sql = func(lower, upper)
                conn.execute(text(sql))
                blocks_processed += (upper - lower + 1)
                
                if lower % (step * 10) == 0:  # Progress every 10 steps
                    progress = (blocks_processed / total_blocks) * 100
                    print(f"    Progress: {progress:.1f}% (blocks {lower}-{upper})")
        
        print(f"  [{func.__name__}] Completed {blocks_processed} blocks")
        return blocks_processed
        
    except Exception as e:
        print(f"Error executing function {func.__name__}: {e}")
        raise

def backfill_configuration(config_name: str, functions: List[Callable], step: int = 100000, start_block: int = 0, end_block: int = None) -> None:
    """Backfill a specific configuration."""
    print(f"\n=== Starting backfill for {config_name} ===")
    
    # Build cache of max blocks for all source tables
    table_cache = build_table_max_blocks_cache(functions, end_block)
    
    if not table_cache:
        print(f"No tables found for {config_name}, skipping...")
        return
    
    config_start_time = time.time()
    total_blocks_processed = 0
    
    for func in functions:
        func_start_time = time.time()
        
        try:
            blocks_processed = execute_function_with_table_blocks(func, table_cache, start_block, step)
            total_blocks_processed += blocks_processed
            
            func_end_time = time.time()
            func_elapsed_time = func_end_time - func_start_time
            print(f"  [{func.__name__}] Completed in {func_elapsed_time:.2f}s")
            
        except Exception as e:
            print(f"Error in function {func.__name__}: {e}")
            raise
    
    config_end_time = time.time()
    config_elapsed_time = config_end_time - config_start_time
    print(f"=== Completed {config_name} in {config_elapsed_time:.2f}s (processed {total_blocks_processed} total blocks) ===")

def main():
    """Main backfill function."""
    # Configuration: which fills to run
    active_fills = [
        # 'account_last_access',
        'storage_last_access', 
        # 'account_first_access',
        # 'storage_first_access',
    ]
    
    step_size = 100000
    start_block = 0

    # This marks the end block for all insert operations
    # Uncomment this if you want to use the default max block in each source table
    end_block = 22431083
    
    print("=== Xatu State Indexer Backfill ===")
    print(f"Step size: {step_size}")
    print(f"Start block: {start_block}")
    print(f"Active fills: {active_fills}")
    
    global_start_time = time.time()
    
    for fill_name in active_fills:
        if fill_name not in FILL_CONFIGURATIONS:
            print(f"Warning: Unknown fill configuration '{fill_name}', skipping...")
            continue
            
        try:
            backfill_configuration(
                fill_name, 
                FILL_CONFIGURATIONS[fill_name], 
                step_size, 
                start_block,
                end_block
            )
        except Exception as e:
            print(f"Fatal error in {fill_name}: {e}")
            raise
    
    global_end_time = time.time()
    global_elapsed_time = global_end_time - global_start_time
    print(f"\n=== Total backfill time: {global_elapsed_time:.2f}s ===")

if __name__ == "__main__":
    main()