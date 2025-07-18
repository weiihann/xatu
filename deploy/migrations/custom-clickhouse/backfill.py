from fill_account_state import (
    mv_nonce_reads_to_accounts_state_local,
    mv_nonce_diffs_to_accounts_state_local,
    mv_balance_diffs_to_accounts_state_local,
    mv_balance_reads_to_accounts_state_local,
    mv_storage_diffs_to_accounts_state_local,
    mv_storage_reads_to_accounts_state_local,
    mv_contracts_to_accounts_state_local,
)
from fill_storage_state import (
    mv_storage_diffs_to_storage_state_local,
    mv_storage_reads_to_storage_state_local,
)
from fill_contract_storage_count import (
    mv_storage_diffs_to_contract_storage_count_agg_local,
)

import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()  # Loads variables from .env into the environment

username = os.getenv('XATU_CLICKHOUSE_USERNAME')
password = os.getenv('XATU_CLICKHOUSE_PASSWORD')
url = os.getenv('XATU_CLICKHOUSE_URL')
protocol = os.getenv('XATU_CLICKHOUSE_PROTOCOL')

db_url = f"clickhouse+http://{username}:{password}@{url}/default?protocol={protocol}"
engine = create_engine(db_url)

account_state_sql = [
    mv_nonce_reads_to_accounts_state_local,
    mv_nonce_diffs_to_accounts_state_local,
    mv_balance_diffs_to_accounts_state_local,
    mv_balance_reads_to_accounts_state_local,
    mv_storage_diffs_to_accounts_state_local,
    mv_storage_reads_to_accounts_state_local,
    mv_contracts_to_accounts_state_local,
]

storage_state_sql = [
    mv_storage_diffs_to_storage_state_local,
    mv_storage_reads_to_storage_state_local,
]

contract_storage_count_agg_sql = [
    mv_storage_diffs_to_contract_storage_count_agg_local,
]

fills = [
    # account_state_sql,
    # storage_state_sql,
    contract_storage_count_agg_sql,
]

def main():
    start_block = 0
    end_block = 22913000
    step = 100000

    global_start_time = time.time()
    for lower in range(start_block, end_block, step):
        start_time = time.time()
        upper = lower + step - 1

        with engine.begin() as conn:
            for fill in fills:
                for sql in fill:
                    conn.execute(text(sql(lower, upper)))

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Done {lower} - {upper} | Time: {elapsed_time:.2f}s")

    global_end_time = time.time()
    global_elapsed_time = global_end_time - global_start_time
    print(f"Total time: {global_elapsed_time:.2f}s")

if __name__ == "__main__":
    main()