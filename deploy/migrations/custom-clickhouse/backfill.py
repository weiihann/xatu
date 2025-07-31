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
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()  # Loads variables from .env into the environment

username = os.getenv('XATU_CLICKHOUSE_USERNAME')
password = os.getenv('XATU_CLICKHOUSE_PASSWORD')
url = os.getenv('XATU_CLICKHOUSE_URL')
protocol = os.getenv('XATU_CLICKHOUSE_PROTOCOL')

db_url = f"clickhouse+http://{username}:{password}@{url}/default?protocol={protocol}"
engine = create_engine(db_url)

account_last_access_sql = [
    mv_nonce_reads_to_accounts_last_access_local,
    mv_nonce_diffs_to_accounts_last_access_local,
    mv_balance_diffs_to_accounts_last_access_local,
    mv_balance_reads_to_accounts_last_access_local,
    mv_storage_diffs_to_accounts_last_access_local,
    mv_storage_reads_to_accounts_last_access_local,
    mv_contracts_to_accounts_last_access_local,
]
storage_last_access_sql = [
    mv_storage_diffs_to_storage_last_access_local,
    mv_storage_reads_to_storage_last_access_local,
]
account_first_access_sql = [
    mv_nonce_reads_to_accounts_first_access_local,
    mv_nonce_diffs_to_accounts_first_access_local,
    mv_balance_diffs_to_accounts_first_access_local,
    mv_balance_reads_to_accounts_first_access_local,
    mv_storage_diffs_to_accounts_first_access_local,
    mv_storage_reads_to_accounts_first_access_local,
    mv_contracts_to_accounts_first_access_local,
]
storage_first_access_sql = [
    mv_storage_diffs_to_storage_first_access_local,
    mv_storage_reads_to_storage_first_access_local,
]

fills = [
    # account_last_access_sql,
    # storage_last_access_sql,
    # account_first_access_sql,
    storage_first_access_sql,
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