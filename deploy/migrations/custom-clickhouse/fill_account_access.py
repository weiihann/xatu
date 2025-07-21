# -- ACCOUNT ACCESS COUNT AGG -- #
def mv_nonce_reads_to_account_access_count_sum_local(start, end):
    return f"""
    INSERT INTO default.account_access_count_sum (address, read_count, write_count)
    SELECT
        lower(address) as address,
        count() AS read_count,
        0 AS write_count
    FROM default.canonical_execution_nonce_reads
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address;
    """

def mv_nonce_diffs_to_account_access_count_sum_local(start, end):
    return f"""
    INSERT INTO default.account_access_count_sum (address, read_count, write_count)
    SELECT
        lower(address) as address,
        0 AS read_count,
        count() AS write_count
    FROM default.canonical_execution_nonce_diffs
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address;
    """

def mv_balance_diffs_to_account_access_count_sum_local(start, end):
    return f"""
    INSERT INTO default.account_access_count_sum (address, read_count, write_count)
    SELECT
        lower(address) as address,
        0 AS read_count,
        count() AS write_count
    FROM default.canonical_execution_balance_diffs
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address;
    """

def mv_balance_reads_to_account_access_count_sum_local(start, end):
    return f"""
    INSERT INTO default.account_access_count_sum (address, read_count, write_count)
    SELECT
        lower(address) as address,
        count() AS read_count,
        0 AS write_count
    FROM default.canonical_execution_balance_reads
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address;
    """

def mv_storage_diffs_to_account_access_count_sum_local(start, end):
    return f"""
    INSERT INTO default.account_access_count_sum (address, read_count, write_count)
    SELECT
        lower(address) as address,
        0 AS read_count,
        count() AS write_count
    FROM default.canonical_execution_storage_diffs
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address;
    """

def mv_storage_reads_to_account_access_count_sum_local(start, end):
    return f"""
    INSERT INTO default.account_access_count_sum (address, read_count, write_count)
    SELECT
        lower(contract_address) as address,
        count() AS read_count,
        0 AS write_count
    FROM default.canonical_execution_storage_reads
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address;
    """