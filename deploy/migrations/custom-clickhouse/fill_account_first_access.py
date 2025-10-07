# -- ACCOUNT STATE -- #
def mv_nonce_reads_to_accounts_first_access_local(start, end):
    return f"""
    INSERT INTO default.accounts_first_access (address, first_access_block, version)
    SELECT
        lower(address) AS address,
        min(block_number) AS first_access_block,
        -toInt64(min(block_number)) AS version
    FROM default.canonical_execution_nonce_reads
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address
    """

def mv_nonce_diffs_to_accounts_first_access_local(start, end):
    return f"""
    INSERT INTO default.accounts_first_access (address, first_access_block, version)
    SELECT
        lower(address) as address,
        min(block_number) AS first_access_block,
        -toInt64(min(block_number)) AS version
    FROM default.canonical_execution_nonce_diffs
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address
    """

def mv_balance_diffs_to_accounts_first_access_local(start, end):
    return f"""
    INSERT INTO default.accounts_first_access (address, first_access_block, version)
    SELECT
        lower(address) as address,
        min(block_number) AS first_access_block,
        -toInt64(min(block_number)) AS version
    FROM default.canonical_execution_balance_diffs
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address
    """

def mv_balance_reads_to_accounts_first_access_local(start, end):
    return f"""
    INSERT INTO default.accounts_first_access (address, first_access_block, version)
    SELECT
        lower(address) as address,
        min(block_number) AS first_access_block,
        -toInt64(min(block_number)) AS version
    FROM default.canonical_execution_balance_reads
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address
    """

def mv_storage_diffs_to_accounts_first_access_local(start, end):
    return f"""
    INSERT INTO default.accounts_first_access (address, first_access_block, version)
    SELECT
        lower(address) as address,
        min(block_number) AS first_access_block,
        -toInt64(min(block_number)) AS version
    FROM default.canonical_execution_storage_diffs
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address
    """

def mv_storage_reads_to_accounts_first_access_local(start, end):
    return f"""
    INSERT INTO default.accounts_first_access (address, first_access_block, version)
    SELECT
        lower(contract_address) as address,
        min(block_number) AS first_access_block,
        -toInt64(min(block_number)) AS version
    FROM default.canonical_execution_storage_reads
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY contract_address
    """

def mv_contracts_to_accounts_first_access_local(start, end):
    return f"""
    INSERT INTO default.accounts_first_access (address, first_access_block, version)
    SELECT
        lower(contract_address) as address,
        min(block_number) AS first_access_block,
        -toInt64(min(block_number)) AS version
    FROM default.canonical_execution_contracts
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY contract_address
    """
