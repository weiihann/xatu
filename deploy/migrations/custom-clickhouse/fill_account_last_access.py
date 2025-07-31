# -- ACCOUNT STATE -- #
def mv_nonce_reads_to_accounts_last_access_local(start, end):
    return f"""
    INSERT INTO default.accounts_last_access (address, last_access_block)
    SELECT
        lower(address) AS address,
        max(block_number) AS last_access_block
    FROM default.canonical_execution_nonce_reads
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address
    """

def mv_nonce_diffs_to_accounts_last_access_local(start, end):
    return f"""
    INSERT INTO default.accounts_last_access (address, last_access_block)
    SELECT

    
        lower(address) as address,
        max(block_number) AS last_access_block
    FROM default.canonical_execution_nonce_diffs
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address
    """

def mv_balance_diffs_to_accounts_last_access_local(start, end):
    return f"""
    INSERT INTO default.accounts_last_access (address, last_access_block)
    SELECT
        lower(address) as address,
        max(block_number) AS last_access_block
    FROM default.canonical_execution_balance_diffs
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address
    """

def mv_balance_reads_to_accounts_last_access_local(start, end):
    return f"""
    INSERT INTO default.accounts_last_access (address, last_access_block)
    SELECT
        lower(address) as address,
        max(block_number) AS last_access_block
    FROM default.canonical_execution_balance_reads
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address
    """

def mv_storage_diffs_to_accounts_last_access_local(start, end):
    return f"""
    INSERT INTO default.accounts_last_access (address, last_access_block)
    SELECT
        lower(address) as address,
        max(block_number) AS last_access_block
    FROM default.canonical_execution_storage_diffs
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address
    """

def mv_storage_reads_to_accounts_last_access_local(start, end):
    return f"""
    INSERT INTO default.accounts_last_access (address, last_access_block)
    SELECT
        lower(contract_address) as address,
        max(block_number) AS last_access_block
    FROM default.canonical_execution_storage_reads
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY contract_address
    """

def mv_contracts_to_accounts_last_access_local(start, end):
    return f"""
    INSERT INTO default.accounts_last_access (address, last_access_block)
    SELECT
        lower(contract_address) as address,
        max(block_number) AS last_access_block
    FROM default.canonical_execution_contracts
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY contract_address
    """
