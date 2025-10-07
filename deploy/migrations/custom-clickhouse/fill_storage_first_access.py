# -- STORAGE FIRST ACCESS -- #
def mv_storage_diffs_to_storage_first_access_local(start, end):
    return f"""
    INSERT INTO default.storage_first_access (address, slot, first_access_block, version)
    SELECT
        lower(address) as address,
        slot,
        min(block_number) AS first_access_block,
        -toInt64(min(block_number)) AS version
    FROM default.canonical_execution_storage_diffs
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address, slot
    """

def mv_storage_reads_to_storage_first_access_local(start, end):
    return f"""
    INSERT INTO default.storage_first_access (address, slot, first_access_block, version)
    SELECT
        lower(contract_address) as address,
        slot,
        min(block_number) AS first_access_block,
        -toInt64(min(block_number)) AS version
    FROM default.canonical_execution_storage_reads
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address, slot
    """
