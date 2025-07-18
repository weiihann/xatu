# -- STORAGE STATE -- #
def mv_storage_diffs_to_storage_state_local(start, end):
    return f"""
    INSERT INTO default.storage_state (address, slot_key, last_access_block)
    SELECT
        lower(address) as address,
        slot AS slot_key,
        max(block_number) AS last_access_block
    FROM default.canonical_execution_storage_diffs
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address, slot
    """

def mv_storage_reads_to_storage_state_local(start, end):
    return f"""
    INSERT INTO default.storage_state (address, slot_key, last_access_block)
    SELECT
        lower(contract_address) as address,
        slot AS slot_key,
        max(block_number) AS last_access_block
    FROM default.canonical_execution_storage_reads
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address, slot
    """
