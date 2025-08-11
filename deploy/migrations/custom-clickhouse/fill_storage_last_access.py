# -- STORAGE STATE -- #
def mv_storage_diffs_to_storage_last_access_local(start, end):
    return f"""
    INSERT INTO default.storage_last_access (address, slot_key, last_access_block, is_deleted, version)
    SELECT
        lower(address) as address,
        slot AS slot_key,
        max(block_number) AS last_access_block,
        argMax(to_value, (block_number, transaction_index, internal_index)) = '0x0000000000000000000000000000000000000000000000000000000000000000' AS is_deleted,
        (bitShiftLeft(toUInt128(max(block_number)), 64) + bitShiftLeft(toUInt128(argMax(transaction_index, (block_number, transaction_index))), 32) + toUInt128(argMax(internal_index, (block_number, transaction_index, internal_index)))) AS version
    FROM default.canonical_execution_storage_diffs
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address, slot
    """

def mv_storage_reads_to_storage_last_access_local(start, end):
    return f"""
    INSERT INTO default.storage_last_access (address, slot_key, last_access_block, is_deleted, version)
    SELECT
        lower(contract_address) as address,
        slot AS slot_key,
        max(block_number) AS last_access_block,
        argMax(value, (block_number, transaction_index, internal_index)) = '0x0000000000000000000000000000000000000000000000000000000000000000' AS is_deleted,
        (bitShiftLeft(toUInt128(max(block_number)), 64)) AS version
    FROM default.canonical_execution_storage_reads
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address, slot
    """