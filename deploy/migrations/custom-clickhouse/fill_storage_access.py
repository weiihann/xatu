# -- STORAGE ACCESS COUNT AGG -- #
def mv_storage_diffs_to_sum_local(start, end):
    return f"""
    INSERT INTO default.storage_access_count_sum (address, slot_key, read_count, write_count)
    SELECT
        lower(address) as address,
        slot AS slot_key,
        0 AS read_count,
        count() AS write_count
    FROM default.canonical_execution_storage_diffs
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address, slot;
    """

def mv_storage_reads_to_sum_local(start, end):
    return f"""
    INSERT INTO default.storage_access_count_sum (address, slot_key, read_count, write_count)
    SELECT
        lower(contract_address) as address,
        slot as slot_key,
        count() AS read_count,
        0 AS write_count
    FROM default.canonical_execution_storage_reads
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address, slot;
    """