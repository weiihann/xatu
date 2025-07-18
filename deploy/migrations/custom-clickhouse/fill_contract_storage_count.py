# -- CONTRACT STORAGE COUNT AGG -- #
def mv_storage_diffs_to_contract_storage_count_agg_local(start, end):
    return f"""
    INSERT INTO default.contract_storage_count_agg (address, total_slots)
    SELECT
        lower(address) as address,
        uniqState(slot) AS total_slots
    FROM default.canonical_execution_storage_diffs
    WHERE block_number BETWEEN {start} AND {end}
    GROUP BY address;
    """