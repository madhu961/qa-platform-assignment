def build_reconciliation_report(
    source1_rows,
    source2_rows,
    final_rows,
    exact_matches,
    fuzzy_matches,
    source1_only,
    source2_only
):
    duplicates_removed = source1_rows + source2_rows - final_rows

    return {
        "source1_rows": source1_rows,
        "source2_rows": source2_rows,
        "final_rows": final_rows,
        "duplicates_removed": duplicates_removed,
        "exact_matches": exact_matches,
        "fuzzy_matches": fuzzy_matches,
        "source1_only": source1_only,
        "source2_only": source2_only,
    }
