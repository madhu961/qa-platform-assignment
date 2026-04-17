from typing import Dict


def build_reconciliation_report(
    source1_rows: int,
    source2_rows: int,
    harmonized_rows: int,
    final_rows: int
) -> Dict:
    total_input = source1_rows + source2_rows
    delivery_reliability_pct = 0.0 if total_input == 0 else round((harmonized_rows / total_input) * 100, 2)

    return {
        "source1_rows": source1_rows,
        "source2_rows": source2_rows,
        "harmonized_rows": harmonized_rows,
        "final_rows": final_rows,
        "delivery_reliability_pct": delivery_reliability_pct
    }
