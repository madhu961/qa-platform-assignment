from src.reconcile import build_reconciliation_report


def test_reconciliation_report():
    source1_rows=1200
    source2_rows=1200
    final_rows=1611
    exact_matches=900
    fuzzy_matches=320
    source1_only=211
    source2_only=180
    report = build_reconciliation_report(source1_rows, source2_rows, final_rows, exact_matches, fuzzy_matches, source1_only, source2_only)

    assert report["source1_rows"] == 1200
    assert report["source2_rows"] == 1200
    assert report["final_rows"] == 1611
    assert report["duplicates_removed"] == 789
    assert report["exact_matches"] == 900
    assert report["fuzzy_matches"] == 320
    assert report["source1_only"] == 211
    assert report["source2_only"] == 180
    assert report["match_rate_pct"] == round(((900 + 320) / 1611) * 100, 2)
