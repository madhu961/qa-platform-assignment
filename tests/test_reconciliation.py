from src.reconcile import build_reconciliation_report


def test_reconciliation_report():
    report = build_reconciliation_report(1000, 1000, 1800, 1800)
    assert report["source1_rows"] == 1000
    assert report["delivery_reliability_pct"] == 90.0
