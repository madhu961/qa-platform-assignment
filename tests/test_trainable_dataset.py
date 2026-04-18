import pandas as pd


def test_training_parquet_columns():
    df = pd.DataFrame({
        "revenue": [100.0, 200.0],
        "profit": [20.0, 50.0],
        "num_top_suppliers": [1, 2],
        "num_main_customers": [2, 1],
        "num_activity_places": [3, 2],
        "label": [0, 1],
    })

    expected = {
        "revenue",
        "profit",
        "num_top_suppliers",
        "num_main_customers",
        "num_activity_places",
        "label",
    }

    assert expected.issubset(set(df.columns))
