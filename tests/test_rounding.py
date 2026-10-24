import pandas as pd

from src.batch_processing import round_half_up


def test_round_half_up_differs_from_bankers_rounding():
    values = pd.Series([0.125, 2.5, 1.005, 3.14159, None])
    assert round_half_up(values, 2).tolist()[:4] == [0.13, 2.5, 1.01, 3.14]
    assert pd.isna(round_half_up(values, 2).iloc[4])
    assert round_half_up(pd.Series([0.25, 0.35]), 1).tolist() == [0.3, 0.4]  # pandas.round would give 0.2, 0.3
