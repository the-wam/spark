import pytest
import pandas as pd

from src.utilities.manipulate_data import import_csv, write_parquet, read_parquet, to_parquet

@pytest.mark.utilities
def test_import_csv():
    dataframe = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})

    dataframe.to_csv("./df_test.csv")

    