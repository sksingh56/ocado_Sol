import pandas as pd
import numpy as np


# import functions
from scripts.pandas_df import (
    retail_df,
    add_column_stockout_days,
    add_column_stock_arrival_day,
    add_column_lifecycle_tree,
    split_lifecycle_tree_in_columns,
)


def test_pandas_data_load(retail_data):
    '''
    Unit test to check pandas data frame defined for the record

    Parameter:
     list of records with retail transaction

    Assert:
      Checks number of columns and rows in pandas dataframe
      compares values in couples in two different rows
    '''
    
    df = retail_df(retail_data)
    assert len(df) == 7
    assert len(df.columns) == 10
    assert df.iloc[0]["skn"] == "302129"
    assert df.iloc[1]["sales_per_day"] == 5


def test_pandas_check_new_cols(retail_data):
    '''
    Unit test to check addition of new columns in pandas dataframe

    Parameter:
     pandas data frame

    Assert:
      Check addition of new column
      check value in new column for couple of rows 
      Checks number of columns and rows in pandas dataframe
      compares values in new column for first row 
    '''

    df = retail_df(retail_data)
    assert df.shape == (7, 10)
    
    # Add column stockout_dyas
    df = add_column_stockout_days(df)
    assert df.shape == (7, 11)
    assert "stockout_days" in df.columns
    assert df.iloc[1]["stockout_days"] == 4
    
    # Add column stock_arrival_days
    df = add_column_stock_arrival_day(df)
    assert df.shape == (7, 12)
    assert "stock_arrival_days" in df.columns
    assert df.iloc[1]["stock_arrival_days"] == 17
    
    ## add column lifecycle_tree
    df = add_column_lifecycle_tree(df)
    assert df.shape == (7, 13)
    assert "lifecycle_tree" in df.columns
    assert (
        df.iloc[1]["lifecycle_tree"]
        == "Scenario: Price RRP scenario_path: (is item dropship ?)_YES"
    )
    
    # add columns scenario and scenario path
    df = split_lifecycle_tree_in_columns(df)
    assert df.shape == (7, 15)
    assert "scenario" in df.columns
    assert "scenario_path" in df.columns
    assert df.iloc[1]["scenario"] == "Price RRP"
    assert df.iloc[1]["scenario_path"] == "(is item dropship ?)_YES"
    