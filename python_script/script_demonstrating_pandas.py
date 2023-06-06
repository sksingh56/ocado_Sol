from conftest import retail_data
import pandas as pd
import numpy as np


def retail_df(retail_data):
    '''
    Define pandas dataframe for the given input data records
    Parameters :
       retail_data - record for retail trainsaction  
    Return
      returns a Pandas Data Frame of retail transaction  
    '''
    df = pd.DataFrame.from_records(retail_data)
    return df


def add_column_stockout_days(data):
    '''
    Function to Add new column stockout_days.
     stockout_days : stock will be consumed in how many days from today (date)
    
    Parameter:
     pandas data frame with retail transaction

    Return:
     Pandas data frame with new column stockout_daus 
    '''

    data["stockout_days"] = data["current_stock"] / data["sales_per_day"]
    return data


def add_column_stock_arrival_day(data):
    '''
    Function to Add new column stock_arrival_days
.    
    stock_arrival_days : new stock will arrive in how many days from today(date)

    Parameter:
     pandas data frame with retail transaction

    Return:
      Pandas data frame with new column stock_arrival_days 
    '''

    data["stock_arrival_days"] = pd.to_timedelta(
        data["date_new_stock_arrival"] - data["date"]
    ).dt.days
    return data


def add_column_lifecycle_tree(data):
    '''
    Function to Add new column stock_arrival_days
.    
    function that takes in each row and returns two strings lifecycle scenario and path

    Parameter:
     pandas data frame with retail transaction

    Return:
      Pandas data frame with new column lifecycle_tree
    '''

    data["lifecycle_tree"] = np.where(
        data["dropship"] == True,
        "Scenario: Price RRP " + "scenario_path: (is item dropship ?)_YES",
        np.where(
            data["days_stock_age"] < 100,
            np.where(
                data["reorder"],
                np.where(
                    data["stock_arrival_days"] - data["stockout_days"] > 7,
                    "Scenario: Inventory Turnover Margin-%20 "
                    + "scenario_path: (is item dropship ?)_NO_(is_days_stock_age_<_100)_YES_(reorder)_YES_(stock_arrival_days-stockout_days_>7)_YES",
                    "Scenario: Margin Optimisation with Market Constraints "
                    + "scenario_path: (is item dropship ?)_NO_(is_days_stock_age_<_100)_YES_(reorder)_YES_(stock_arrival_days-stockout_days_>7)_NO",
                ),
                "Scenario: Margin Optimisation "
                + "scenario_path: (is item dropship ?)_NO_(is_days_stock_age_<_100)_NO",
            ),
            "Scenario: Markdown" + "scenario_path: (is item dropship ?)_NO",
        ),
    )
    return data


def split_lifecycle_tree_in_columns(data):
    '''
    function to data, so that there are two new columns in data called 'scenario', 'scenario_path'
    for every product SKN function that takes in each row and returns two strings lifecycle scenario and path

    Parameter:
     pandas data frame with retail transaction

    Return:
      Pandas data frame with new column lifecycle attributes scenario and scenario_path
    '''

    data[["scenario", "scenario_path"]] = data["lifecycle_tree"].str.split(
        "scenario_path:", expand=True
    )
    # remove text 'scenario' from the column as it is the column title.
    data["scenario"] = data["scenario"].str.replace(r"Scenario: ", "")
    # remove trailing spaces
    data["scenario"] = data["scenario"].str.strip()
    data["scenario_path"] = data["scenario_path"].str.strip()
    # drop column lifecyle_tree as new columns have been added
    ##data = data.drop(["lifecycle_tree"],axis=1)
    
    return data
