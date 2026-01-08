"""This module is responsible for cleaning the dataset based on decision plan."""
import pandas as pd

def execute_cleaning(df: pd.DataFrame, decision_plan: dict):
    """Clean the dataset based on decision plan"""
    cleaned_df = df.copy()
    cleaning_log = []

    # Drop Columns
    drop_columns = decision_plan.get("columns_to_drop", [])
    for column in drop_columns:
        if column in cleaned_df.columns:
            cleaned_df.drop(columns=[column], inplace=True)
            cleaning_log.append(f"Dropped column '{column}' as per decision plan.")

    # Imputation
    imputation_plan = decision_plan.get("imputation_plan", {})
    for column, strategy in imputation_plan.items():

        if column not in cleaned_df.columns:
            continue

        if strategy == "median":
            median_value = cleaned_df[column].median()
            cleaned_df[column]= cleaned_df[column].fillna(median_value)
            cleaning_log.append(f"Imputed missing values in column '{column}' with median value {median_value}.")

        elif strategy == "mean":
            mean_value = cleaned_df[column].mean()
            cleaned_df[column]= cleaned_df[column].fillna(mean_value)
            cleaning_log.append(f"Imputed missing values in column '{column}' with mean value {mean_value}.")

        elif strategy == "mode":
            mode_value = cleaned_df[column].mode()[0]
            cleaned_df[column]= cleaned_df[column].fillna(mode_value)
            cleaning_log.append(f"Imputed missing values in column '{column}' with mode value {mode_value}.")

    return {
        "cleaned_data": cleaned_df,
        "cleaning_log": cleaning_log
    }
