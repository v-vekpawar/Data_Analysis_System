"""This module is responsible for profiling dataset and rules"""
import pandas as pd
import numpy as np 
import re  # regular expression library (regex)

ID_PATTERN = re.compile(r"(id|uuid|index|sno|s.no|s.no.)",re.IGNORECASE) # pattern to match column header which are ids

def is_id_like(column_name: str, series: pd.Series, total_rows:int):
    """Checks if a column is an id column"""

    if ID_PATTERN.search(column_name):
        return True # Column name matches patter -> than it is id column
    
    if total_rows == 0:
        return False # If no rows -> it can't be id column
    
    unique_ratio = series.nunique() / total_rows
    return unique_ratio > 0.95 # If unique ratio is high -> than it is id column

def outlier_percentage(series: pd.Series):
    """Calculate percentage of outliers in a column"""
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1

    if iqr == 0:
        return 0.0
    
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    outliers = series[(series<lower_bound) | (series>upper_bound)]

    return round((len(outliers)/len(series))*100,2)

def profile_columns(df: pd.DataFrame):
    """Profile columns in a dataset"""
    
    profiles={}
    total_rows = len(df)

    for column in df.columns:
        series = df[column]
        missing_pct = round(series.isnull().mean()*100,2)
        unique_values = series.nunique(dropna=True)

        if pd.api.types.is_numeric_dtype(series):
            column_type = "numeric"
            variance = float(series.var()) if series.var() is not None else None
            outlier_pct = outlier_percentage(series)
        else:
            column_type = "categorical"
            variance = None
            outlier_pct = None

        profiles[column] = {
            "type":column_type,
            "missing_pct":missing_pct,
            "unique_values":unique_values,
            "variance":variance,
            "outlier_pct":outlier_pct,
            "is_id_like":is_id_like(column,series,total_rows)
        }

    return profiles