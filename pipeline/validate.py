"""This module is responsible for validating dataset and rules"""
import pandas as pd

# Custom error for this module
class ValidationError(Exception):
    pass

def validate_dataset(df:pd.DataFrame, rules: dict):
    """Validate dataset against rules"""
    validation_rules = rules.get("dataset_validation")

    # Calculate metrics from dataset
    metrics = {
        "rows": df.shape[0],
        "columns": df.shape[1],
        "overall_missing_pct":round(df.isnull().mean().mean()*100,2),
        "numeric_columns":df.select_dtypes(include='number').shape[1]
    }

    reasons=[] # List to store reasons for validation failure

    # Check for minimum number of rows
    if metrics["rows"] < validation_rules["min_rows"]:
        reasons.append(f"Dataset must have at least {validation_rules['min_rows']} rows, but found {metrics['rows']} rows")

    # Check for minimum number of columns
    if metrics["columns"] < validation_rules["min_columns"]:
        reasons.append(f"Dataset must have at least {validation_rules['min_columns']} columns, but found {metrics['columns']} columns")

    # Check for Overall missing percentage
    if metrics["overall_missing_pct"] > validation_rules["max_missing_overall_pct"]:
        reasons.append(f"Dataset cannot have more than {validation_rules['max_missing_overall_pct']} missing percentage, but found {metrics['overall_missing_pct']} missing percentage")

    # Check for minimum number of numeric columns
    required_numeric = validation_rules.get("required_column_types", {}).get("numeric", 0)
    if metrics["numeric_columns"] < required_numeric:
        reasons.append(f"Dataset must have at least {required_numeric} numeric columns, but found {metrics['numeric_columns']} numeric columns")

    status = "PASS" if not reasons else "FAIL"
    return {
        "status":status,
        "reasons":reasons,
        "metrics":metrics
    }
