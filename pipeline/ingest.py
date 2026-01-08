"""This module is responsible for ingesting dataset and rules"""

import os
import pandas as pd
import yaml

# Custom Exception
class IngestError(Exception):
    pass

# Load Dataset Function
def load_dataset(dataset_path: str):
    """Load dataset from a CSV or Excel file"""

    if not os.path.exists(dataset_path):
        raise IngestError(f"Dataset not found at {dataset_path}")
    
    if dataset_path.endswith(".csv"):
        return pd.read_csv(dataset_path)
    
    if dataset_path.endswith((".xls","xlsx")):
        return pd.read_excel(dataset_path)

    raise IngestError("Unsupported file format. Only CSV and Excel files are supported") 

def load_rules(rules_path: str):
    """Load rules from a YAML file"""

    if not os.path.exists(rules_path):
        raise IngestError(f"Rules file not found at: {rules_path}")
    
    with open(rules_path, "r") as f:
        return yaml.safe_load(f)
    
def load_inputs(dataset_path: str, rules_path: str):
    """Entry point for data and rules ingestion."""

    data = load_dataset(dataset_path)
    rules = load_rules(rules_path)
    
    return {
        "data":data,
        "rules":rules
    }
    
