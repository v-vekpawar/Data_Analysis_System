"""This is the core engine for decision making based on predefined rules."""

def generate_decision_plan(validation_result: dict, column_profiles: dict, rules: dict):
    """Generates a decision plan based on validation results, column profiles, and rules."""

    decision_log = []

    # Dataset failed validation, nothing is allowed
    if validation_result["status"]=="FAIL":
        decision_log.append("Dataset failed validation. All downstream actions disabled.")
        return {
            "columns_to_drop": [],
            "imputation_plan": {},
            "eda_allowed": False,
            "modeling_allowed": False,
            "modeling_reason": "Dataset failed validation.",
            "risk_checks": [],
            "decision_log": decision_log
        }
    
    # Dataset passed validation, proceed with profiling and rule application
    columns_to_drop = []
    imputation_plan = {}

    col_rules = rules.get("column_cleaning", {})
    missing_rules = rules.get("missing_values", {})
    eda_rules = rules.get("eda", {})
    modeling_rules = rules.get("modeling", {})

    # Column-level decisions
    for column, profile in column_profiles.items():

        # Drop Rules
        if profile["missing_pct"] >= col_rules.get("drop_column_if_missing_pct_gt",100):
            columns_to_drop.append(column)
            decision_log.append(f"Column '{column}' dropped due to high missing percentage = ({profile['missing_pct']}%).")
            continue
    
        if col_rules.get("drop_constant_columns", False) and profile["unique_values"] <= 1:
            columns_to_drop.append(column)
            decision_log.append(f"Column '{column}' dropped due to being constant.")
            continue

        if col_rules.get("drop_id_like_columns", False) and profile["is_id_like"]:
            columns_to_drop.append(column)
            decision_log.append(f"Column '{column}' dropped due to ID-like characterstics.")
            continue

        # Imputation Decisions
        if profile["missing_pct"] > 0:
            if profile["type"]=="numeric":
                if profile["missing_pct"] <= missing_rules["numeric"]["if_missing_pct_lt"]:
                    imputation_plan[column] = missing_rules["numeric"]["strategy"]
                    decision_log.append(f"Column '{column}' will be imputed using {missing_rules['numeric']['strategy']}.")
                
                else:
                    decision_log.append(f"Column '{column}' missing percentage too high ({profile['missing_pct']}%), imputation skipped.")

            elif profile["type"]=="categorical":
                if profile["missing_pct"] <= missing_rules["categorical"]["if_missing_pct_lt"]:
                    imputation_plan[column] = missing_rules["categorical"]["strategy"]
                    decision_log.append(f"Column '{column}' will be imputed using {missing_rules['categorical']['strategy']}.")
                
                else:
                    decision_log.append(f"Column '{column}' missing percentage too high ({profile['missing_pct']}%), imputation skipped.")
        
    # EDA Decisions
    eda_allowed=True
    if validation_result["metrics"]["rows"] < eda_rules.get("skip_eda_if_rows_lt",0):
        eda_allowed=False
        decision_log.append(f"EDA Disabled due to small data size.")

    # Modeling Decisions
    modeling_allowed=True
    modeling_reason=None
    target_column=None
    target_type=None

    allow_rules = modeling_rules.get("allow_if",{})

    if validation_result["metrics"]["rows"] < allow_rules.get("min_rows",0):
        modeling_allowed=False
        modeling_reason=f"Modeling Disabled due to insufficient rows."

    if not modeling_allowed:
        decision_log.append(modeling_reason)
        
    # Target selection rule (v1)
    numeric_columns = [col for col,prof in column_profiles.items() if prof["type"]=="numeric" and col not in columns_to_drop]
    
    if modeling_allowed:
        if not numeric_columns:
            modeling_allowed=False
            modeling_reason="No numeric columns available for modeling."
            decision_log.append(modeling_reason)
        else:
            #deterministic choice: last numeric column
            target_column = numeric_columns[-1]
            target_type = "numeric"
            decision_log.append(f"Selected '{target_column}' as target column for modeling.")

    return {
        "columns_to_drop": columns_to_drop,
        "imputation_plan": imputation_plan,
        "eda_allowed": eda_allowed,
        "modeling": {
            "modeling_allowed": modeling_allowed,
            "modeling_reason": modeling_reason,
            "target_column": target_column,
            "target_type": target_type
            },
        "risk_checks": list(rules.get("risk_flags",{}).keys()),
        "decision_log": decision_log
    }
