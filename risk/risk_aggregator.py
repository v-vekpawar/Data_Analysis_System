"""It aggregates risks from upstream outputs and reports them pessimistically."""
def aggregate_risk(validation_result: dict, column_profiles: dict, decision_plan: dict, cleaning_log: dict, eda_result: dict, model_result: dict, rules: dict):
    """Aggregate risks from various stages of the pipeline"""
    
    data_quality_risks = []
    analysis_risks = []
    modeling_risks = []
    business_risks = []

    risk_rules = rules.get("risk_flags",{})

    # Data Quality Risks
    for column,profile in column_profiles.items():
        if profile["missing_pct"] >= risk_rules.get("data_quality",{}).get("high_missing_pct",100):
            data_quality_risks.append(f"Column '{column}' has high missing percentage: {profile['missing_pct']}%")

        if profile["outlier_pct"] is not None and profile["outlier_pct"] >= risk_rules.get("data_quality",{}).get("high_outlier_pct",100):
            data_quality_risks.append(f"Column '{column}' has high outlier percentage: {profile['outlier_pct']}%")
    
    if len(decision_plan.get("columns_to_drop",[])) > 0:
        data_quality_risks.append(f"{decision_plan['columns_to_drop']} Columns were dropped during cleaning")

    # Cleaning behavior risks (part of data quality)
    drop_events = [log for log in cleaning_log if "Dropped column" in log]

    if len(drop_events) >= 3:
        data_quality_risks.append(f"{len(drop_events)} columns were dropped during cleaning, indicating weak data quality.")

    imputation_events = [log for log in cleaning_log if "Imputed missing values" in log]

    if len(imputation_events) >= 2:
        data_quality_risks.append("Multiple columns were imputed; results may rely heavily on synthetic values.")


    # Analysis Risks
    if not decision_plan.get("eda_allowed",False):
        analysis_risks.append("EDA was skipped due to dataset constraints.")
    
    if validation_result["metrics"]["rows"] < 200:
        analysis_risks.append(f"Dataset contains only {validation_result['metrics']['rows']} rows; statistical insights may be unstable.")

    # EDA Behavioral risks (part of analysis risks)
    eda_logs = eda_result.get("eda_log", [])
    eda_metrics = eda_result.get("metrics", {})

    # Case 1: EDA allowed but produced nothing
    if decision_plan.get("eda_allowed", False) and not eda_metrics:
        analysis_risks.append("EDA was allowed but produced no measurable insights.")

    # Case 2: EDA explicitly skipped during execution
    if any("EDA skipped" in log for log in eda_logs):
        analysis_risks.append("EDA was skipped during execution; analytical visibility is limited.")

    # Case 3: No numeric relationships found
    has_numeric_analysis = any("correlation" in key.lower() for key in eda_metrics.keys())

    if decision_plan.get("eda_allowed", False) and not has_numeric_analysis:
        analysis_risks.append("EDA did not identify any numeric relationships; signal strength may be weak.")


    # Modeling Risks
    modeling_config = decision_plan.get("modeling",{})
    if not modeling_config.get("modeling_allowed",False):
        modeling_risks.append(f"Modeling was skipped due to reason: '{modeling_config.get("modeling_reason","Unknown")}'")
    else:
        if validation_result["metrics"]["rows"] < risk_rules.get("modeling_risks",{}).get("small_sample_warning_if_rows_lt",800):
            modeling_risks.append(f"Model trained on relatively small dataset: results may not generalize.")
        
        rmse = model_result.get("metrics",{}).get("rmse",None)
        if rmse is not None and rmse <=0:
            modeling_risks.append("Model RMSE is zero or negative; possible data leakage.")

    # Business Risks
    for column, profile in column_profiles.items():
        if profile["outlier_pct"] is not None and profile["outlier_pct"] >= risk_rules.get("business_risks",{}).get("extreme_values_pct_gt",100):
            business_risks.append(f"Extreme values detected in '{column}' which may skew business conclusions.")
    
    # Final Fallback
    if not any([data_quality_risks, analysis_risks, modeling_risks, business_risks]):
        data_quality_risks.append("No significant risks detected; based on current rules.")
    
    return {
        "data_quality_risks": data_quality_risks,
        "analysis_risks": analysis_risks,
        "modeling_risks": modeling_risks,
        "business_risks": business_risks
    }
