from pipeline.ingest import load_inputs
from pipeline.validate import validate_dataset
from profiling.column_profiler import profile_columns
from core.decision_engine import generate_decision_plan
from pipeline.clean import execute_cleaning
from pipeline.eda import execute_eda
from pipeline.model import execute_model
from risk.risk_aggregator import aggregate_risk
from outputs.report_generator import generate_report

def run_pipeline(dataset_path: str, rules_path: str):
    """
    Runs the full decision-driven operational pipeline.
    Returns path to generated report.
    """

    # 1. Ingest
    ingest_result = load_inputs(dataset_path, rules_path)
    df = ingest_result['data']
    rules = ingest_result['rules']

    # 2. Validate
    validation_result = validate_dataset(df, rules)
    if validation_result['status'] == 'FAIL':
        decision_plan = {
            "columns_to_drop": [],
            "imputation_plan": {},
            "eda_allowed": False,
            "modeling": {
                "modeling_allowed": False,
                "modeling_reason": "Dataset failed validation.",
                "target_column": None,
                "target_type": None
                },
            "decision_log": validation_result["reasons"]
        }

        risk_summary = {
            "data_quality_risks": validation_result["reasons"],
            "analysis_risks": [],
            "modeling_risks": [],
            "buisness_risks": []
        }

        report_path = generate_report(
            validation_result = validation_result,
            decision_plan = decision_plan,
            cleaning_result = {"cleaning_log" : []},
            eda_result={"eda_metrics": {}, "plots": [], "eda_log": []},
            model_result={"model_used": None, "metrics": {}, "model_log": []},
            risk_summary=risk_summary
        )

        return report_path
    
    # Profile
    column_profiles = profile_columns(df)

    # Decision Plan
    decision_plan = generate_decision_plan(validation_result, column_profiles, rules)

    # Clean 
    cleaning_result = execute_cleaning(df, decision_plan)

    # EDA
    eda_result = execute_eda(cleaning_result['cleaned_data'], decision_plan)

    # Model
    model_result = execute_model(cleaning_result["cleaned_data"], decision_plan, rules)

    # Risk Aggregation
    risk_summary = aggregate_risk(
        validation_result=validation_result,
        column_profiles=column_profiles,
        decision_plan=decision_plan,
        cleaning_log=cleaning_result["cleaning_log"],
        eda_result=eda_result,
        model_result=model_result,
        rules=rules
        )
    
    # Report 
    report_path = generate_report(
        validation_result=validation_result,
        decision_plan=decision_plan,
        cleaning_result=cleaning_result,
        eda_result=eda_result,
        model_result=model_result,
        risk_summary=risk_summary
    )

    return report_path

if __name__ == "__main__":
    DATASET_PATH = "data/amazon_dataset.csv"
    RULES_PATH = "core/rules.yaml"

    report = run_pipeline(DATASET_PATH, RULES_PATH)
    print(f"Report generated at: {report}")
