"""This module is responsible for model training and evaluation."""
import os
import joblib
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import root_mean_squared_error
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor

ALLOWED_MODELS = {
    "linear_regression": LinearRegression,
    "random_forest": RandomForestRegressor
}

def execute_model(df: pd.DataFrame, decision_plan: dict,rules: dict, output_dir: str="outputs/model",):
    """Implement model training and evaluation based on decision plan and rules."""
    
    model_log = []
    metrics = {}

    modeling_config = decision_plan.get("modeling",{})
    if not modeling_config.get("modeling_allowed",False):
        model_log.append(f"Modeling skipped: {decision_plan.get("modeling_reason","Not Permitted")}")
        return {
            "model_used": None,
            "metrics": metrics,
            "model_artifact": None,
            "model_log": model_log
        }
    
    target_column = modeling_config.get("target_column")

    if not target_column or target_column not in df.columns:
        model_log.append("Target column 'target' not found in dataset. Modeling aborted.")
        return {
            "model_used": None,
            "metrics": metrics,
            "model_artifact": None,
            "model_log": model_log
        }
    
    # Prepare X and Y
    X = df.drop(columns=[target_column])
    y = df[target_column]

    # Numeric features only for modeling    
    X = X.select_dtypes(include=['number']).fillna(0)

    if X.empty:
        model_log.append("No numeric features available for modeling.")
        return {
            "model_used": None,
            "metrics": metrics,
            "model_artifact": None,
            "model_log": model_log
        }
    
    model_rules = rules.get("modeling",{})
    max_features = model_rules.get("max_features",15)
    model_name = model_rules.get("default_model","linear_regression")
    
    if X.shape[1] > max_features:
        X = X.iloc[:, :max_features]
        model_log.append(f"Feature count exceed limit.Truncated to first {max_features} features") 
    
    # Train / Test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model_cls = ALLOWED_MODELS.get(model_name, LinearRegression)
    model = model_cls()
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    rmse = root_mean_squared_error(y_test, preds, squared=False)
    metrics["rmse"] = round(rmse, 4)

    # Persist artifact
    os.makedirs(output_dir, exist_ok=True)
    model_path = os.path.join(output_dir, f"{model_name}.joblib")
    joblib.dump(model, model_path)

    model_log.append(f"Trained {model_name} model.")
    model_log.append(f"Target column: '{target_column}'.")
    model_log.append(f"RMSE on test set: {metrics['rmse']}.")

    return {
        "model_used": model_name,
        "metrics": metrics,
        "model_artifact": model_path,
        "model_log": model_log
    }