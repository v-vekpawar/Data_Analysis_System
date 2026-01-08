"""This module is responsible for exploratory data analysis (EDA) on the dataset."""
import os
import pandas as pd
import matplotlib.pyplot as plt

def execute_eda(df: pd.DataFrame, decision_plan: dict, output_dir: str = "outputs/eda"):
    """Execute exploratory data analysis based on decision plan."""
    
    eda_metrics = {}
    eda_log = []
    plots = []

    if not decision_plan.get("eda_allowed", False):
        eda_log.append("EDA skipped as per decision plan.")
        return {
            "eda_metrics": eda_metrics,
            "eda_log": eda_log,
            "plots": plots
        }
    
    os.makedirs(output_dir, exist_ok=True)

    numeric_columns = df.select_dtypes(include=['number']).columns
    categorical_columns = df.select_dtypes(include=['object', 'category']).columns

    # Numeric vs Numeric Correlation
    if len(numeric_columns) >= 2:
        corr_matrix = df[numeric_columns].corr()
        eda_metrics["correlation_matrix"] = corr_matrix.round(3).to_dict()
        eda_log.append("Calculated correlation matrix for numeric columns.")

        plt.figure()
        corr_matrix.plot(kind='bar')
        plot_path = os.path.join(output_dir, "numeric_correlation.png")
        plt.tight_layout()
        plt.savefig(plot_path)
        plt.close()
        plots.append(plot_path)
        eda_log.append(f"Saved numeric correlation plot at {plot_path}.")

    else:
        eda_log.append("Not enough numeric columns for correlation analysis.")

    # Numeric Distributions
    for column in numeric_columns:
        plt.figure()
        df[column].dropna().hist()
        plt.title(f"Distribution of {column}")
        plt.xlabel(column)
        plt.ylabel("Frequency")
        plot_path = os.path.join(output_dir, f"{column}_distribution.png")
        plt.tight_layout()
        plt.savefig(plot_path)
        plt.close()
        plots.append(plot_path)
        eda_log.append(f"Saved distribution plot for numeric column '{column}' at {plot_path}.")
    
    # Category vs Numeric: Group means
    for cat_col in categorical_columns:
        for num_col in numeric_columns:
            group_means = df.groupby(cat_col)[num_col].mean()
            eda_metrics[f"{cat_col}_vs_{num_col}_mean"] = group_means.round(2).to_dict()
            eda_log.append(f"Calculated group means of numerical column '{num_col}' group by categorical column '{cat_col}'.")

            plt.figure()
            df.boxplot(column=num_col, by=cat_col)
            plt.title(f"{num_col} by {cat_col}")
            plt.suptitle("")
            plt.xlabel(cat_col)
            plt.ylabel(num_col)
            plot_path = os.path.join(output_dir, f"{cat_col}_vs_{num_col}_boxplot.png")
            plt.tight_layout()
            plt.savefig(plot_path)
            plt.close()
            plots.append(plot_path)
            eda_log.append(f"Saved boxplot for '{num_col}' by '{cat_col}' at {plot_path}.")
        
    return {
        "eda_metrics": eda_metrics,
        "eda_log": eda_log,
        "plots": plots
    }