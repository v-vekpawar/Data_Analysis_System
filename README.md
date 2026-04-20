# Decision-Driven Data Analysis Pipeline

A rule-based automated pipeline for data validation, cleaning, analysis, and modeling that enforces deterministic decision-making at every stage.

## Overview

This system processes raw datasets through a configurable rules engine, making explicit decisions about data quality, feature engineering, exploratory analysis, and model training. Every action is logged and justified, producing a comprehensive PDF report with findings, decisions, and risk assessments.

## Architecture

The pipeline consists of six key stages:

1. **Ingest** → Load dataset and rules from CSV/Excel and YAML
2. **Validate** → Check dataset against minimum quality thresholds
3. **Profile** → Analyze column characteristics (missing %, outliers, data type, etc.)
4. **Decide** → Generate action plan based on validation and profiling
5. **Execute** → Clean, analyze (EDA), and model based on decisions
6. **Report** → Aggregate risks and generate PDF report

## Project Structure

```
├── core/
│   ├── decision_engine.py    # Rule-based decision making logic
│   └── rules.yaml            # Configuration: validation, cleaning, EDA, modeling
├── pipeline/
│   ├── ingest.py             # Load data and rules
│   ├── validate.py           # Dataset validation
│   ├── clean.py              # Data cleaning (drop columns, imputation)
│   ├── eda.py                # Exploratory data analysis
│   └── model.py              # Model training and evaluation
├── profiling/
│   └── column_profiler.py    # Column-level profiling
├── risk/
│   └── risk_aggregator.py    # Risk detection and aggregation
├── outputs/
│   └── report_generator.py   # PDF report generation
└── run.py                    # Pipeline orchestrator
```

## Key Features

### Rules-Based Decision Making
All decisions are defined in `core/rules.yaml`. No improvisations—if a rule doesn't exist, the action is skipped.

**Rule Categories:**
- **Dataset Validation** — Min/max rows, columns, overall missing %
- **Column Cleaning** — Drop rules (missing %, constant columns, ID-like)
- **Missing Values** — Imputation strategy by column type (numeric/categorical)
- **EDA** — Correlation thresholds, plot generation, skip conditions
- **Modeling** — Minimum rows, allowed models, feature limits, evaluation metrics
- **Risk Flags** — Data quality, modeling, and business risk thresholds

### Deterministic Data Cleaning
- Drops columns based on missing % and ID-like characteristics
- Imputes missing values using median (numeric) or mode (categorical)
- All actions logged with justification

### Automated EDA
- Correlation analysis for numeric columns
- Distribution plots
- Group-wise comparisons (categorical vs numeric)
- Generates visualizations and metrics

### Constrained Modeling
- Only runs if dataset meets minimum size thresholds
- Supports Linear Regression and Random Forest
- Automatically selects last numeric column as target
- Evaluates on test set (80/20 split)
- Saves trained model as artifact

### Risk Aggregation
Flags risks across four categories:
- **Data Quality** — High missing %, outliers, dropped columns
- **Analysis** — EDA skipped or weak signals
- **Modeling** — Small sample size, poor model performance
- **Business** — Extreme values affecting conclusions

## Usage

### Installation

```bash
pip install pandas scikit-learn reportlab pyyaml openpyxl
```

### Running the Pipeline

```bash
python run.py
```

By default, it loads:
- Dataset: `data/amazon_dataset.csv`
- Rules: `core/rules.yaml`

Output: PDF report at `outputs/report.pdf`

### Custom Dataset

Edit `run.py` to specify your dataset and rules paths:

```python
DATASET_PATH = "path/to/your/dataset.csv"
RULES_PATH = "path/to/your/rules.yaml"
```

## Output

The pipeline generates a structured PDF report containing:
- **Executive Summary** — Dataset metrics and decisions
- **Decisions Taken** — All decision logs
- **Cleaning Summary** — Columns dropped, values imputed
- **Exploratory Data Analysis** — Metrics and plots
- **Modeling Results** — Model type, performance metrics
- **Risks & Warnings** — Categorized risk flags

## Configuration Example

```yaml
dataset_validation:
  min_rows: 50
  min_columns: 3
  max_missing_overall_pct: 40

column_cleaning:
  drop_column_if_missing_pct_gt: 30
  drop_constant_columns: true

missing_values:
  numeric:
    if_missing_pct_lt: 10
    strategy: median
  categorical:
    if_missing_pct_lt: 10
    strategy: mode

modeling:
  allow_if:
    min_rows: 500
```

## Design Philosophy

- **Explainability** — Every decision is logged and justified
- **Governance** — Rules are centralized and version-controlled
- **Safety** — Modeling doesn't run unless conditions are met
- **Scalability** — Easy to add new rules without code changes

## Work Remaining
This project is not fully complete. The following improvements are expected:
- Add handling for numeric data wrapped in string type.
- Implement Telegram bot for deployment.
- Add GenAI features for chatting with data.

## 👤 Author
Vivek Pawar (Original Creator)

## 📜 License
This project is licensed under the MIT License - see the LICENSE file for details.