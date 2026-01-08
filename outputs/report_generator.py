"""It formats existing truth into consumable artifacts."""
import os
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet

def generate_report(validation_result: dict, decision_plan: dict, cleaning_result: dict, eda_result: dict, model_result: dict, risk_summary: dict, output_path: str = "outputs/report.pdf"):
    """Generate Report pdf which includes Executive Summary,Decision Taken, Key Findings, Risks & Warnings, Appendix"""

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    doc = SimpleDocTemplate(output_path, pagesize=A4)
    styles = getSampleStyleSheet()
    story = []

    def add_heading(text):
        story.append(Paragraph(f"<b>{text}</b>", styles["Heading2"]))
        story.append(Spacer(1, 12))
    
    def add_text(text):
        story.append(Paragraph(text, styles["Normal"]))
        story.append(Spacer(1, 8))
    
    # Executive Summary
    add_heading("Executive Summary")

    add_text(f"Dataset rows: {validation_result['metrics']['rows']}")
    add_text(f"Dataset columns: {validation_result['metrics']['columns']}")
    add_text(f"EDA allowed: {decision_plan.get('eda_allowed')}")
    add_text(f"Modeling allowed: {decision_plan.get('modeling', {}).get('modeling_allowed')}")

    # Decision Taken
    add_heading("Decisions Taken")

    for log in decision_plan.get("decision_log", []):
        add_text(f"- {log}")
    
    # Cleaning Summary
    add_heading("Cleaning Summary")

    for log in cleaning_result.get("cleaning_log", []):
        add_text(f"- {log}")
    
    # EDA Findings
    add_heading("Exploratory Data Analysis")

    if not eda_result.get("eda_metrics"):
        add_text("EDA was skipped or produced no metrics.")
    else:
        for key, value in eda_result["eda_metrics"].items():
            add_text(f"{key}: {value}")
    
    # Embed plots
    for plot_path in eda_result.get("plots", []):
        if os.path.exists(plot_path):
            story.append(Image(plot_path, width=400, height=250))
            story.append(Spacer(1, 12))
    
    # Modeling Results
    add_heading("Modeling Results")

    if model_result.get("model_used") is None:
        for log in model_result.get("model_log", []):
            add_text(f"- {log}")
    else:
        add_text(f"Model used: {model_result['model_used']}")
        for metric, value in model_result.get("metrics", {}).items():
            add_text(f"{metric}: {value}")
    
    # Risks & Warnings
    add_heading("Risks & Warnings")

    for category, risks in risk_summary.items():
        add_text(f"<b>{category.replace('_', ' ').title()}</b>")
        for r in risks:
            add_text(f"- {r}")

    doc.build(story)
    return output_path