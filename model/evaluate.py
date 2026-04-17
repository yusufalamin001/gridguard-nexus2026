"""
model/evaluate.py

NOTE: In this MVP, evaluation logic is embedded directly in train.py
as part of the Out-of-Time Validation step using December 2025 holdout
data. This is intentional for a hackathon scope.

In a production system, this script would:
- Load the saved model artifact (xgboost_gridguard.json)
- Run it weekly against new SCADA data
- Detect model drift across seasons
- Generate ROC curves and performance trend reports

See train.py Step 4 for current evaluation implementation.
"""