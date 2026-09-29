# Automated ERP Forensic Audit Analytics Engine

An automated PySpark-based forensic accounting and audit analytics framework designed to analyze enterprise financial data, flag compliance risks, and detect transactional anomalies.

## Overview
This project integrates directly with enterprise general ledger and journal entry tables to execute automated fraud-detection tests and financial controls verification using PySpark.

## Key Audit Tests & Analytics
* **Duplicate Invoice Detection**: Flags duplicate invoice numbers within the transactional dataset.
* **Round Amount Anomaly Detection**: Identifies suspicious round-figure postings that may indicate manual override or fraud.
* **Weekend Posting Checks**: Detects unauthorized or unusual entries posted on weekends and non-business days.
* **Below-Threshold Monitoring**: Tracks transactions structured just below approval thresholds.
* **Benford’s Law Distribution Analysis**: Evaluates numerical digit distributions to spot unnatural reporting patterns.
* **Unbalanced Entry Checks**: Flags journal entries where debits do not equal credits.
