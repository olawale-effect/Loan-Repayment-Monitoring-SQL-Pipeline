# Loan-Repayment-Monitoring-SQL-Pipeline
Incentive Eligibility and Status Modeling for Partner Collections

## Overview
This project implements a SQL-based data pipeline to monitor loan repayment behaviors, identify defaulters, and categorize payment statuses based on timing delays.

Built for business intelligence and operational oversight for financial service providers.

## Problem Statement
Financial organizations often struggle with distinguishing between minor payment delays and chronic defaulters. A reliable and automated repayment classification system is critical for early detection and intervention.

## Key Features
- Capture payments made within a 5-day grace window after the due date.
- Differentiate between on-time payments, minor delays, significant delays, and unpaid loans.
- Identify asset ownership status (Owner vs Lease-To-Own).
- Provide ready-to-analyze final dataset for Excel modeling.

## Data Sources
- `upya_lcp_incentive_table1`: Original loan repayment records.
- `defaulters_check_upya`: Payment transactions captured within 5 days into the new month.
- `upya_activity_log`: Asset and account metadata.

## Technical Stack
- **SQL** (MySQL, BigQuery Compatible)
- **Excel** (for final modeling phase)

## Pipeline Steps
1. Base table creation from original loan repayment records.
2. Join with defaulters check table to capture first-5-day payments.
3. Build repayment log dump including grace period adjustments.
4. Classify final repayment status (Ontime, 1-5days Delay, >5days Delay Paid, >5days Delay Unpaid).
5. Generate final spool ready for Excel modeling.

## Folder Structure
- `/sql`: Modular SQL scripts broken down into pipeline steps.
- `/notes`: Business assumptions and rule documentation.
- `/diagrams`: Visual flow (optional for later enhancements).

## How to Run
1. Execute each script sequentially under the `sql/` folder.
2. Final output: `upya_ag_ageing` table ready for export or dashboard modeling.

## Future Enhancements
- Build a full ETL pipeline using Airflow or dbt.
- Deploy a monitoring dashboard using Tableau or Power BI.
- Integrate predictive modeling for delinquency risk.

📂 2. SQL Files
(cleaned, modular, easy to read)

- [Loan-Repayment-Monitoring-SQL-Pipeline](https://github.com/olawale-effect/Loan-Repayment-Monitoring-SQL-Pipeline/blob/main/Repayment_Incentive_Process.sql)
- [Loan-Repayment-Monitoring-SQL-Pipeline](https://github.com/olawale-effect/Loan-Repayment-Monitoring-SQL-Pipeline/blob/main/Repayment_Incentive_Process%20(CTE%20Refactored).sql)


📝 3. Notes Folder
assumptions.md
markdown
CopyEdit
# Assumptions
- Payments captured in the first 5 days of a new month still belong to the previous month's dues.
- Expected paid periods are aligned based on contract rules.
- "Owner" is defined as users who have fully paid past their leasing period (ltoperiod).
- "LTO" refers to Lease-To-Own customers still within contractual periods.

business_rules.md
markdown
CopyEdit
# Business Rules

- Payment delays of 1-5 days are considered 'Minor Delays'.
- Delays of over 5 days are 'Significant Delays'.
- Payments captured after expected periods but within 5 days get reprieved from full delinquency classification.
- Only records validated against latest activity logs (`upya_activity_log`) are reported.

________________________________________
📸 4. (Optional) Diagrams Folder
You can add a small diagram later showing:
Data Ingestion -> Defaulter Join -> Delay Categorization -> Final Report

## Author
Olawale | Business Intelligence | Data Engineering | Data Analytics
