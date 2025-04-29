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
01_base_tables.sql
sql
CopyEdit

-- Base Table
CREATE TABLE upya_lcp_incentive_table1 AS
SELECT * FROM source_table_name;
(This is placeholder, you can describe the real base if needed)
________________________________________
02_join_defaulters.sql
sql
CopyEdit

-- Join defaulters to capture first-5-days payments
CREATE TABLE upya_lcp_incentive_table2 AS (
SELECT a.*, 
       CASE WHEN paidperiod > paidperiod_ii THEN paidperiod ELSE paidperiod_ii END AS paidperiod_ii, 
       payment_date_ii
FROM (
    SELECT * FROM upya_lcp_incentive_table1
) a
LEFT OUTER JOIN (
    SELECT contract_number, 
           paidperiod AS paidperiod_ii, 
           payment_date AS payment_date_ii
    FROM defaulters_check_upya
) b
ON a.contract_number = b.contract_number
);
________________________________________
03_repayment_log_dump.sql
sql
CopyEdit

-- Create repayment log table including grace period logic
CREATE TABLE upya_l1_repayment_log AS
SELECT *,
  CASE
    WHEN monthly_collection_status NOT IN ('Unpaid', 'Revised Anniversary') THEN collection_interval
    ELSE CASE 
        WHEN monthly_collection_status IN ('Unpaid', 'Revised Anniversary') AND paidperiod_ii > paidperiod AND payment_date_ii IS NOT NULL
        THEN TIMESTAMPDIFF(DAY, exp_payment_date, payment_date_ii)
        ELSE TIMESTAMPDIFF(DAY, exp_payment_date, '2025-04-05')
    END
  END AS final_collection_interval,
  '2025-04-05' AS report_reference_date
FROM upya_lcp_incentive_table2;
________________________________________
04_status_categorization.sql
sql
CopyEdit

-- Repayment Status Categorization
ALTER TABLE upya_l1_repayment_log
ADD COLUMN final_collection_status VARCHAR(255);

UPDATE upya_l1_repayment_log
SET final_collection_status = CASE
    WHEN monthly_collection_status NOT IN ('Unpaid', 'Revised Anniversary') THEN monthly_collection_status
    ELSE CASE
        WHEN monthly_collection_status = 'Revised Anniversary' AND days_deficit > 0 AND TIMESTAMPDIFF(DAY, exp_payment_date, payment_date) <= 0 THEN 'Ontime'
        WHEN monthly_collection_status = 'Revised Anniversary' AND days_deficit > 0 AND TIMESTAMPDIFF(DAY, exp_payment_date, payment_date) > 5 THEN '> 5days Delay Paid'
        WHEN monthly_collection_status = 'Revised Anniversary' AND days_deficit > 0 AND TIMESTAMPDIFF(DAY, exp_payment_date, payment_date) BETWEEN 1 AND 5 THEN '1-5days Delay'
        WHEN monthly_collection_status IN ('Unpaid', 'Revised Anniversary') AND paidperiod_ii = expected_paidperiod AND payment_date_ii IS NOT NULL
             AND final_collection_interval <= 5 THEN '1-5days Delay'
        WHEN monthly_collection_status IN ('Unpaid', 'Revised Anniversary') AND paidperiod_ii = expected_paidperiod AND payment_date_ii IS NOT NULL
             AND final_collection_interval > 5 THEN '> 5days Delay Paid'
        ELSE '> 5days Delay Unpaid'
    END
END;
________________________________________
05_final_spool.sql
sql
CopyEdit

-- Final Aging Spool
CREATE TABLE upya_ag_ageing AS
SELECT a.*, 
       CASE
           WHEN paidperiod_ii >= ltoperiod THEN 'Owner'
           ELSE 'LTO'
       END AS own_status
FROM (
    SELECT l1.*, act.status
    FROM upya_l1_repayment_log l1
    JOIN upya_activity_log act
      ON l1.contract_number = act.contract_number
    WHERE act.report_date = '2025-03-31'
      AND l1.report_reference_date = '2025-04-05'
) a;

06_full_pipeline_final.sql
________________________________________
🚀 Refactored SQL (CTE Style)

WITH 
-- Step 1: Base Table (upya_lcp_incentive_table1)

base_table AS (
  SELECT *
  FROM `upya_lcp_incentive_table1`
),

-- Step 2: Defaulters Payment Capture

defaulters_check AS (
  SELECT 
    `contract_number`, 
    `paidperiod` AS paidperiod_ii, 
    `payment_date` AS payment_date_ii
  FROM `defaulters_check_upya`
),

-- Step 3: Joining Base Table with Defaulters Check (upya_lcp_incentive_table2)

joined_table AS (
  SELECT 
    a.*,
    CASE 
      WHEN b.paidperiod_ii IS NOT NULL AND (b.paidperiod_ii > a.paidperiod) 
        THEN b.paidperiod_ii 
      ELSE a.paidperiod 
    END AS paidperiod_ii,
    b.payment_date_ii
  FROM base_table a
  LEFT JOIN defaulters_check b
    ON a.contract_number = b.contract_number
),

-- Step 4: Calculating Final Collection Intervals and Statuses (upya_l1_repayment_log)

repayment_log AS (
  SELECT 
    *,
    CASE 
      WHEN monthly_collection_status NOT IN ('Unpaid', 'Revised Anniversary') 
        THEN collection_interval
      ELSE CASE 
        WHEN paidperiod_ii > paidperiod AND payment_date_ii IS NOT NULL
          THEN TIMESTAMPDIFF(DAY, exp_payment_date, payment_date_ii)
        ELSE TIMESTAMPDIFF(DAY, exp_payment_date, '2025-04-05')
      END
    END AS final_collection_interval,
    '2025-04-05' AS report_reference_date
  FROM joined_table
),

repayment_status AS (
  SELECT 
    *,
    CASE 
      WHEN monthly_collection_status NOT IN ('Unpaid', 'Revised Anniversary') 
        THEN monthly_collection_status
      ELSE CASE 
        WHEN monthly_collection_status = 'Revised Anniversary' 
             AND days_deficit > 0 
             AND TIMESTAMPDIFF(DAY, exp_payment_date, payment_date) <= 0 
          THEN 'Ontime'
        WHEN monthly_collection_status = 'Revised Anniversary' 
             AND days_deficit > 0 
             AND TIMESTAMPDIFF(DAY, exp_payment_date, payment_date) > 5 
          THEN '> 5days Delay Paid'
        WHEN monthly_collection_status = 'Revised Anniversary' 
             AND days_deficit > 0 
             AND TIMESTAMPDIFF(DAY, exp_payment_date, payment_date) BETWEEN 1 AND 5
          THEN '1-5days Delay'
        WHEN monthly_collection_status IN ('Unpaid', 'Revised Anniversary') 
             AND paidperiod_ii = expected_paidperiod 
             AND payment_date_ii IS NOT NULL
             AND final_collection_interval <= 5 
          THEN '1-5days Delay'
        WHEN monthly_collection_status IN ('Unpaid', 'Revised Anniversary') 
             AND paidperiod_ii = expected_paidperiod 
             AND payment_date_ii IS NOT NULL
             AND final_collection_interval > 5 
          THEN '> 5days Delay Paid'
        ELSE '> 5days Delay Unpaid'
      END
    END AS final_collection_status
  FROM repayment_log
),

-- Step 5: Final Table for Modelling and Excel Spool (upya_ag_ageing)

final_spool AS (
  SELECT 
    a.*,
    b.status,
    CASE 
      WHEN paidperiod_ii >= ltoperiod 
        THEN 'Owner'
      ELSE 'LTO'
    END AS own_status
  FROM repayment_status a
  INNER JOIN `upya_activity_log` b
    ON a.contract_number = b.contract_number
  WHERE 
    b.report_date = '2025-03-31'
    AND a.report_reference_date = '2025-04-05'
)

-- Final Select

SELECT *
FROM final_spool;

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
