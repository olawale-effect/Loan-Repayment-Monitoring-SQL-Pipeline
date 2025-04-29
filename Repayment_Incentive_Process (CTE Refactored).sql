
-- Creation process of the Base Table. The dates for these processes changes Month on Month
CREATE TABLE lcp_incentive_table1
AS
(SELECT *,
CASE WHEN payment_count = 1 THEN monthly_payment 
	WHEN payment_count > 1 AND (Overdue_Payment+curr_month_collection) > total_charged_amount THEN total_charged_amount
		ELSE prev_collection_curr_month+Overdue_Payment+curr_month_collection END AS Total_Incentive_Collection,
Cur_Advance_Count*monthly_payment Current_for_Advance,
CASE WHEN collection_interval = -99 AND `advanced_status` > 0 THEN 'Ontime'
	WHEN current_payment_status = 'Ontime Repayment' AND `advanced_status` = 0 AND TIMESTAMPDIFF(DAY, exp_payment_date, payment_date) <= 0 THEN 'Ontime'
	WHEN current_payment_status = 'Ontime Repayment' AND `advanced_status` = 0 AND TIMESTAMPDIFF(DAY, exp_payment_date, payment_date) BETWEEN 1 AND 5 THEN '1-5days Delay'
	WHEN current_payment_status = 'Ontime Repayment' AND `advanced_status` = 0 AND TIMESTAMPDIFF(DAY, exp_payment_date, payment_date) > 5 THEN '> 5days Delay Paid'
	WHEN current_payment_status = 'Delayed Repayment' AND collection_interval BETWEEN 1 AND 5 THEN '1-5days Delay'
	WHEN current_payment_status = 'Delayed Repayment' AND collection_interval > 5 THEN '> 5days Delay Paid' #and owed_period > 0
	WHEN current_payment_status = 'Delayed Repayment' AND collection_interval < 0 THEN '> 5days Delay Paid'
	WHEN current_payment_status = 'Defaulter' THEN 'Unpaid' ELSE current_payment_status END monthly_collection_status
FROM (
SELECT *, CASE WHEN paidperiod > expected_paidperiod THEN advanced_status*monthly_payment 
	  ELSE 0 END Total_Advance,
	  CASE WHEN prev_exp_pp > previous_paidperiod AND expected_paidperiod > paidperiod THEN 0 ELSE
	  CASE 
	    WHEN previous_paidperiod = paidperiod THEN 0 ELSE CASE
							WHEN expected_paidperiod - prev_exp_pp > 1 AND previous_paidperiod > prev_exp_pp AND paidperiod > previous_paidperiod AND paidperiod <= expected_paidperiod THEN 0
							WHEN previous_paidperiod > prev_exp_pp AND paidperiod > previous_paidperiod
							THEN paidperiod - previous_paidperiod ELSE CASE WHEN paidperiod - expected_paidperiod < 0 THEN 0
													ELSE paidperiod - expected_paidperiod END
							END
	    END
	  END Cur_Advance_Count,
	CASE WHEN Payment_Status = 'Yes' AND previous_paidperiod >= expected_paidperiod AND`payment_count` > 0 THEN 0
	ELSE CASE WHEN Payment_Status = 'Yes' AND `payment_count` > 0 THEN monthly_payment ELSE 0 END END curr_month_collection,
	CASE WHEN previous_paidperiod >= expected_paidperiod THEN monthly_payment ELSE
	CASE WHEN expected_paidperiod - prev_exp_pp > 1 AND previous_paidperiod > prev_exp_pp AND payment_status = 'Yes' THEN monthly_payment ELSE 0 END
	END prev_collection_curr_month,
	CASE WHEN prev_exp_pp > previous_paidperiod AND paidperiod >= expected_paidperiod
	     THEN (prev_exp_pp - previous_paidperiod)*monthly_payment
	     WHEN expected_paidperiod - prev_exp_pp > 1 AND payment_count > 0 AND payment_status = 'No' THEN payment_count*monthly_payment
		ELSE
		  CASE WHEN prev_exp_pp > previous_paidperiod AND paidperiod < expected_paidperiod THEN `total_charged_amount`##payment_count*monthly_payment
		  ELSE 0
		END
	END Overdue_Payment,
	CASE WHEN current_payment_status = 'Ontime Repayment' THEN -99
		WHEN current_payment_status = 'Delayed Repayment' THEN TIMESTAMPDIFF(DAY, exp_payment_date, payment_date)
		#when current_payment_status = 'Revised Anniversary' and month(payment_date) = (MONTH(CURDATE()- INTERVAL 1 MONTH)) and payment_date < last_anniversary_date THEN 0
	ELSE ABS(days_deficit) END collection_interval
FROM(
SELECT DISTINCT contract_number, client_number, entry_date, tm_name, `current_agent_id`, customer_name, system_id, full_initial_payment, monthly_payment, upfront_days, CASE WHEN prev_exp_pp - ltoperiod > 0 THEN ltoperiod ELSE prev_exp_pp END AS prev_exp_pp, expected_paidperiod, previous_paidperiod, ltoperiod, paidperiod, 
`total_days_activated`, last_status_update, next_status_update, days_deficit, exp_payment_date, previous_payment_date, payment_count, total_charged_amount, IFNULL(`min_payment_date`,`last_payment_date`) payment_date,  IFNULL(next_exp_payment_date, next_status_update) next_payment_date,
CASE WHEN ltoperiod = paidperiod THEN 'Yes' ELSE (CASE WHEN paidperiod >= expected_paidperiod THEN 'Yes' ELSE 'No' END) END Payment_Status,
CASE WHEN paidperiod >= expected_paidperiod AND COALESCE(min_payment_date,`last_payment_date`) <= exp_payment_date THEN 'Ontime Repayment'
	WHEN paidperiod >= expected_paidperiod AND previous_payment_date > entry_date THEN 'Ontime Repayment'
	WHEN previous_paidperiod >= expected_paidperiod THEN 'Ontime Repayment'
	WHEN `status` = 'PAIDOFF' AND paidperiod = `ltoperiod` AND COALESCE(`min_payment_date`,`last_payment_date`) <= `lto_exit_date` THEN 'Ontime Repayment'
	WHEN `status` = 'PAIDOFF' AND paidperiod = `ltoperiod` AND COALESCE(`min_payment_date`,`last_payment_date`) > `lto_exit_date` THEN 'Delayed Repayment'
	WHEN paidperiod >= expected_paidperiod AND payment_count > 0 AND COALESCE(min_payment_date,`last_payment_date`) > exp_payment_date THEN 'Delayed Repayment'
	WHEN `status` != 'PAIDOFF' AND `days_deficit` <= 0 AND paidperiod < expected_paidperiod AND next_status_update <= '2025-02-28' THEN 'Defaulter'
	WHEN `status` != 'PAIDOFF' AND `days_deficit` <= 0 AND exp_payment_date > COALESCE(`min_payment_date`,`last_payment_date`) THEN 'Defaulter'
	WHEN contract_number IN (SELECT `contractNumber` FROM `l1_forgiven_contracts_use`) AND paidperiod < expected_paidperiod AND next_exp_payment_date > '2025-03-31' THEN 'Revised Anniversary' ###Write this in such way that it captures all contracts that have once been repossessed, add it to thus current condition
	ELSE 'Defaulter' END current_payment_status, advanced_status, owed_period, `customer_state`, `customer_district`, `agent_district`
FROM (
SELECT DISTINCT a.contract_number, client_number, entry_date, tm_name, `current_agent_id`, customer_name, system_id, full_initial_payment, monthly_payment, upfront_days, `status`, FLOOR((TIMESTAMPDIFF(DAY, entry_date, '2025-02-28'))/30)+1 prev_exp_pp, CASE WHEN expected_paidperiod >= ltoperiod THEN ltoperiod ELSE expected_paidperiod END AS expected_paidperiod, previous_paidperiod, ltoperiod, paidperiod, 
`total_days_activated`, last_status_update, next_status_update, days_deficit, previous_payment_date, min_payment_date, last_payment_date, payment_count, total_charged_amount, 
CASE WHEN a.contract_number = `contractNumber` THEN ADDDATE(`new_anniversary_date`, (FLOOR(TIMESTAMPDIFF(DAY, `new_anniversary_date`, '2025-03-31')/30)*30))
	WHEN `paidperiod` = `ltoperiod` THEN `lto_exit_date`
	ELSE ADDDATE(entry_date, (FLOOR(TIMESTAMPDIFF(DAY, entry_date, '2025-03-31')/30)*30)) END AS exp_payment_date,
CASE WHEN a.contract_number = `contractNumber` THEN ADDDATE(`new_anniversary_date`, ((FLOOR((TIMESTAMPDIFF(DAY, `new_anniversary_date`, '2025-03-31'))/30)+1)*30))
	WHEN `paidperiod` = `ltoperiod` THEN `lto_exit_date`
	ELSE ADDDATE(entry_date, ((FLOOR((TIMESTAMPDIFF(DAY, entry_date, '2025-03-31'))/30)+1)*30)) END AS next_exp_payment_date,
CASE WHEN paidperiod = expected_paidperiod THEN 0
						WHEN expected_paidperiod > ltoperiod AND paidperiod >= ltoperiod THEN 0
						ELSE paidperiod - expected_paidperiod END AS advanced_status, CASE WHEN paidperiod >= ltoperiod THEN 0 ELSE expected_paidperiod - paidperiod END AS owed_period, `customer_state`, `customer_district`, `agent_district`,`lto_exit_date`
FROM (
(SELECT a.*, `tm_name`, `current_agent_id`, customer_name, `customer_state`, `customer_district`, `agent_district`
FROM (
(SELECT DISTINCT a.`contract_number`, a.`client_number`, a.`created_date` entry_date, a.`system_id`, a.`full_initial_payment`, a.`monthly_payment`, a.`upfront_days`, DAY(a.created_date) created_day, FLOOR((TIMESTAMPDIFF(DAY, a.created_date, '2025-03-31'))/30)+1 AS expected_paidperiod
FROM `upya_activity_log` a
JOIN `upya_activity_log` b
WHERE a.`contract_number` = b.`contract_number`
AND a.`report_date` = LAST_DAY(CURDATE() - INTERVAL 1 MONTH)
AND b.`report_date` = LAST_DAY(CURDATE() - INTERVAL 2 MONTH)
AND a.`new_active_status` != 'Lost'
AND b.`new_active_status` = 'Active') a

INNER JOIN

(SELECT DISTINCT `contract_number`, `current_tm` AS `tm_name`, `current_agent_id`, CONCAT(`first_name`,' ',`last_name`) AS customer_name, `customer_state`, `customer_district`, `agent_district`
FROM `upya_lcp_base_use`
WHERE `tenure` != 'Outright plan'
AND created_date <= LAST_DAY(CURDATE() - INTERVAL 1 MONTH)
#AND DAY(created_date) <= 31
) b
ON a.`contract_number` = b.`contract_number`
	)
) a

LEFT OUTER JOIN

(SELECT `contract_number`, CASE WHEN `tenure` != 'Outright plan' AND `status` = 'PAIDOFF' THEN `ltoperiod`
ELSE `paidperiod` END previous_paidperiod
FROM `upya_activity_log`
WHERE `report_date` = LAST_DAY(CURDATE() - INTERVAL 2 MONTH)) b
ON a.`contract_number` = b.`contract_number`

LEFT OUTER JOIN

(SELECT `contract_number`, `status`, `ltoperiod`, CASE WHEN `tenure` != 'Outright plan' AND `status` = 'PAIDOFF' THEN `ltoperiod` ELSE `paidperiod` END AS `paidperiod`, `total_days_activated`, DATE(`last_status_update`) last_status_update, DATE(`next_status_update`) next_status_update, `days_deficit`,
`lto_exit_date`
FROM `upya_activity_log`
WHERE `report_date` = LAST_DAY(CURDATE() - INTERVAL 1 MONTH)) c
ON a.`contract_number` = c.`contract_number`

LEFT OUTER JOIN

(SELECT `contract_number`, `created_date`, MAX(CASE WHEN `report_date` <= LAST_DAY(CURDATE() - INTERVAL 2 MONTH) THEN DATE(`payment_date`) END) previous_payment_date, MAX(CASE WHEN `report_date` <= LAST_DAY(CURDATE() - INTERVAL 1 MONTH) THEN DATE(`payment_date`) END) last_payment_date,
MIN(CASE WHEN `report_date` = LAST_DAY(CURDATE() - INTERVAL 1 MONTH) THEN DATE(`payment_date`) END) min_payment_date, SUM(CASE WHEN `report_date` = LAST_DAY(CURDATE() - INTERVAL 1 MONTH) THEN `amount` END) `total_charged_amount`,
COUNT(CASE WHEN `report_date` = LAST_DAY(CURDATE() - INTERVAL 1 MONTH) THEN `contract_number` END) `payment_count`
FROM `upya_daily_transactions`
WHERE `contract_number` IN (SELECT `contract_number` FROM `upya_lcp_base_use` WHERE `tenure` != 'Outright plan' AND created_date <= LAST_DAY(CURDATE() - INTERVAL 2 MONTH) AND DAY(created_date) <= 31)
AND `payment_classification` IN ('Repayment','Check')
GROUP BY `contract_number`, `created_date`) d
ON a.`contract_number` = d.`contract_number`

LEFT OUTER JOIN

(SELECT `contractNumber`, `new_anniversary_date` FROM `l1_forgiven_contracts_use`) e
ON a.`contract_number` = e.`contractNumber`
	)
     ) f
    ) g
  ) h
);



🚀 -- Refactored SQL (CTE Style)

WITH 
-- Step 1: Base Table (upya_lcp_incentive_table1)
base_table AS (
  SELECT *
  FROM `lcp_incentive_table1`
),

-- Step 2: Defaulters Payment Capture
defaulters_check AS (
  SELECT 
    `contract_number`, 
    `paidperiod` AS paidperiod_ii, 
    `payment_date` AS payment_date_ii
  FROM `defaulters_check` -- Another Base Table that captures payment status within the first 5 days of the new month. This will be referencing the previous moth however
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
