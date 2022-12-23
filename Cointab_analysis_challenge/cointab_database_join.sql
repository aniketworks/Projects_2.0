-- Business Scenario:
-- Company X wants to verify if charges levied by Partner(Delivery) company per Order are correct or not?

-- Expected Deliverables:
-- 1. Merged CSV File(desired fields are listed in Challenge document)
-- 2. Summary Table mentioning
--  2.1  Orders that has been correctly charged, count, and total amount
--  2.2  Orders that has been under charged, count, and total amount
--  2.3  Orders that has been over charged, count, and total amount

-- High Level Database intro:
-- (A) Company_x database
--      1. Orders table: Contains details of all the orders.
--      2. SKU table: Contains information about weights in gram for all the available products.
--      3. pincode table: Contains detailed list of Customer pincodes against delivery facility
--         pincode and determines zone based on that.
-- (B) Delivery company database
--      1. invoice table: contains invoices of all the ready to dispatch orders. Contains all the information
--                        about weight, zone, charges calculated by delivery company.
--      2. rates: rates table contains charges decided by delivery company based on zone, route, and weight of the package.


WITH x_calculated_weight AS
-- This CTE calculates total weight of all the orders based on invoices. Here we need group by 
-- as some of the orders contains more than one product as well.
-- Also you can observe that I have selected DISTINCT records from sku table because it had few
-- duplicate records that were skewing our join output.
(
  SELECT
    i.Order_ID,
    i.AWB_Code,
    ROUND(SUM(o.Order_Qty*s.Weight__g_)/1000,2) AS total_weight_kg_x,
    IF(ROUND(SUM(o.Order_Qty*s.Weight__g_)/1000,2)<=CEIL(ROUND(SUM(o.Order_Qty*s.Weight__g_)/1000,2))-0.5,
      CEIL(ROUND(SUM(o.Order_Qty*s.Weight__g_)/1000,2))-0.5, 
      CEIL(ROUND(SUM(o.Order_Qty*s.Weight__g_)/1000,2))
      ) AS weight_slab_x
  FROM
    `portfolio-370909.cointab_challenge.invoice_cc` AS i
  LEFT JOIN
    `portfolio-370909.cointab_challenge.order_x` AS o
  ON
    i.Order_ID = o.ExternOrderNo
  LEFT JOIN
    (SELECT DISTINCT * FROM`portfolio-370909.cointab_challenge.sku_x`) AS s
  ON
    o.SKU = s.SKU
  GROUP BY
    i.Order_ID, i.AWB_Code
),
x_calculated_zone AS
-- This other CTE takes care of calculating Zone pincode based on invoices and pincode table in X's database.
-- Even pincode table had few duplicates as well.
(
  SELECT
    i.Order_ID,
    p.Zone AS zone_x,
    IF(REGEXP_CONTAINS(i.Type_of_shipment, 'RTO'),
    CONCAT('rto_',p.Zone),
    CONCAT('fwd_',p.Zone)) AS zone_code_x
  FROM
    `portfolio-370909.cointab_challenge.invoice_cc` AS i
  LEFT JOIN
    (SELECT DISTINCT * FROM `portfolio-370909.cointab_challenge.pincode_x`) AS p
  ON
    i.Customer_Pincode = p.Customer_Pincode AND
    i.Warehouse_Pincode = p.Warehouse_Pincode
)
-- Finally, we join above two CTE's with invoices table once again to fetch other necessary fields.
-- We were not able to fetch those previously as we did group by in first CTE.
SELECT
  x_w.Order_ID,
  x_w.AWB_Code,
  x_w.total_weight_kg_x,
  x_w.weight_slab_x,
  icc.Charged_Weight AS total_weight_kg_c,
  IF(ROUND(icc.Charged_Weight,2)<=CEIL(ROUND(icc.Charged_Weight,2))-0.5,
      CEIL(ROUND(icc.Charged_Weight,2))-0.5, 
      CEIL(ROUND(icc.Charged_Weight,2))
      ) AS weight_slab_c,
  x_z.zone_x,
  icc.Zone AS zone_c,
  -- final price as per x
  (IF(x_w.weight_slab_x=0.5,
    (SELECT price FROM `portfolio-370909.cointab_challenge.rates_improved_cc` WHERE zone_rate_code = CONCAT(x_z.zone_code_x,'_fixed')),
    (SELECT price FROM `portfolio-370909.cointab_challenge.rates_improved_cc` WHERE zone_rate_code = CONCAT(x_z.zone_code_x,'_fixed')))+
  IF(x_w.weight_slab_x>0.5,(x_w.weight_slab_x-0.5)/0.5
  ,0)*
  IF(x_w.weight_slab_x>0.5,
    (SELECT price FROM `portfolio-370909.cointab_challenge.rates_improved_cc` WHERE zone_rate_code = CONCAT(x_z.zone_code_x,'_additional')),0)) AS x_expected_price,
  icc.Billing_Amount__Rs__ AS c_charged_price
FROM
  x_calculated_weight AS x_w
LEFT JOIN
  x_calculated_zone AS x_z
ON
  x_w.Order_ID = x_z.Order_ID
LEFT JOIN
  `portfolio-370909.cointab_challenge.invoice_cc` AS icc
ON
  x_w.Order_ID = icc.Order_ID
;
-- Above query returns all the crucial fields needed for the deliverable. Other remaining fields will be calculated in 
-- Spreadsheet


-- rates table was in very different format than other tables hence I had to
-- manually improved rates table so that it is searchable

-- This CTE fixed the formatting and later I exported the output as BigQuery Table to use it in main query.
WITH improved_rate_cc AS(
  SELECT 'fwd_a_fixed' AS zone_rate_code, 29.5 AS price UNION ALL
  SELECT 'fwd_a_additional' AS zone_rate_code, 23.6 AS price UNION ALL
  SELECT 'fwd_b_fixed' AS zone_rate_code, 33 AS price UNION ALL
  SELECT 'fwd_b_additional' AS zone_rate_code, 28.3 AS price UNION ALL
  SELECT 'fwd_c_fixed' AS zone_rate_code, 40.1 AS price UNION ALL
  SELECT 'fwd_c_additional' AS zone_rate_code, 38.9 AS price UNION ALL
  SELECT 'fwd_d_fixed' AS zone_rate_code, 45.5 AS price UNION ALL
  SELECT 'fwd_d_additional' AS zone_rate_code, 44.8 AS price UNION ALL
  SELECT 'fwd_e_fixed' AS zone_rate_code, 56.6 AS price UNION ALL
  SELECT 'fwd_e_additional' AS zone_rate_code, 55.5 AS price UNION ALL
  SELECT 'rto_a_fixed' AS zone_rate_code, 13.6 AS price UNION ALL
  SELECT 'rto_a_additional' AS zone_rate_code, 23.6 AS price UNION ALL
  SELECT 'rto_b_fixed' AS zone_rate_code, 20.5 AS price UNION ALL
  SELECT 'rto_b_additional' AS zone_rate_code, 28.3 AS price UNION ALL
  SELECT 'rto_c_fixed' AS zone_rate_code, 31.9 AS price UNION ALL
  SELECT 'rto_c_additional' AS zone_rate_code, 38.9 AS price UNION ALL
  SELECT 'rto_d_fixed' AS zone_rate_code, 41.3 AS price UNION ALL
  SELECT 'rto_d_additional' AS zone_rate_code, 44.8 AS price UNION ALL
  SELECT 'rto_e_fixed' AS zone_rate_code, 50.7 AS price UNION ALL
  SELECT 'rto_e_additional' AS zone_rate_code, 55.5 AS price
)
SELECT
  *
FROM
  improved_rate_cc
;