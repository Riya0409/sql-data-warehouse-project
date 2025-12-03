/*
====================================================================
Quality Checks
====================================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy,
  and standarization across the 'silver' schema.It includes checks for:
  - Null or duplicate primary keys.
  - unwanted spaces in string fields.
  - Data Standarization and Normalisation
  - Invalid date ranges and orders.
  - Data Consistency between related fields.

Usage Notes:
  - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepencies found during the checks.

====================================================================
*/

----------------------------------------------------------------------
-- Table: silver.crm_cust_info
-----------------------------------------------------------------------


-- Check for NULLS or DUPLICATES in Primary Key.
SELECT cst_id,count(*) FROM silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null;

-- Check for unwanted spaces in string values.
select cst_firstname from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

-- Check for unwanted spaces in string values.
select cst_lastname from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

-- Check for unwanted spaces in string values.
select cst_marital_status from silver.crm_cust_info
where cst_marital_status != TRIM(cst_marital_status);

-- Check for unwanted spaces in string values.
select cst_gndr from silver.crm_cust_info
where cst_gndr != TRIM(cst_gndr);

-- Check the consistency of values in low cardinality columns -- limited no. of possible values.
SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info;

-- Check the consistency of values in low cardinality columns -- limited no. of possible values.
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;


-----------------------------------------------------------------------
-- Table: silver.crm_prd_info
-----------------------------------------------------------------------
-- Check for NULLS or DUPLICATES in Primary Key.
SELECT prd_id,count(*) FROM silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

-- Check for unwanted spaces in string values.
select prd_key from silver.crm_prd_info
where prd_key != TRIM(prd_key);

-- Check for unwanted spaces in string values.
select prd_nm from silver.crm_prd_info
where prd_nm != TRIM(prd_nm);

-- Check for unwanted spaces in string values.
select prd_line from silver.crm_prd_info
where prd_line != TRIM(prd_line);

select prd_cost from  silver.crm_prd_info
where prd_cost <0 or prd_cost is null;

-- Data Standarization and Consistency

select distinct prd_line From silver.crm_prd_info;

-- Check for Invalid Date Orders
-- Solution: ignore given end date and use given start date to find end date. end date = start date of next record-1
-- Always consult expert for it.

SELECT * FROM  silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-----------------------------------------------------------------------
-- Table: silver.crm_sales_details
-----------------------------------------------------------------------

select sls_ord_num from silver.crm_sales_details
where sls_ord_num != TRIM(sls_ord_num);

-- Check for unwanted spaces in string values.
select sls_prd_key from silver.crm_sales_details
where sls_prd_key != TRIM(sls_prd_key);

-- check if prd_key not present in silver.crm_prd_info --> using it will connect to silver prd_info table
select sls_prd_key from silver.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info);

select sls_cust_id from silver.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info);

-- Check for Invalid Dates


select * from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt>  sls_due_dt;

-- Check for quantity,sales and price -- interrelated

select * from  silver.crm_sales_details;

-----------------------------------------------------------------------
-- Table: silver.erp_cust_az12
-----------------------------------------------------------------------

select * from(

SELECT * FROM silver.erp_cust_az12
where CID not in (select cst_key from bronze.crm_cust_info); 

select distinct bdate from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE();

-- customer older than 100 -- bad data.

select distinct gen
from silver.erp_cust_az12;

-----------------------------------------------------------------------
-- Table: silver.erp_loc_a101
-----------------------------------------------------------------------

select distinct Cntry from silver.erp_loc_a101;
select * from silver.erp_loc_a101;

-----------------------------------------------------------------------
-- Table: silver.erp_px_cat_g1v2
-----------------------------------------------------------------------

select * from silver.erp_px_cat_g1v2 
where ID not in (select cat_id from silver.crm_prd_info);
select * from silver.crm_prd_info;

select * from  silver.erp_px_cat_g1v2 
where trim(CAT) != CAT OR TRIM(SUBCAT) != SUBCAT OR TRIM(MAINTENANCE) != MAINTENANCE;

SELECT DISTINCT CAT from  silver.erp_px_cat_g1v2 ; 
SELECT DISTINCT SUBCAT from  silver.erp_px_cat_g1v2 ; 
SELECT DISTINCT MAINTENANCE from  silver.erp_px_cat_g1v2 ; 
