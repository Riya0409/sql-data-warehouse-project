/*
=========================================================================
Stored Procedure: Load silver Layer (Source -> silver)
=========================================================================
Script Purpose:
  This stored procedure performs the ETL (Extract, Transform, Load) process to
  populate the 'silver' schemas tables from 'bronze' tables.
  It performs the following actions:
    - Truncates the silver tables before loading data.
    - Inserts transformed and cleansed data from 'Bronze' into 'Silver' tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC silver.load_silver;

=====================================================================================
*/

create or ALTER PROCEDURE silver.load_silver AS 
BEGIN
	DECLARE @starttime DATETIME, @endtime DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time= getdate()

		PRINT '=========================='
		PRINT 'lOADING SILVER LAYER'
		PRINT '=========================='
		-----------------------------------------------------------------------
		-- Table: bronze.crm_cust_info
		-----------------------------------------------------------------------
		SET @starttime = GETDATE()
		PRINT '>> Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Insrting Data into: silver.crm_cust_info';

		INSERT INTO silver.crm_cust_info(
		cst_id,	
		cst_key, 
		cst_firstname, 
		cst_lastname,
		cst_marital_status , 
		cst_gndr ,
		cst_create_date 
		)


		SELECT 
		cst_id,	
		cst_key	, 
		TRIM(cst_firstname) AS cst_firstname , 
		TRIM(cst_lastname) AS cst_lastname ,
		case when UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			 when UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 else 'n/a'
		end as cst_marital_status,
		case when UPPER(TRIM(cst_gndr))= 'F' then 'Female' 
			 when UPPER(TRIM(cst_gndr))= 'M' then 'Male'
			 else 'n/a'
		end as cst_gndr,
		cst_create_date 
		FROM (
			SELECT *, row_number() over(partition by cst_id order by cst_create_date desc) as rn FROM bronze.crm_cust_info)A
		WHERE rn=1 and cst_id is not null;
		SET @endtime = GETDATE()
		PRINT('>> LOAD TIME '+CAST(DATEDIFF(SECOND,@starttime,@endtime) as NVARCHAR) + ' seconds')
		print('-----------------------------------------------')
		-----------------------------------------------------------------------
		-- Table: silver.crm_prd_info
		-----------------------------------------------------------------------

		SET @starttime= GETDATE()
		PRINT '>> Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data into: silver.crm_prd_info';

		Insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt

		)

		SELECT 
		prd_id,
		Replace(SUBSTRING(prd_key,1, 5),'-','_') as cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
		prd_nm	,
		isnull(prd_cost,0) as prd_cost ,
		case when Upper(trim(prd_line))= 'M' then 'Mountain'
			 when Upper(trim(prd_line))= 'R' then 'Road'
			 when Upper(trim(prd_line))= 'S' then 'Other Sales'
			 when Upper(trim(prd_line))= 'T' then 'Touring'
			 else 'n/a'
		end as prd_line,
		cast(prd_start_dt as Date) as prd_start_dt,	
		cast(Lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as Date) as prd_end_dt
		FROM bronze.crm_prd_info;

		SET @endtime= GETDATE()
		PRINT('>> LOAD TIME '+CAST(DATEDIFF(SECOND,@starttime,@endtime) as NVARCHAR) + ' seconds')
		print('-----------------------------------------------')
		-----------------------------------------------------------------------
		-- Table: silver.crm_sales_details
		-----------------------------------------------------------------------

		SET @starttime= GETDATE()
		PRINT '>> Truncating: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,	
			sls_prd_key	,
			sls_cust_id	,
			sls_order_dt ,	
			sls_ship_dt	,
			sls_due_dt	,
			sls_sales ,
			sls_quantity ,
			sls_price 

		)



		Select
		sls_ord_num ,	
		sls_prd_key	,
		sls_cust_id	,
		case when sls_order_dt = 0 or len(sls_order_dt) != 8 then Null
			else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt,
		case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then Null
			else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt,
		case when sls_due_dt = 0 or len(sls_due_dt) != 8 then Null
			else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt,
		case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
				then sls_quantity*ABS(sls_price)
			else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <= 0 
				then sls_sales/nullif(sls_quantity,0)
			 else sls_price
		end as sls_price
		From bronze.crm_sales_details;
		SET @endtime = GETDATE()
		PRINT('>> LOAD TIME '+CAST(DATEDIFF(SECOND,@starttime,@endtime) as NVARCHAR) + ' seconds')
		print('-----------------------------------------------')
		-----------------------------------------------------------------------
		-- Table: silver.erp_cust_az12
		-----------------------------------------------------------------------

		SET @starttime = GETDATE()
		PRINT '>> Truncating Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
		CID,
		BDATE,
		GEN

		)



		SELECT 
		case when CID LIKE 'NAS%' then SUBSTRING(CID,4,len(CID))
			 else CID 
		end as CID,
		case when bdate > GETDATE() then Null
			 else BDATE
		end as BDATE,
		case when upper(Trim(gen)) in ('F', 'FEMALE') then 'Female'
			 when upper(Trim(gen)) in('M', 'MALE') then 'Male'
			 else 'n/a'
		end as GEN
		FROM bronze.erp_cust_az12;

		SET @endtime= GETDATE()
		PRINT('>> LOAD TIME '+CAST(DATEDIFF(SECOND,@starttime,@endtime) as NVARCHAR) + ' seconds')
		print('-----------------------------------------------')
		-----------------------------------------------------------------------
		-- Table: silver.erp_loc_a101
		-----------------------------------------------------------------------
		SET @starttime= GETDATE()
		PRINT '>> Truncating: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data into: silver.erp_loc_a101';

		Insert into silver.erp_loc_a101(
		CID,
		CNTRY
		)


		select 
		Replace(CID,'-','') as CID,
		case when upper(trim(CNTRY)) in ('USA', 'US', 'UNITED STATES') THEN 'USA'
			  when upper(trim(CNTRY)) in ('DE', 'GGERMANY') THEN 'Germany'
			  when CNTRY is null or CNTRY = ' ' then 'n/a'
			  else CNTRY
		end as CNTRY
		from bronze.erp_loc_a101;
		SET @endtime= GETDATE()
		PRINT('>> LOAD TIME '+CAST(DATEDIFF(SECOND,@starttime,@endtime) as NVARCHAR) + ' seconds')
		print('-----------------------------------------------')
		-----------------------------------------------------------------------
		-- Table: silver.erp_px_cat_g1v2
		-----------------------------------------------------------------------
		SET @starttime= GETDATE()
		PRINT '>> Truncating: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data into: silver.erp_px_cat_g1v2';

		INSERT INTO silver.erp_px_cat_g1v2(
		ID,CAT,SUBCAT,MAINTENANCE
		)

		SELECT ID,CAT,SUBCAT,MAINTENANCE 
		FROM bronze.erp_px_cat_g1v2;
		SET @endtime= GETDATE()
		PRINT('>> LOAD TIME '+CAST(DATEDIFF(SECOND,@starttime,@endtime) as NVARCHAR) + ' seconds')
		print('-----------------------------------------------')
		SET @batch_end_time = GETDATE()
		Print('Loading Silver Layer is completed')
		PRINT('               - Total Duration: '+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) as NVARCHAR) + ' seconds')
		print('-----------------------------------------------')
	END TRY
	BEGIN CATCH
			PRINT '=========================='
			PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
			PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
			PRINT 'ERROR NUMBER'+ CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT '=========================='
	END CATCH
END
