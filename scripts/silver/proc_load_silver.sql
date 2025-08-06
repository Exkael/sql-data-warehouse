/*
==============================================================
Stored Procedure: Load Silver layer
==============================================================
Purpose:  
  This script creates a a stored procedure called load_silver.
  It performs the following actions:
    - Truncates the silver tables before loading the data
    - Insert cleaned / transformed data from the bronze to the silver tables

Parameters:
  None

Usage example:
  EXECUTE silver.load_silver;
*/

Create or alter procedure silver.load_silver as

BEGIN

	DECLARE @start_time DATETIME,
			@end_time DATETIME,
			@batch_start_time DATETIME,
			@batch_end_time DATETIME

	BEGIN TRY

		SET @batch_start_time = GETDATE();

		PRINT '===================================================================';
		PRINT 'Loading silver layer';
		PRINT '===================================================================';

		PRINT '-------------------------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '-------------------------------------------------------------------';

		SET @start_time = GETDATE();

		PRINT '>> Truncating table silver.crm_cust_info';
		truncate table silver.crm_cust_info;

		PRINT '>> Inserting data into silver.crm_cust_info';
		insert into silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
		select cst_id
		, cst_key
		, trim(cst_firstname) cst_firstname
		, trim(cst_lastname) cst_lastname
		, case	when upper(trim(cst_marital_status)) = 'S' then 'Single'
				when upper(trim(cst_marital_status)) = 'M' then 'Married'
				else 'n/a'
		  end cst_marital_status
		, case	when upper(trim(cst_gndr)) = 'F' then 'Female'
				when upper(trim(cst_gndr)) = 'M' then 'Male'
				else 'n/a'
		  end cst_gndr
		, cst_create_date 
		from
		(
			select cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
			, row_number() over (partition by cst_id order by cst_create_date desc) flg_last
			from bronze.crm_cust_info
		) t
		where flg_last = 1;

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'
		PRINT '-----------'

		SET @start_time = GETDATE();

		PRINT '>> Truncating table silver.crm_prd_info';
		truncate table silver.crm_prd_info;

		PRINT '>> Inserting data into silver.crm_prd_info';
		insert into silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
		select prd_id
			, REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id
			, SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key
			, prd_nm
			, ISNULL(prd_cost, 0) as prd_cost
			, case upper(trim(prd_line)) 
					when 'M' then 'Mountain'
					when 'R' then 'Road'
					when 'S' then 'Other sales'
					when 'S' then 'Touring'
					else 'n/a'
			  end as prd_line
			, cast(prd_start_dt as DATE)
			, cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1 as DATE) as new_prd_end_dt
		from bronze.crm_prd_info;

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'
		PRINT '-----------'

		SET @start_time = GETDATE();

		PRINT '>> Truncating table silver.crm_sales_details';
		truncate table silver.crm_sales_details;

		PRINT '>> Inserting data into silver.crm_sales_details';
		insert into silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
		select sls_ord_num
			  , sls_prd_key
			  , sls_cust_id
			  , case when sls_order_dt = 0 OR len(sls_order_dt) != 8 then NULL
					 else CAST(CAST(sls_order_dt as VARCHAR) as DATE)
				end as sls_order_dt
			  , case when sls_ship_dt = 0 OR len(sls_ship_dt) != 8 then NULL
					 else CAST(CAST(sls_ship_dt as VARCHAR) as DATE)
				end as sls_ship_dt
			  , case when sls_due_dt = 0 OR len(sls_due_dt) != 8 then NULL
					 else CAST(CAST(sls_due_dt as VARCHAR) as DATE)
				end as sls_due_dt
			  , case when sls_sales is null OR sls_sales <= 0 OR sls_sales != sls_quantity * abs(sls_price)
					 then sls_quantity * abs(sls_price)
					 else sls_sales
				end as sls_sales
			  , sls_quantity
			  , case when sls_price is null OR sls_price <= 0
					 then sls_price / NULLIF(sls_quantity, 0)
					 else sls_price
				end as sls_price
		FROM bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'
		PRINT '-----------'

		SET @start_time = GETDATE();

		PRINT '>> Truncating table silver.erp_cust_az12';
		truncate table silver.erp_cust_az12;

		PRINT '>> Inserting data into silver.erp_cust_az12';
		insert into silver.erp_cust_az12 (cid, bdate, gen)
		select case when cid like 'NAS%'
					then SUBSTRING(cid, 4, LEN(cid))
					else cid
				end as cid
				, case when bdate > GETDATE()
					   then NULL
					   else bdate
				  end as bdate
				, case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
					   when upper(trim(gen)) in ('M', 'MALE') then 'Male'
					   else 'n/a'
				  end as gen
		FROM bronze.erp_cust_az12;

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'
		PRINT '-----------'

		SET @start_time = GETDATE();

		PRINT '>> Truncating table silver.erp_loc_a101';
		truncate table silver.erp_loc_a101;

		PRINT '>> Inserting data into silver.erp_loc_a101';
		insert into silver.erp_loc_a101 (cid, cntry)
		select REPLACE(cid, '-', '') as cid
				, case when trim(cntry) = 'DE' then 'Germany'
					   when trim(cntry) in ('US', 'USA') then 'United States'
					   when trim(cntry) = '' or cntry is null then 'n/a'
					   else trim(cntry)
				  end cntry
		FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'
		PRINT '-----------'

		SET @start_time = GETDATE();

		PRINT '>> Truncating table silver.erp_px_cat_g1v2';
		truncate table silver.erp_px_cat_g1v2;

		PRINT '>> Inserting data into silver.erp_px_cat_g1v2';
		insert into silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		select  id
				, cat
				, subcat
				, maintenance
		from bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'

		SET @batch_end_time = GETDATE();

		PRINT '===================================================================';
		PRINT 'Loading silver layer complete';
		PRINT '>> Batch duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds'
		PRINT '==================================================================='

	END TRY
	
	BEGIN CATCH
	
		PRINT '===================================================================';
		PRINT 'An error occured during the loading of silver layer';
		PRINT 'Error message: ' + ERROR_MESSAGE();
		PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '===================================================================';

	END CATCH

END
