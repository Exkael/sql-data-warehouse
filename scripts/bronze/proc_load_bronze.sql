/*
==============================================================
Stored Procedure: Load Bronze layer
==============================================================
Purpose:  
  This script creates a a stored procedure called load_bronze.
  It performs the following actions:
    - Truncates the bronze tables before loading the data
    - Uses Bulk Insert command to load the data from the source CSV to the bronze tables

Parameters:
  None

Usage example:
  EXECUTE bronze.load_bronze;
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	
	DECLARE @start_time DATETIME,
			@end_time DATETIME,
			@batch_start_time DATETIME,
			@batch_end_time DATETIME

	BEGIN TRY

		SET @batch_start_time = GETDATE();

		PRINT '===================================================================';
		PRINT 'Loading bronze layer';
		PRINT '===================================================================';
	
		PRINT '-------------------------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '-------------------------------------------------------------------';

		SET @start_time = GETDATE();

		PRINT '>> Truncating table bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting data into table bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Michael Boutonnet\Downloads\Training\Data Warehouse\datasets\source_crm\cust_info.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'
		PRINT '-----------'

		SET @start_time = GETDATE();

		PRINT '>> Truncating table bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting data into table bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Michael Boutonnet\Downloads\Training\Data Warehouse\datasets\source_crm\prd_info.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'
		PRINT '-----------'

		SET @start_time = GETDATE();

		PRINT '>> Truncating table bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting data into table bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Michael Boutonnet\Downloads\Training\Data Warehouse\datasets\source_crm\sales_details.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'
		PRINT '-----------'

		PRINT '-------------------------------------------------------------------';
		PRINT 'Loading ERP tables';
		PRINT '-------------------------------------------------------------------';

		SET @start_time = GETDATE();

		PRINT '>> Truncating table bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting data into table bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Michael Boutonnet\Downloads\Training\Data Warehouse\datasets\source_erp\CUST_AZ12.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'
		PRINT '-----------'

		SET @start_time = GETDATE();

		PRINT '>> Truncating table bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting data into table bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Michael Boutonnet\Downloads\Training\Data Warehouse\datasets\source_erp\LOC_A101.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'
		PRINT '-----------'

		SET @start_time = GETDATE();

		PRINT '>> Truncating table bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting data into table bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Michael Boutonnet\Downloads\Training\Data Warehouse\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH 
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds'

		SET @batch_end_time = GETDATE();

		PRINT '===================================================================';
		PRINT 'Loading bronze layer complete';
		PRINT '>> Batch duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds'
		PRINT '==================================================================='

	END TRY
	
	BEGIN CATCH
	
		PRINT '===================================================================';
		PRINT 'An error occured during the loading of bronze layer';
		PRINT 'Error message: ' + ERROR_MESSAGE();
		PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '===================================================================';

	END CATCH
END;
