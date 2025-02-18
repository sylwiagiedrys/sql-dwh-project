/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose: 
  This script defines a stored procedure to load data into the bronze schema 
  from CSV files, tracking load times and handling errors.

Run:
  EXEC bronze.load_bronze;

Example Output:
  ================================================
  Loading Bronze Layer..
  ================================================
  Loading CRM Tables..
  (18493 rows affected)
  (397 rows affected)
  (60398 rows affected)
  Load Time: 0 seconds
  ------------------------------------------------
  Loading ERP Tables..
  (18483 rows affected)
  (18484 rows affected)
  (37 rows affected)
  Load Time: 0 seconds
  ================================================
  Total Bronze Layer Load Time: 0 seconds
  ================================================
  Total execution time: 00:00:00.292
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT '================================================';
		PRINT 'Loading Bronze Layer..';
		PRINT '================================================';

        	SET @start_time = GETDATE();
		PRINT 'Loading CRM Tables..';
		TRUNCATE TABLE bronze.crm_cost_info;
		BULK INSERT bronze.crm_cost_info
		FROM '/var/opt/mssql/datasets/source_crm/cust_info.csv' -- on mac we need to mount our datasets inside the docker container to be able to read from it 
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A', -- mac/linux line breaks (by default SQL server expects windows CRLF)
			TABLOCK
		);
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM '/var/opt/mssql/datasets/source_crm/prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A',
			TABLOCK
		);
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM '/var/opt/mssql/datasets/source_crm/sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
        	PRINT '------------------------------------------------';       
		SET @start_time = GETDATE();
		PRINT 'Loading ERP Tables..';
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM '/var/opt/mssql/datasets/source_erp/cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A',
			TABLOCK
		);
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM '/var/opt/mssql/datasets/source_erp/loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A',
			TABLOCK
		);
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM '/var/opt/mssql/datasets/source_erp/px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
    
    		PRINT '================================================';
		SET @batch_end_time = GETDATE();
		PRINT 'Total Bronze Layer Load Time: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds';
    		PRINT '================================================';

	END TRY
	BEGIN CATCH
		PRINT 'Error occured during loading bronze layer';
		PRINT 'Error message ' + ERROR_MESSAGE();
		PRINT 'Error message ' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error message ' + CAST(ERROR_STATE() AS VARCHAR);
	END CATCH
END
