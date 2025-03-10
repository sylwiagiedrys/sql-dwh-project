/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose: 
  This script defines a stored procedure to extract, transform and load data 
  from bronze raw schema into the silver schema, tracking load times and 
  handling errors.

Run:
  EXEC silver.load_silver;

Example Output:
Loading Silver Layer..
================================================
Loading CRM Tables..
(18484 rows affected)
(397 rows affected)
(60398 rows affected)
Load Time: 1 seconds
------------------------------------------------
Loading ERP Tables..
(18483 rows affected)
(18484 rows affected)
(37 rows affected)
Load Time: 0 seconds
================================================
Total Silver Layer Load Time: 1 seconds
================================================
Total execution time: 00:00:00.465
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();

		PRINT '================================================';
		PRINT 'Loading Silver Layer..';
		PRINT '================================================';

    SET @start_time = GETDATE();
		PRINT 'Loading CRM Tables..';
    TRUNCATE TABLE silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info(
      cst_id,
      cst_key,
      cst_firstname,
      cst_lastname,
      cst_material_status,
      cst_gndr,
      cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE
            WHEN UPPER(cst_material_status) = 'M' THEN 'Married'
            WHEN UPPER(cst_material_status) = 'S' THEN 'Single'
        END AS cst_material_status,
        cst_gndr AS cst_gndr,
        cst_create_date
        
    FROM(
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY cst_id order by cst_create_date desc) as rn
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) s WHERE rn = 1;

    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info(
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
        REPLACE(LEFT(prd_key, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
        prd_nm,
        ISNULL(prd_cost, 0) AS prd_cost,
        CASE UPPER(prd_line)
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'T' THEN 'Touring'
            WHEN 'S' THEN 'Other Sales'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
        
    FROM bronze.crm_prd_info;

    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE 
            WHEN LEN(sls_order_dt) != 8 THEN NULL ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
        END AS sls_order_dt,
        CASE 
            WHEN LEN(sls_ship_dt) != 8 THEN NULL ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
        END AS sls_ship_dt,
        CASE 
            WHEN LEN(sls_due_dt) != 8 THEN NULL ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
        END AS sls_due_dt,
            CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales END AS sls_sales,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price END AS sls_price

    FROM bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT 'Load Time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
        	PRINT '------------------------------------------------';       
		SET @start_time = GETDATE();
		PRINT 'Loading ERP Tables..';

    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12(
        cid,
        bdate,
        gen
    )
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END as cid,
        CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
        CASE 
            WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(10), ''), CHAR(13), ''))) IN ('F', 'FEMALE') THEN 'F'
            WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(10), ''), CHAR(13), ''))) IN ('M', 'MALE') THEN 'M'
        ELSE NULL END AS gen

    FROM bronze.erp_cust_az12;

    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101(
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE 
            WHEN (TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), ''))) = 'DE' THEN 'Germany'
            WHEN (TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), ''))) IN ('US', 'USA') THEN 'United States'
            WHEN (TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), ''))) = '' THEN NULL
            ELSE (TRIM(REPLACE(REPLACE(cntry, CHAR(10), ''), CHAR(13), ''))) 
        END AS cntry

    FROM bronze.erp_loc_a101;

    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2(
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT 
        id,
        cat,
        subcat,
        (TRIM(REPLACE(REPLACE(maintenance, CHAR(10), ''), CHAR(13), ''))) AS maintenance

    FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT 'Load Time: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds';
    
    PRINT '================================================';
		SET @batch_end_time = GETDATE();
		PRINT 'Total Silver Layer Load Time: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds';
    PRINT '================================================';

	END TRY
	BEGIN CATCH
		PRINT 'Error occured during loading silver layer';
		PRINT 'Error message ' + ERROR_MESSAGE();
		PRINT 'Error message ' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error message ' + CAST(ERROR_STATE() AS VARCHAR);
	END CATCH
END
