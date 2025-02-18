/*
===============================================================================
DDL Script: Create Bronze Layers
===============================================================================
Script Purpose: 
  This script creates multiple tables in the bronze schema to store customer, 
  product, and sales data from CRM and ERP systems. It assigns appropriate 
  data types and prepares the tables for data loading.
===============================================================================
*/

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_material_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE
);

CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key VARCHAR(50),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);

CREATE TABLE bronze.crm_sales_details (
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

CREATE TABLE bronze.erp_cust_az12 (
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(50)
);

CREATE TABLE bronze.erp_loc_a101 (
	cid VARCHAR(50),
	cntry VARCHAR(50)
);

CREATE TABLE bronze.erp_px_cat_g1v2 (
	ID VARCHAR(50),
	CAT VARCHAR(50),
	SUBCAT VARCHAR(50),
	MAINTENANCE VARCHAR (50)
);
