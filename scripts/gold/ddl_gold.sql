IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_no,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    CASE 
        WHEN ci.cst_gndr IS NOT NULL THEN ci.cst_gndr -- CRM is master for gender
        ELSE ca.gen
    END AS gender,
    ci.cst_material_status AS marital_status,
    cl.cntry AS country,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 cl
    ON ci.cst_key = cl.cid;
GO
  
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt DESC, pi.prd_key ASC) AS product_key,
    pi.prd_id AS product_id,
    pi.prd_key AS product_no,
    pi.prd_nm AS product_name,
    pi.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance AS maintenance,
    pi.prd_cost AS cost,
    pi.prd_line AS product_line,
    pi.prd_start_dt AS product_start_date

FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pi.cat_id = pc.id
WHERE pi.prd_end_dt IS NULL;
GO

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO  
CREATE VIEW gold.fact_sales AS
SELECT
    cs.sls_ord_num AS order_no,
    dp.product_key,
    dc.customer_key,
    cs.sls_quantity AS quantity,
    cs.sls_price AS price,
    cs.sls_sales AS sales_amount,
    cs.sls_order_dt AS order_date,
    cs.sls_ship_dt AS shipping_date,
    cs.sls_due_dt AS due_date

FROM silver.crm_sales_details cs
LEFT JOIN gold.dim_customers dc
    ON cs.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_products dp
    ON cs.sls_prd_key = dp.product_no;
GO
