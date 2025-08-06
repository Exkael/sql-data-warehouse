/*
==============================================================
DDL Script : Create Gold Views
==============================================================
Purpose:  
  This script creates the views of the gold Schema.
  The gold layer represents the final dimension and fact tables (Star Schema)

  Each view erforms transformations and combines data from the Silver layer
  to produce a clean, enriched and business-ready dataset.

Usage:
  These views can ce queried directly for analytics and reporting

*/

create view gold.dim_customer as
SELECT row_number() over (order by ci.cst_id) as customer_key
      , ci.cst_id as customer_id
      , ci.cst_key as customer_number
      , ci.cst_firstname as first_name
      , ci.cst_lastname as last_name
      , la.cntry as country
      , ci.cst_marital_status as marital_status
      , case when ci.cst_gndr != 'n/a' then ci.cst_gndr
             else coalesce(ca.gen, 'n/a')
        end as gender
      , ca.bdate as birthdate
      , ci.cst_create_date as create_date
FROM  silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca 
    on ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    on ci.cst_key = la.cid;

create view gold.dim_products as
SELECT row_number() over (order by pn.prd_start_dt, pn.prd_key) as product_key
      , pn.prd_id as product_id
      , pn.prd_key as product_number
      , pn.prd_nm as product_name
      , pn.cat_id as category_id
      , pc.cat as category
      , pc.subcat as sub_categody
      , pc.maintenance
      , pn.prd_cost as cost
      , pn.prd_line as product_line
      , pn.prd_start_dt
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    on pn.cat_id = pc.id
where prd_end_dt is null; -- filter historical data

create view gold.fact_sales as
SELECT  sd.sls_ord_num as order_nuber
      , pr.product_key
      , cu.customer_key
      , sd.sls_order_dt as oder_date
      , sd.sls_ship_dt as shipping_date
      , sd.sls_due_dt as due_date
      , sd.sls_sales as sales_amount
      , sd.sls_quantity as quantity
      , sd.sls_price as price
FROM  silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    on sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customer cu
    on sd.sls_cust_id = cu.customer_id
