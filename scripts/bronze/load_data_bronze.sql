-- SHOW VARIABLES LIKE 'secure_file_priv';
-- SHOW GRANTS FOR CURRENT_USER;

ALTER TABLE bronze.crm_cust_info MODIFY cst_id INT NULL;
TRUNCATE TABLE bronze.crm_cust_info;
LOAD DATA INFILE 'C:\\projects\\SQL-Data-Warehouse\\datasets\\source_crm\\cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
IGNORE 1 ROWS
(@cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, @cst_create_date)
SET
  cst_id = NULLIF(@cst_id, ''),
  cst_create_date = NULLIF(TRIM(REPLACE(REPLACE(@cst_create_date, '\r', ''), '\n', '')), '')


SELECT COUNT(*) FROM bronze.crm_cust_info;

TRUNCATE TABLE bronze.crm_prd_info;

ALTER TABLE bronze.crm_prd_info 
  MODIFY prd_cost INT NULL,
  MODIFY prd_start_dt DATE NULL,
  MODIFY prd_end_dt DATE NULL;

LOAD DATA INFILE 'C:\\projects\\SQL-Data-Warehouse\\datasets\\source_crm\\prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@prd_id, prd_key, prd_nm, @prd_cost, prd_line, @prd_start_dt, @prd_end_dt)
SET
  prd_id = NULLIF(@prd_id, ''),
  prd_cost = NULLIF(@prd_cost, ''),
  prd_start_dt = NULLIF(TRIM(REPLACE(REPLACE(@prd_start_dt, '\r', ''), '\n', '')), ''),
  prd_end_dt = NULLIF(TRIM(REPLACE(REPLACE(@prd_end_dt, '\r', ''), '\n', '')), '');


SELECT COUNT(*) FROM bronze.crm_sales_details


TRUNCATE TABLE bronze.crm_sales_details;

LOAD DATA INFILE 'C:\\projects\\SQL-Data-Warehouse\\datasets\\source_crm\\sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@sls_ord_num, @sls_prd_key, @sls_cust_id, @sls_order_dt, @sls_ship_dt, @sls_due_dt, @sls_sales, @sls_quantity, @sls_price)
SET
  sls_ord_num  = NULLIF(TRIM(REPLACE(REPLACE(@sls_ord_num, '\r', ''), '\n', '')), ''),
  sls_prd_key  = NULLIF(TRIM(REPLACE(REPLACE(@sls_prd_key, '\r', ''), '\n', '')), ''),
  sls_cust_id  = NULLIF(TRIM(REPLACE(REPLACE(@sls_cust_id, '\r', ''), '\n', '')), ''),
  sls_order_dt = NULLIF(TRIM(REPLACE(REPLACE(@sls_order_dt, '\r', ''), '\n', '')), ''),
  sls_ship_dt  = NULLIF(TRIM(REPLACE(REPLACE(@sls_ship_dt, '\r', ''), '\n', '')), ''),
  sls_due_dt   = NULLIF(TRIM(REPLACE(REPLACE(@sls_due_dt, '\r', ''), '\n', '')), ''),
  sls_sales    = NULLIF(TRIM(REPLACE(REPLACE(@sls_sales, '\r', ''), '\n', '')), ''),
  sls_quantity = NULLIF(TRIM(REPLACE(REPLACE(@sls_quantity, '\r', ''), '\n', '')), ''),
  sls_price    = NULLIF(TRIM(REPLACE(REPLACE(@sls_price, '\r', ''), '\n', '')), '');


SELECT COUNT(*) FROM bronze.crm_sales_details



TRUNCATE TABLE bronze.erp_cust_az12;

LOAD DATA INFILE 'C:\\projects\\SQL-Data-Warehouse\\datasets\\source_erp\\cust_az12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@CID,@BDATE,@GEN
)
-- SET
--   CID = NULLIF(TRIM(REPLACE(REPLACE(@CID, '\r', ''), '\n', '')), ''),
--   DATE  = NULLIF(TRIM(REPLACE(REPLACE(@DATE, '\r', ''), '\n', '')), ''),
--   GEN  = NULLIF(TRIM(REPLACE(REPLACE(@GEN, '\r', ''), '\n', '')), ''),
--   


SELECT COUNT(*) FROM bronze.erp_cust_az12;


TRUNCATE TABLE bronze.erp_loc_a101;

LOAD DATA INFILE 'C:\\projects\\SQL-Data-Warehouse\\datasets\\source_erp\\loc_a101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@CID, @CNTRY
)
-- SET
--   CID = NULLIF(TRIM(REPLACE(REPLACE(@CID, '\r', ''), '\n', '')), ''),
--   CNTRY  = NULLIF(TRIM(REPLACE(REPLACE(@CNTRY, '\r', ''), '\n', '')), ''),
--   


SELECT COUNT(*) FROM bronze.erp_loc_a101;

TRUNCATE TABLE bronze.erp_px_cat_g1v2;

LOAD DATA INFILE 'C:\\projects\\SQL-Data-Warehouse\\datasets\\source_erp\\px_cat_g1v2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@ID,@CAT,@SUBCAT,@MAINTENANCE
)
-- SET
--   ID = NULLIF(TRIM(REPLACE(REPLACE(@ID, '\r', ''), '\n', '')), ''),
--   CAT  = NULLIF(TRIM(REPLACE(REPLACE(@CAT, '\r', ''), '\n', '')), ''),
--   SUBCAT  = NULLIF(TRIM(REPLACE(REPLACE(@SUBCAT, '\r', ''), '\n', '')), ''),
--   MAINTENANCE  = NULLIF(TRIM(REPLACE(REPLACE(@MAINTENANCE, '\r', ''), '\n', '')), ''),



SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;