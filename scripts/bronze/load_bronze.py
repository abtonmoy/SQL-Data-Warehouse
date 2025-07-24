import os
import time
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import schedule

# Load .env variables
load_dotenv()

def timed_query(cursor, description, query):
    print(f"⏳ Starting: {description} ...")
    start = time.time()
    cursor.execute(query)
    end = time.time()
    print(f" {description} done in {round(end - start, 2)} seconds.")

def load_bronze():
    try:
        cnx = mysql.connector.connect(
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            host=os.getenv("MYSQL_HOST", "localhost"),
            database=os.getenv("MYSQL_DATABASE")
        )
        cursor = cnx.cursor()

        # === CRM CUSTOMER ===
        timed_query(cursor, "ALTER crm_cust_info", "ALTER TABLE bronze.crm_cust_info MODIFY cst_id INT NULL;")
        timed_query(cursor, "TRUNCATE crm_cust_info", "TRUNCATE TABLE bronze.crm_cust_info;")
        timed_query(cursor, "LOAD crm_cust_info", """
            LOAD DATA INFILE 'C:/projects/SQL-Data-Warehouse/datasets/source_crm/cust_info.csv'
            INTO TABLE bronze.crm_cust_info
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            IGNORE 1 ROWS
            (@cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, @cst_create_date)
            SET
              cst_id = NULLIF(@cst_id, ''),
              cst_create_date = NULLIF(TRIM(REPLACE(REPLACE(@cst_create_date, '\\r', '')), '\\n', ''));
        """)

        # === CRM PRODUCT ===
        timed_query(cursor, "TRUNCATE crm_prd_info", "TRUNCATE TABLE bronze.crm_prd_info;")
        timed_query(cursor, "ALTER crm_prd_info", """
            ALTER TABLE bronze.crm_prd_info 
              MODIFY prd_cost INT NULL,
              MODIFY prd_start_dt DATE NULL,
              MODIFY prd_end_dt DATE NULL;
        """)
        timed_query(cursor, "LOAD crm_prd_info", """
            LOAD DATA INFILE 'C:/projects/SQL-Data-Warehouse/datasets/source_crm/prd_info.csv'
            INTO TABLE bronze.crm_prd_info
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            IGNORE 1 ROWS
            (@prd_id, prd_key, prd_nm, @prd_cost, prd_line, @prd_start_dt, @prd_end_dt)
            SET
              prd_id = NULLIF(@prd_id, ''),
              prd_cost = NULLIF(@prd_cost, ''),
              prd_start_dt = NULLIF(TRIM(REPLACE(REPLACE(@prd_start_dt, '\\r', '')), '\\n', '')),
              prd_end_dt = NULLIF(TRIM(REPLACE(REPLACE(@prd_end_dt, '\\r', '')), '\\n', ''));
        """)

        # === CRM SALES ===
        timed_query(cursor, "TRUNCATE crm_sales_details", "TRUNCATE TABLE bronze.crm_sales_details;")
        timed_query(cursor, "LOAD crm_sales_details", """
            LOAD DATA INFILE 'C:/projects/SQL-Data-Warehouse/datasets/source_crm/sales_details.csv'
            INTO TABLE bronze.crm_sales_details
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            IGNORE 1 ROWS
            (@sls_ord_num, @sls_prd_key, @sls_cust_id, @sls_order_dt, @sls_ship_dt, @sls_due_dt, @sls_sales, @sls_quantity, @sls_price)
            SET
              sls_ord_num  = NULLIF(TRIM(REPLACE(REPLACE(@sls_ord_num, '\\r', '')), '\\n', '')),
              sls_prd_key  = NULLIF(TRIM(REPLACE(REPLACE(@sls_prd_key, '\\r', '')), '\\n', '')),
              sls_cust_id  = NULLIF(TRIM(REPLACE(REPLACE(@sls_cust_id, '\\r', '')), '\\n', '')),
              sls_order_dt = NULLIF(TRIM(REPLACE(REPLACE(@sls_order_dt, '\\r', '')), '\\n', '')),
              sls_ship_dt  = NULLIF(TRIM(REPLACE(REPLACE(@sls_ship_dt, '\\r', '')), '\\n', '')),
              sls_due_dt   = NULLIF(TRIM(REPLACE(REPLACE(@sls_due_dt, '\\r', '')), '\\n', '')),
              sls_sales    = NULLIF(TRIM(REPLACE(REPLACE(@sls_sales, '\\r', '')), '\\n', '')),
              sls_quantity = NULLIF(TRIM(REPLACE(REPLACE(@sls_quantity, '\\r', '')), '\\n', '')),
              sls_price    = NULLIF(TRIM(REPLACE(REPLACE(@sls_price, '\\r', '')), '\\n', ''));
        """)

        # === ERP CUSTOMER ===
        timed_query(cursor, "TRUNCATE erp_cust_az12", "TRUNCATE TABLE bronze.erp_cust_az12;")
        timed_query(cursor, "LOAD erp_cust_az12", """
            LOAD DATA INFILE 'C:/projects/SQL-Data-Warehouse/datasets/source_erp/cust_az12.csv'
            INTO TABLE bronze.erp_cust_az12
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            IGNORE 1 ROWS
            (@CID, @BDATE, @GEN)
            SET
              CID   = NULLIF(TRIM(REPLACE(REPLACE(@CID, '\\r', '')), '\\n', '')),
              BDATE = NULLIF(TRIM(REPLACE(REPLACE(@BDATE, '\\r', '')), '\\n', '')),
              GEN   = NULLIF(TRIM(REPLACE(REPLACE(@GEN, '\\r', '')), '\\n', ''));
        """)

        # === ERP LOCATION ===
        timed_query(cursor, "TRUNCATE erp_loc_a101", "TRUNCATE TABLE bronze.erp_loc_a101;")
        timed_query(cursor, "LOAD erp_loc_a101", """
            LOAD DATA INFILE 'C:/projects/SQL-Data-Warehouse/datasets/source_erp/loc_a101.csv'
            INTO TABLE bronze.erp_loc_a101
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            IGNORE 1 ROWS
            (@CID, @CNTRY)
            SET
              CID   = NULLIF(TRIM(REPLACE(REPLACE(@CID, '\\r', '')), '\\n', '')),
              CNTRY = NULLIF(TRIM(REPLACE(REPLACE(@CNTRY, '\\r', '')), '\\n', ''));
        """)

        # === ERP PX CAT ===
        timed_query(cursor, "TRUNCATE erp_px_cat_g1v2", "TRUNCATE TABLE bronze.erp_px_cat_g1v2;")
        timed_query(cursor, "LOAD erp_px_cat_g1v2", """
            LOAD DATA INFILE 'C:/projects/SQL-Data-Warehouse/datasets/source_erp/px_cat_g1v2.csv'
            INTO TABLE bronze.erp_px_cat_g1v2
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            IGNORE 1 ROWS
            (@ID, @CAT, @SUBCAT, @MAINTENANCE)
            SET
              ID          = NULLIF(TRIM(REPLACE(REPLACE(@ID, '\\r', '')), '\\n', '')),
              CAT         = NULLIF(TRIM(REPLACE(REPLACE(@CAT, '\\r', '')), '\\n', '')),
              SUBCAT      = NULLIF(TRIM(REPLACE(REPLACE(@SUBCAT, '\\r', '')), '\\n', '')),
              MAINTENANCE = NULLIF(TRIM(REPLACE(REPLACE(@MAINTENANCE, '\\r', '')), '\\n', ''));
        """)

        cnx.commit()
        cursor.close()
        cnx.close()
        print(" All CRM & ERP tables loaded successfully!")

    except mysql.connector.Error as err:
        print(f"❌ Error: {err}")

if __name__ == "__main__":
    schedule.every().day.at("03:00").do(load_bronze)
    print(" Scheduler running. Will run daily at 03:00 AM. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(60)
