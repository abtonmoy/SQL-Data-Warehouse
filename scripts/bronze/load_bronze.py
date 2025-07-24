import os
import time
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import schedule

# Load environment variables from .env file
load_dotenv()

def load_bronze():
    try:
        cnx = mysql.connector.connect(
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            host=os.getenv("MYSQL_HOST", "localhost"),
            database=os.getenv("MYSQL_DATABASE")
        )
        cursor = cnx.cursor()

        print("Loading crm_cust_info...")
        cursor.execute("ALTER TABLE bronze.crm_cust_info MODIFY cst_id INT NULL;")
        cursor.execute("TRUNCATE TABLE bronze.crm_cust_info;")
        cursor.execute("""
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

        print("Loading crm_prd_info...")
        cursor.execute("TRUNCATE TABLE bronze.crm_prd_info;")
        cursor.execute("""
            ALTER TABLE bronze.crm_prd_info
              MODIFY prd_cost INT NULL,
              MODIFY prd_start_dt DATE NULL,
              MODIFY prd_end_dt DATE NULL;
        """)
        cursor.execute("""
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

        print("Loading crm_sales_details...")
        cursor.execute("TRUNCATE TABLE bronze.crm_sales_details;")
        cursor.execute("""
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

        cnx.commit()
        cursor.close()
        cnx.close()
        print("All data loaded successfully!")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

if __name__ == "__main__":
    # Schedule the job every day at 3 AM
    schedule.every().day.at("03:00").do(load_bronze)

    print("Scheduler started. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(60)
