/*
Create Database and Schema

=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


USE master;
GO

-- Drop and recreate the 'data_warehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'data_warehouse')
BEGIN 
    ALTER DATABASE data_warehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE data_warehouse;
END;
GO


-- Create the 'data_warehouse'
CREATE DATABASE data_warehouse;
GO

USE data_warehouse;
GO

-- Create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO