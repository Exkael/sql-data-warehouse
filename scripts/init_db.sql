/*
==============================================================
Create Database and Schemas
==============================================================
Purpose:  
  This script creates a new database named 'DataWarehouse'.
  If the database already exists, it is dropped and recreated.
  Additionally, this script creates the 3 following schemas : bronze, silver, gold

WARNING:
  Running this script will drop the entire DataWarehouse database if it already exists.
  All data will be permanently deleted. Proceed with caution and ensure you have the
  proper backups if needed.
*/

USE master;
GO

-- Drop the DataWarehouse DB if it already exists
IF EXISTS(select 1 from sys.databases where name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

-- Create the DataWarehouse DB
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
