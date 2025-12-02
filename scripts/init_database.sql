/*
======================================================
Create Database and Schemas
======================================================

Script Purpose:
  This script creates a new database named 'DataWarehouse' after checking if it already exists.
  If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
  within the database: 'bronze', 'silver', 'gold'.

WARNING:
  Running this script will drop the entire 'DataWarehouse' database if it exists.
  All data in the database will be permanently deleted. Proceed with caution and ensure you have proper backups before running this script.

*/

=====================================================================================================================================================================
use master;
go

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases Where name= 'DataWarehouse')
Begin
    ALTER Database DateWarehouse Set Single_user with rollback immediate;
    Drop database DataWarehouse;
end;
go

-- create the 'DataWarehouse' database
create database DataWarehouse;
go

use DataWarehouse;
go
  
-- creating schemas
create schema bronze;
go
create schema silver;
go
create schema gold;
