/*
===============================================================================
Create Database and Schemas
===============================================================================
Script Purpose: 
  This script creates a new database called DWH and then defines 
  three schemas: bronze, silver, and gold within it. It also verifies that 
  the schemas were successfully created by querying their names.
===============================================================================
*/

-- switch to database master in order to create a database
USE master;

CREATE DATABASE DWH;

-- switch to the created database to create schemas
USE DWH;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO

-- check if the schemas were properly created
SELECT schema_name
FROM information_schema.schemata
WHERE schema_name in ('bronze', 'silver', 'gold');
