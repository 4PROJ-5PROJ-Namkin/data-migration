CREATE DATABASE DWH_PRODUCTION;
GO

CREATE LOGIN namkin WITH PASSWORD = 'namkin';
GO

USE DWH_PRODUCTION;
GO
CREATE USER namkin FOR LOGIN namkin;
GO

EXEC sp_addrolemember 'db_datareader', 'namkin';
EXEC sp_addrolemember 'db_datawriter', 'namkin';

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES TO namkin;
GO
