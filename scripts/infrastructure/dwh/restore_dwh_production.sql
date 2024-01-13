USE master;
GO

DECLARE @BackupDirectory NVARCHAR(4000);
DECLARE @MDFLocation NVARCHAR(255);
DECLARE @LDFLocation NVARCHAR(255);
DECLARE @BackupFile NVARCHAR(255);

EXEC master.dbo.xp_instance_regread
    N'HKEY_LOCAL_MACHINE',
    N'Software\Microsoft\MSSQLServer\MSSQLServer',
    N'BackupDirectory',
    @BackupDirectory OUTPUT,
    'no_output';

SELECT @MDFLocation = physical_name
FROM sys.master_files
WHERE database_id = DB_ID(N'DWH_PRODUCTION') AND type_desc = 'ROWS';

SELECT @LDFLocation = physical_name
FROM sys.master_files
WHERE database_id = DB_ID(N'DWH_PRODUCTION') AND type_desc = 'LOG';

SET @BackupFile = @BackupDirectory + N'\DWH_PRODUCTION_BACKUP_2024-01-03 20-47-20.bak';
PRINT @BackupFile

ALTER DATABASE [DWH_PRODUCTION] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
GO

RESTORE DATABASE [DWH_PRODUCTION] 
FROM DISK = @BackupFile
WITH 
    MOVE 'DWH_PRODUCTION' TO @MDFLocation, 
    MOVE 'DWH_PRODUCTION_log' TO @LDFLocation,
    REPLACE, RECOVERY, STATS = 5;

ALTER DATABASE [DWH_PRODUCTION] SET MULTI_USER;
GO
