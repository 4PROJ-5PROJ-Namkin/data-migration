DECLARE @BackupDirectory NVARCHAR(4000)
DECLARE @date VARCHAR(20)
DECLARE @path NVARCHAR(255)

EXEC master.dbo.xp_instance_regread
    N'HKEY_LOCAL_MACHINE',
    N'Software\Microsoft\MSSQLServer\MSSQLServer',
    N'BackupDirectory',
    @BackupDirectory OUTPUT,
    'no_output'

SELECT @date = REPLACE(CONVERT(VARCHAR(20), GETDATE(), 120), ':', '-')

SET @path = @BackupDirectory + N'\DWH_PRODUCTION_BACKUP_' + @date + '.bak'

BACKUP DATABASE [DWH_PRODUCTION] 
TO DISK = @path
WITH NOFORMAT, NOINIT, NAME = N'DWH_PRODUCTION_BACKUP', SKIP, NOREWIND, NOUNLOAD, STATS = 10;
