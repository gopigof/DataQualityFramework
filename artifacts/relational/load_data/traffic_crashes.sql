USE SFTrafficCrashes;
GO

-- Create a procedure to load a single CSV file
CREATE OR ALTER PROCEDURE LoadCSVFile
    @FilePath NVARCHAR(500),
    @TableName NVARCHAR(128),
    @HasIdentity BIT = 0
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX);

    IF @HasIdentity = 1
    BEGIN
        SET @SQL = N'SET IDENTITY_INSERT ' + @TableName + ' ON;';
        EXEC sp_executesql @SQL;
    END

    SET @SQL = N'
    BULK INSERT ' + @TableName + '
    FROM ''' + @FilePath + '''
    WITH (
        FORMAT = ''CSV'',
        ROWTERMINATOR = ''0x0a'',
        KEEPNULLS,
        TABLOCK,
        MAXERRORS = 0,
        FIRSTROW = 2
    );';

    BEGIN TRY
        EXEC sp_executesql @SQL;
        PRINT 'Successfully loaded data into ' + @TableName;
    END TRY
    BEGIN CATCH
        PRINT 'Error loading data into ' + @TableName + ': Error Code -' + CONVERT(VARCHAR, ERROR_NUMBER()) + ': '+ ERROR_MESSAGE() ;
    END CATCH

    IF @HasIdentity = 1
    BEGIN
        SET @SQL = N'SET IDENTITY_INSERT ' + @TableName + ' OFF;';
        EXEC sp_executesql @SQL;
    END
END;
GO

-- Load reference tables first (no dependencies)
EXEC LoadCSVFile
    @FilePath = '/data/TrafficCrashes/Weather.csv',
    @TableName = 'Weather',
    @HasIdentity = 1;

EXEC LoadCSVFile
    @FilePath = '/data/TrafficCrashes/Road.csv',
    @TableName = 'Road',
    @HasIdentity = 1;

EXEC LoadCSVFile
    @FilePath = '/data/TrafficCrashes/location.csv',
    @TableName = 'Location',
    @HasIdentity = 1;

EXEC LoadCSVFile
    @FilePath = '/data/TrafficCrashes/Severity.csv',
    @TableName = 'Severity',
    @HasIdentity = 1;

EXEC LoadCSVFile
    @FilePath = '/data/TrafficCrashes/Party.csv',
    @TableName = 'Party',
    @HasIdentity = 1;

-- Finally load the main Collision table with foreign keys
EXEC LoadCSVFile
    @FilePath = '/data/TrafficCrashes/Collision.csv',
    @TableName = 'Collision',
    @HasIdentity = 1;

DROP PROCEDURE IF EXISTS LoadCSVFile;
GO