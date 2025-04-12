USE SFParkingMeters;
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
        MAXERRORS = 40,
        FIRSTROW = 2
    );';

    BEGIN TRY
        EXEC sp_executesql @SQL;
        PRINT 'Successfully loaded data into ' + @TableName;
    END TRY
    BEGIN CATCH
        PRINT 'Error loading data into ' + @TableName + ': ' + ERROR_MESSAGE();
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
    @FilePath = '/data/ParkingMeters/Meter_Types.csv',
    @TableName = 'MeterTypes',
    @HasIdentity = 0;

EXEC LoadCSVFile
    @FilePath = '/data/ParkingMeters/Street_Types.csv',
    @TableName = 'StreetTypes',
    @HasIdentity = 0;

EXEC LoadCSVFile
    @FilePath = '/data/ParkingMeters/Collection_Routes.csv',
    @TableName = 'CollectionRoutes',
    @HasIdentity = 0;

EXEC LoadCSVFile
    @FilePath = '/data/ParkingMeters/Parking_Zones.csv',
    @TableName = 'ParkingZones',
    @HasIdentity = 0;

EXEC LoadCSVFile
    @FilePath = '/data/ParkingMeters/Streets.csv',
    @TableName = 'Streets',
    @HasIdentity = 0;

-- Then load tables with identity columns
EXEC LoadCSVFile
    @FilePath = '/data/ParkingMeters/Locations.csv',
    @TableName = 'Locations',
    @HasIdentity = 1;

EXEC LoadCSVFile
    @FilePath = '/data/ParkingMeters/Districts.csv',
    @TableName = 'Districts',
    @HasIdentity = 1;

EXEC LoadCSVFile
    @FilePath = '/data/ParkingMeters/Vendors.csv',
    @TableName = 'Vendors',
    @HasIdentity = 1;

-- Finally load the main table with foreign keys
EXEC LoadCSVFile
    @FilePath = '/data/ParkingMeters/Parking_Meters.csv',
    @TableName = 'ParkingMeters',
    @HasIdentity = 0;

DROP PROCEDURE IF EXISTS LoadCSVFile;
GO