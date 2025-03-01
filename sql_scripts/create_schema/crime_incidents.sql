CREATE DATABASE PoliceIncidentReports;
GO

USE PoliceIncidentReports;
GO

CREATE TABLE Location (
    LocationID INT IDENTITY(1,1) PRIMARY KEY,
    Address NVARCHAR(255),
    PdDistrict NVARCHAR(100),
    Latitude FLOAT,
    Longitude FLOAT,
    Location GEOGRAPHY -- SQL Server spatial data type
);
GO

-- Step 3: Create the Incident table
CREATE TABLE Incident (
    IncidentID INT IDENTITY(1,1) PRIMARY KEY,
    IncidentNum NVARCHAR(50) UNIQUE,
    IncidentCode NVARCHAR(50),
    Category NVARCHAR(100),
    Description NVARCHAR(255),
    DateTime DATETIME, -- Combined Date and Time
    DayOfWeek NVARCHAR(15),
    Resolution NVARCHAR(100),
    LocationID INT,
    FOREIGN KEY (LocationID) REFERENCES Location(LocationID)
);
GO