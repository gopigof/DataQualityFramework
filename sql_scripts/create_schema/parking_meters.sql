USE master;
GO

-- Drop database if it exists
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'SFParkingMeters')
BEGIN
    ALTER DATABASE SFParkingMeters SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE SFParkingMeters;
END
GO

CREATE DATABASE SFParkingMeters;
GO

USE SFParkingMeters;
GO

-- Geographic Locations
CREATE TABLE Locations (
    location_id INT IDENTITY(1,1) PRIMARY KEY,
    longitude FLOAT NOT NULL,
    latitude FLOAT NOT NULL,
    geo_point NVARCHAR(255) NULL,
    location_grid NVARCHAR(255) NULL
);

-- Administrative Districts
CREATE TABLE Districts (
    district_id INT IDENTITY(1,1) PRIMARY KEY,
    pm_district_id FLOAT NULL,
    analysis_neighborhood NVARCHAR(255) NULL,
    supervisor_district INT NULL,
    analysis_neighborhoods_id INT NULL,
    neighborhoods_id INT NULL,
    sf_find_neighborhoods_id INT NULL,
    current_police_districts_id INT NULL,
    current_supervisor_districts_id INT NULL
);

-- Streets
CREATE TABLE Streets (
    street_id FLOAT PRIMARY KEY,
    street_name NVARCHAR(255) NOT NULL,
    street_num FLOAT NULL,
    street_seg_ctrln_id FLOAT NULL,
    blockface_id INT NULL
);

-- Meter Types Reference
CREATE TABLE MeterTypes (
    meter_type_id NVARCHAR(50) PRIMARY KEY,
    meter_type_desc NVARCHAR(255) NOT NULL
);

-- On/Off Street Types Reference
CREATE TABLE StreetTypes (
    street_type_id NVARCHAR(50) PRIMARY KEY,
    street_type_desc NVARCHAR(255) NOT NULL
);

-- Collection Routes
CREATE TABLE CollectionRoutes (
    route_id NVARCHAR(50) PRIMARY KEY,
    collection_route_desc NVARCHAR(255) NULL,
    collection_subroute NVARCHAR(50) NULL,
    collection_subroute_desc NVARCHAR(255) NULL,
    pmr_route NVARCHAR(50) NULL
);

-- Vendors
CREATE TABLE Vendors (
    vendor_id INT IDENTITY(1,1) PRIMARY KEY,
    meter_vendor NVARCHAR(100) NOT NULL,
    meter_model NVARCHAR(100) NULL
);

-- Parking Zones
CREATE TABLE ParkingZones (
    zone_id NVARCHAR(50) PRIMARY KEY,
    jurisdiction NVARCHAR(100) NOT NULL
);

-- Main Parking Meters Table
CREATE TABLE ParkingMeters (
    object_id INT PRIMARY KEY,
    post_id NVARCHAR(50) NOT NULL,
    ms_space_num INT NULL,
    active_meter_flag NVARCHAR(10) NULL,
    reason_code NVARCHAR(50) NULL,
    cap_color NVARCHAR(50) NULL,
    work_order NVARCHAR(100) NULL,
    meter_status NVARCHAR(50) NULL,
    is_active BIT NULL,
    is_temporary BIT NULL,
    shape NVARCHAR(MAX) NULL,
    
    -- Foreign Keys
    meter_type_id NVARCHAR(50) NULL,
    street_type_id NVARCHAR(50) NULL,
    location_id INT NULL,
    district_id INT NULL,
    street_id FLOAT NULL,
    route_id NVARCHAR(50) NULL,
    vendor_id INT NULL,
    zone_id NVARCHAR(50) NULL,
    
    CONSTRAINT FK_ParkingMeters_MeterTypes FOREIGN KEY (meter_type_id) 
        REFERENCES MeterTypes(meter_type_id),
    CONSTRAINT FK_ParkingMeters_StreetTypes FOREIGN KEY (street_type_id) 
        REFERENCES StreetTypes(street_type_id),
    CONSTRAINT FK_ParkingMeters_Locations FOREIGN KEY (location_id) 
        REFERENCES Locations(location_id),
    CONSTRAINT FK_ParkingMeters_Districts FOREIGN KEY (district_id) 
        REFERENCES Districts(district_id),
    CONSTRAINT FK_ParkingMeters_Streets FOREIGN KEY (street_id) 
        REFERENCES Streets(street_id),
    CONSTRAINT FK_ParkingMeters_CollectionRoutes FOREIGN KEY (route_id) 
        REFERENCES CollectionRoutes(route_id),
    CONSTRAINT FK_ParkingMeters_Vendors FOREIGN KEY (vendor_id) 
        REFERENCES Vendors(vendor_id),
    CONSTRAINT FK_ParkingMeters_ParkingZones FOREIGN KEY (zone_id) 
        REFERENCES ParkingZones(zone_id)
);

CREATE INDEX IX_ParkingMeters_LocationId ON ParkingMeters(location_id);
CREATE INDEX IX_ParkingMeters_DistrictId ON ParkingMeters(district_id);
CREATE INDEX IX_ParkingMeters_StreetId ON ParkingMeters(street_id);
CREATE INDEX IX_ParkingMeters_MeterTypeId ON ParkingMeters(meter_type_id);
CREATE INDEX IX_ParkingMeters_StreetTypeId ON ParkingMeters(street_type_id);
CREATE INDEX IX_ParkingMeters_RouteId ON ParkingMeters(route_id);
CREATE INDEX IX_ParkingMeters_VendorId ON ParkingMeters(vendor_id);
CREATE INDEX IX_ParkingMeters_ZoneId ON ParkingMeters(zone_id);

CREATE INDEX IX_ParkingMeters_IsActive ON ParkingMeters(is_active);