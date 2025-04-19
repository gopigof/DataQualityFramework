USE master;
GO

-- Drop database if it exists
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'SFTrafficCrashes')
BEGIN
    ALTER DATABASE SFTrafficCrashes SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE SFTrafficCrashes;
END
GO

CREATE DATABASE SFTrafficCrashes;
GO

USE SFTrafficCrashes;
GO


-- Weather conditions table
CREATE TABLE TC_Weather (
    weather_id INT IDENTITY(1,1) PRIMARY KEY,
    weather_1 VARCHAR(50),
    weather_2 VARCHAR(50)
);

-- Road conditions table
CREATE TABLE TC_Road (
    road_id INT IDENTITY(1,1) PRIMARY KEY,
    road_surface VARCHAR(50),
    road_cond_1 VARCHAR(50),
    road_cond_2 VARCHAR(50),
    lighting VARCHAR(50),
    control_device VARCHAR(50)
);

-- Location table
CREATE TABLE TC_Location (
    location_id INT IDENTITY(1,1) PRIMARY KEY,
    tb_latitude VARCHAR(25),
    tb_longitude VARCHAR(25),
--     geocode_source VARCHAR(50),
--     geocode_location VARCHAR(50),
--     primary_rd VARCHAR(100),
--     secondary_rd VARCHAR(100) NULL,
--     distance DECIMAL(10, 1),
--     direction VARCHAR(10),
--     analysis_neighborhood VARCHAR(100),
--     supervisor_district INT NULL,
    police_district VARCHAR(50),
--     location_grid VARCHAR(20),
--     geo_point VARCHAR(100),
--     is_intersection BIT
);

-- Severity table
CREATE TABLE TC_Severity (
    severity_id INT IDENTITY(1,1) PRIMARY KEY,
    collision_severity VARCHAR(50),
    number_killed DECIMAL(5, 1),
    number_injured DECIMAL(5, 1),
    total_casualties DECIMAL(5, 1),
    has_fatality BIT,
    has_injury BIT,
    severity_score DECIMAL(5, 1)
);

-- Party table (for involved parties)
CREATE TABLE TC_Party (
    party_id INT IDENTITY(1,1) PRIMARY KEY,
    party_at_fault VARCHAR(10),
    party1_type VARCHAR(50),
    party1_dir_of_travel VARCHAR(50),
    party1_move_pre_acc VARCHAR(100),
    party2_type VARCHAR(50),
    party2_dir_of_travel VARCHAR(50),
    party2_move_pre_acc VARCHAR(100),
    type_of_collision VARCHAR(50),
    involves_pedestrian BIT,
    involves_bicycle BIT,
    involves_motorcycle BIT,
    involves_vehicle BIT,
    ped_action VARCHAR(100),
    point VARCHAR(100),
    mviw VARCHAR(50)
);

-- Primary Collision table
CREATE TABLE TC_Collision (
    collision_id INT IDENTITY(1,1) PRIMARY KEY,
    unique_id INT,
    case_id_pkey DECIMAL(10, 1),
--     collision_datetime DATETIME,
    collision_date VARCHAR(50),
    collision_time VARCHAR(50),
    accident_year INT,
    month VARCHAR(20),
    day_of_week VARCHAR(20),
    time_cat VARCHAR(50),
    hour_of_day INT,
    time_of_day VARCHAR(50),
    season VARCHAR(20),
--     juris VARCHAR(10),
--     officer_id VARCHAR(50),
--     reporting_district VARCHAR(50),
--     beat_number VARCHAR(50),
--     vz_pcf_code VARCHAR(50),
--     vz_pcf_group VARCHAR(50),
--     vz_pcf_description VARCHAR(100),
--     dph_col_grp VARCHAR(5),
--     dph_col_grp_description VARCHAR(100),
    location_id INT FOREIGN KEY REFERENCES TC_Location(location_id),
    weather_id INT FOREIGN KEY REFERENCES TC_Weather(weather_id),
    road_id INT FOREIGN KEY REFERENCES TC_Road(road_id),
    party_id INT FOREIGN KEY REFERENCES TC_Party(party_id),
    severity_id INT FOREIGN KEY REFERENCES TC_Severity(severity_id)
);

CREATE INDEX idx_collision_year ON TC_Collision(accident_year);
-- CREATE INDEX idx_collision_datetime ON Collision(collision_datetime);
CREATE INDEX idx_location_coords ON TC_Location(tb_latitude, tb_longitude);
-- CREATE INDEX idx_location_district ON Location(supervisor_district);
CREATE INDEX idx_severity_has_injury ON TC_Severity(has_injury);
CREATE INDEX idx_severity_has_fatality ON TC_Severity(has_fatality);
CREATE INDEX idx_party_involves_pedestrian ON TC_Party(involves_pedestrian);
CREATE INDEX idx_party_involves_bicycle ON TC_Party(involves_bicycle);