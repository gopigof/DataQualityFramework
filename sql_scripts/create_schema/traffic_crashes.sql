CREATE DATABASE TrafficCollisionDB;
GO

USE TrafficCollisionDB;
GO

-- Crashes Table (Primary Table)
CREATE TABLE Collision (
    collision_id INT IDENTITY(1,1) PRIMARY KEY,
    collision_datetime DATETIME,
    collision_date DATE,
    collision_time TIME,
    accident_year INT,
    day_of_week VARCHAR(20),
    time_cat VARCHAR(50),
    collision_severity VARCHAR(50),
    type_of_collision VARCHAR(100),
    intersection BIT,
    point GEOGRAPHY
);
GO

-- Location Table
CREATE TABLE Location (
    location_id INT IDENTITY(1,1) PRIMARY KEY,
    collision_id INT UNIQUE,
    tb_latitude FLOAT,
    tb_longitude FLOAT,
    geocode_source VARCHAR(100),
    geocode_location VARCHAR(255),
    reporting_district VARCHAR(100),
    beat_number VARCHAR(50),
    primary_rd VARCHAR(255),
    secondary_rd VARCHAR(255),
    distance FLOAT,
    direction VARCHAR(50),
    analysis_neighborhood VARCHAR(255),
    supervisor_district VARCHAR(50),
    police_district VARCHAR(50),
    FOREIGN KEY (collision_id) REFERENCES Collision(collision_id) ON DELETE CASCADE
);
GO

-- Weather Table
CREATE TABLE Weather (
    weather_id INT IDENTITY(1,1) PRIMARY KEY,
    collision_id INT UNIQUE,
    weather_1 VARCHAR(100),
    weather_2 VARCHAR(100),
    FOREIGN KEY (collision_id) REFERENCES Collision(collision_id) ON DELETE CASCADE
);
GO

-- Road Condition Table
CREATE TABLE RoadCondition (
    road_condition_id INT IDENTITY(1,1) PRIMARY KEY,
    collision_id INT UNIQUE,
    road_surface VARCHAR(100),
    road_cond_1 VARCHAR(100),
    road_cond_2 VARCHAR(100),
    lighting VARCHAR(100),
    control_device VARCHAR(100),
    FOREIGN KEY (collision_id) REFERENCES Collision(collision_id) ON DELETE CASCADE
);
GO

-- Involved Parties Table
CREATE TABLE InvolvedParties (
    party_id INT IDENTITY(1,1) PRIMARY KEY,
    collision_id INT,
    party_at_fault INT,
    party1_type VARCHAR(100),
    party1_dir_of_travel VARCHAR(100),
    party1_move_pre_acc VARCHAR(100),
    party2_type VARCHAR(100),
    party2_dir_of_travel VARCHAR(100),
    party2_move_pre_acc VARCHAR(100),
    mviv VARCHAR(100),
    ped_action VARCHAR(100),
    FOREIGN KEY (collision_id) REFERENCES Collision(collision_id) ON DELETE CASCADE
);
GO

-- Law Enforcement & Violations Table
CREATE TABLE Violations (
    violation_id INT IDENTITY(1,1) PRIMARY KEY,
    collision_id INT,
    officer_id VARCHAR(50),
    vz_pcf_code VARCHAR(50),
    vz_pcf_group VARCHAR(100),
    vz_pcf_description VARCHAR(255),
    vz_pcf_link VARCHAR(255),
    FOREIGN KEY (collision_id) REFERENCES Collision(collision_id) ON DELETE CASCADE
);
GO

-- Injury Details Table
CREATE TABLE InjuryDetails (
    injury_id INT IDENTITY(1,1) PRIMARY KEY,
    collision_id INT UNIQUE,
    number_killed INT,
    number_injured INT,
    FOREIGN KEY (collision_id) REFERENCES Collision(collision_id) ON DELETE CASCADE
);
GO

-- Metadata Table
CREATE TABLE Metadata (
    metadata_id INT IDENTITY(1,1) PRIMARY KEY,
    collision_id INT UNIQUE,
    data_as_of DATETIME,
    data_updated_at DATETIME,
    data_loaded_at DATETIME,
    FOREIGN KEY (collision_id) REFERENCES Collision(collision_id) ON DELETE CASCADE
);
GO