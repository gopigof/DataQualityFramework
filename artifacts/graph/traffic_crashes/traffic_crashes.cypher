// Clear the database if needed
MATCH (n) DETACH DELETE n;

// Create constraints for uniqueness
CREATE CONSTRAINT crash_id IF NOT EXISTS FOR (c:Crash) REQUIRE c.unique_id IS UNIQUE;
CREATE CONSTRAINT location_id IF NOT EXISTS FOR (l:Location) REQUIRE l.location_grid IS UNIQUE;
CREATE CONSTRAINT timeframe_id IF NOT EXISTS FOR (t:TimeFrame) REQUIRE t.time_id IS UNIQUE;
CREATE CONSTRAINT party_id IF NOT EXISTS FOR (p:Party) REQUIRE p.party_id IS UNIQUE;
CREATE CONSTRAINT vehicle_id IF NOT EXISTS FOR (v:Vehicle) REQUIRE v.vehicle_id IS UNIQUE;

// Load CSV and create TimeFrame nodes
LOAD CSV WITH HEADERS FROM 'file:///sf_crashes.csv' AS row
WITH row WHERE row.unique_id IS NOT NULL
MERGE (t:TimeFrame {
    time_id: row.collision_date + '_' + row.collision_time,
    year: toInteger(row.accident_year),
    month: row.month,
    day_of_week: row.day_of_week,
    time_category: row.time_cat,
    hour_of_day: toInteger(SUBSTRING(row.collision_time, 0, 2)),
    is_weekend: CASE row.is_weekend WHEN 'True' THEN true ELSE false END,
    time_of_day: row.time_of_day,
    season: row.season
});

// Create Location nodes
LOAD CSV WITH HEADERS FROM 'file:///sf_crashes.csv' AS row
WITH row WHERE row.unique_id IS NOT NULL
MERGE (l:Location {
    location_grid: row.location_grid,
    latitude: toFloat(row.tb_latitude),
    longitude: toFloat(row.tb_longitude),
    geocode_source: row.geocode_source,
    primary_road: row.primary_rd,
    secondary_road: row.secondary_rd,
    police_district: row.police_district,
    supervisor_district: row.supervisor_district,
    analysis_neighborhood: row.analysis_neighborhood
});

// Create Crash nodes
LOAD CSV WITH HEADERS FROM 'file:///sf_crashes.csv' AS row
WITH row WHERE row.unique_id IS NOT NULL
CREATE (c:Crash {
    unique_id: toInteger(row.unique_id),
    case_id: row.case_id_pkey,
    collision_datetime: datetime(REPLACE(row.collision_datetime, ' ', 'T')),
    collision_severity: row.collision_severity,
    type_of_collision: row.type_of_collision,
    mviw: row.mviw,
    weather_1: row.weather_1,
    weather_2: row.weather_2,
    road_surface: row.road_surface,
    road_cond_1: row.road_cond_1,
    lighting: row.lighting,
    control_device: row.control_device,
    intersection: row.intersection,
    pcf_code: row.vz_pcf_code,
    pcf_description: row.vz_pcf_description,
    number_killed: toInteger(COALESCE(row.number_killed, '0')),
    number_injured: toInteger(COALESCE(row.number_injured, '0')),
    total_casualties: toInteger(COALESCE(row.total_casualties, '0')),
    has_fatality: CASE row.has_fatality WHEN 'True' THEN true ELSE false END,
    has_injury: CASE row.has_injury WHEN 'True' THEN true ELSE false END,
    severity_score: toInteger(COALESCE(row.severity_score, '0')),
    involves_pedestrian: CASE row.involves_pedestrian WHEN 'True' THEN true ELSE false END,
    involves_bicycle: CASE row.involves_bicycle WHEN 'True' THEN true ELSE false END,
    involves_motorcycle: CASE row.involves_motorcycle WHEN 'True' THEN true ELSE false END,
    involves_vehicle: CASE row.involves_vehicle WHEN 'True' THEN true ELSE false END
});

// Create Party1 nodes
LOAD CSV WITH HEADERS FROM 'file:///sf_crashes.csv' AS row
WITH row WHERE row.unique_id IS NOT NULL AND row.party1_type IS NOT NULL AND row.party1_type <> ''
MERGE (p:Party {
    party_id: row.unique_id + '_party1',
    party_type: row.party1_type,
    direction_of_travel: row.party1_dir_of_travel,
    movement_preceding_accident: row.party1_move_pre_acc
});

// Create Party2 nodes
LOAD CSV WITH HEADERS FROM 'file:///sf_crashes.csv' AS row
WITH row WHERE row.unique_id IS NOT NULL AND row.party2_type IS NOT NULL AND row.party2_type <> ''
MERGE (p:Party {
    party_id: row.unique_id + '_party2',
    party_type: row.party2_type,
    direction_of_travel: row.party2_dir_of_travel,
    movement_preceding_accident: row.party2_move_pre_acc
});

// Create Vehicle nodes for Party1
LOAD CSV WITH HEADERS FROM 'file:///sf_crashes.csv' AS row
WITH row WHERE row.unique_id IS NOT NULL AND row.party1_type = 'Driver'
MERGE (v:Vehicle {vehicle_id: row.unique_id + '_vehicle1'});

// Create Vehicle nodes for Party2
LOAD CSV WITH HEADERS FROM 'file:///sf_crashes.csv' AS row
WITH row WHERE row.unique_id IS NOT NULL AND row.party2_type = 'Driver'
MERGE (v:Vehicle {vehicle_id: row.unique_id + '_vehicle2'});

// Connect Crash to Location
//LOAD CSV WITH HEADERS FROM 'file:///sf_crashes.csv' AS row
//WITH row WHERE row.unique_id IS NOT NULL
//MATCH (c:Crash {unique_id: toInteger(row.unique_id)})
//MATCH (l:Location {location_grid: row.location_grid})
//CREATE (c)-[:OCCURRED_AT {
//    distance: CASE WHEN row.distance <> '' THEN toFloat(row.distance) ELSE null END,
//    direction: row.direction,
//    is_intersection: CASE row.is_intersection WHEN 'True' THEN true ELSE false END
//}]->(l);


// Connect Crash to TimeFrame
LOAD CSV WITH HEADERS FROM 'file:///sf_crashes.csv' AS row
WITH row WHERE row.unique_id IS NOT NULL
MATCH (c:Crash {unique_id: toInteger(row.unique_id)})
MATCH (t:TimeFrame {time_id: row.collision_date + '_' + row.collision_time})
CREATE (c)-[:HAPPENED_DURING]->(t);

// Connect Crash to Party1
LOAD CSV WITH HEADERS FROM 'file:///sf_crashes.csv' AS row
WITH row WHERE row.unique_id IS NOT NULL AND row.party1_type IS NOT NULL AND row.party1_type <> ''
MATCH (c:Crash {unique_id: toInteger(row.unique_id)})
MATCH (p:Party {party_id: row.unique_id + '_party1'})
CREATE (c)-[:INVOLVES {
    party_number: 1,
    party_at_fault: CASE WHEN row.party_at_fault = '1.0' THEN true ELSE false END
}]->(p);

// Connect Crash to Party2
LOAD CSV WITH HEADERS FROM 'file:///sf_crashes.csv' AS row
WITH row WHERE row.unique_id IS NOT NULL AND row.party2_type IS NOT NULL AND row.party2_type <> ''
MATCH (c:Crash {unique_id: toInteger(row.unique_id)})
MATCH (p:Party {party_id: row.unique_id + '_party2'})
CREATE (c)-[:INVOLVES {
    party_number: 2,
    party_at_fault: CASE WHEN row.party_at_fault = '2.0' THEN true ELSE false END
}]->(p);

// Connect Party1 to Vehicle1
LOAD CSV WITH HEADERS FROM 'file:///sf_crashes.csv' AS row
WITH row WHERE row.unique_id IS NOT NULL AND row.party1_type = 'Driver'
MATCH (p:Party {party_id: row.unique_id + '_party1'})
MATCH (v:Vehicle {vehicle_id: row.unique_id + '_vehicle1'})
CREATE (p)-[:DRIVING]->(v);

// Connect Party2 to Vehicle2
LOAD CSV WITH HEADERS FROM 'file:///sf_crashes.csv' AS row
WITH row WHERE row.unique_id IS NOT NULL AND row.party2_type = 'Driver'
MATCH (p:Party {party_id: row.unique_id + '_party2'})
MATCH (v:Vehicle {vehicle_id: row.unique_id + '_vehicle2'})
CREATE (p)-[:DRIVING]->(v);

// Create COLLIDED_WITH relationships
LOAD CSV WITH HEADERS FROM 'file:///sf_crashes.csv' AS row
WITH row WHERE row.unique_id IS NOT NULL
  AND row.party1_type IS NOT NULL AND row.party1_type <> ''
  AND row.party2_type IS NOT NULL AND row.party2_type <> ''
MATCH (p1:Party {party_id: row.unique_id + '_party1'})
MATCH (p2:Party {party_id: row.unique_id + '_party2'})
CREATE (p1)-[:COLLIDED_WITH]->(p2);

// Create indexes for better performance
CREATE INDEX crash_date_idx IF NOT EXISTS FOR (c:Crash) ON (c.collision_datetime);
CREATE INDEX location_neighborhood_idx IF NOT EXISTS FOR (l:Location) ON (l.analysis_neighborhood);
CREATE INDEX time_year_idx IF NOT EXISTS FOR (t:TimeFrame) ON (t.year);
CREATE INDEX party_type_idx IF NOT EXISTS FOR (p:Party) ON (p.party_type);