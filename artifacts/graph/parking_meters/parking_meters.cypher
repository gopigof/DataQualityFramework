// Run the next three commands first to create a new database and load the graphs separately
DROP DATABASE SFParkingMeters IF EXISTS DESTROY DATA;

CREATE DATABASE SFParkingMeters;

:USE SFParkingMeters;

// Run all the next commands in one go
// Create constraints for unique IDs
CREATE CONSTRAINT IF NOT EXISTS FOR (p:ParkingMeter) REQUIRE p.post_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (s:Street) REQUIRE s.street_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (l:Location) REQUIRE (l.name, l.type) IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (t:MeterType) REQUIRE t.type_code IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (v:MeterVendor) REQUIRE (v.name, v.model) IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (z:ParkingZone) REQUIRE z.zone_id IS UNIQUE;

// Load CSV and create ParkingMeter nodes
LOAD CSV WITH HEADERS FROM 'file:///processed_ParkingMeters.csv' AS row
// Create ParkingMeter nodes
MERGE (meter:ParkingMeter {
    post_id: row.POST_ID
})
SET meter.objectid = toInteger(row.OBJECTID),
    meter.ms_space_num = CASE WHEN row.MS_SPACE_NUM <> '' THEN toInteger(row.MS_SPACE_NUM) ELSE null END,
    meter.active_meter_flag = row.ACTIVE_METER_FLAG,
    meter.reason_code = row.REASON_CODE,
    meter.cap_color = row.CAP_COLOR,
    meter.latitude = CASE WHEN row.LATITUDE <> '' THEN toFloat(row.LATITUDE) ELSE null END,
    meter.longitude = CASE WHEN row.LONGITUDE <> '' THEN toFloat(row.LONGITUDE) ELSE null END,
    meter.work_order = row.WORK_ORDER,
    meter.collection_route = row.COLLECTION_ROUTE,
    meter.collection_subroute = row.COLLECTION_SUBROUTE,
    meter.pmr_route = row.PMR_ROUTE,
    meter.on_offstreet_type = row.ON_OFFSTREET_TYPE,
    meter.on_offstreet_type_desc = row.ON_OFFSTREET_TYPE_DESC,
    meter.meter_status = row.METER_STATUS,
    meter.is_active = CASE WHEN row.IS_ACTIVE = 'TRUE' THEN true ELSE false END,
    meter.is_temporary = CASE WHEN row.IS_TEMPORARY = 'TRUE' THEN true ELSE false END;

// Create or merge MeterType nodes and relationships
LOAD CSV WITH HEADERS FROM 'file:///processed_ParkingMeters.csv' AS row
MATCH (meter:ParkingMeter {post_id: row.POST_ID})
MERGE (type:MeterType {type_code: row.METER_TYPE})
SET type.description = row.METER_TYPE_DESC
MERGE (meter)-[:IS_TYPE]->(type);

// Create or merge MeterVendor nodes and relationships
LOAD CSV WITH HEADERS FROM 'file:///processed_ParkingMeters.csv' AS row
MATCH (meter:ParkingMeter {post_id: row.POST_ID})
WHERE row.METER_VENDOR <> '' AND row.METER_MODEL <> ''
MERGE (vendor:MeterVendor {name: row.METER_VENDOR, model: row.METER_MODEL})
MERGE (meter)-[:MANUFACTURED_BY]->(vendor);

// Create or merge Street nodes and relationships
LOAD CSV WITH HEADERS FROM 'file:///processed_ParkingMeters.csv' AS row
MATCH (meter:ParkingMeter {post_id: row.POST_ID})
WHERE row.STREET_ID <> ''
MERGE (street:Street {
    street_id: CASE WHEN row.STREET_ID <> '' THEN toFloat(row.STREET_ID) ELSE null END
})
SET street.street_name = row.STREET_NAME,
    street.street_seg_ctrln_id = CASE WHEN row.STREET_SEG_CTRLN_ID <> '' THEN toFloat(row.STREET_SEG_CTRLN_ID) ELSE null END
MERGE (meter)-[:LOCATED_ON]->(street);

// Create or merge Location nodes for neighborhoods and relationships
LOAD CSV WITH HEADERS FROM 'file:///processed_ParkingMeters.csv' AS row
MATCH (meter:ParkingMeter {post_id: row.POST_ID})
WHERE row.analysis_neighborhood <> ''
MERGE (neighborhood:Location {name: row.analysis_neighborhood, type: 'Neighborhood'})
MERGE (meter)-[:BELONGS_TO]->(neighborhood);

// Create or merge Location nodes for supervisor districts and relationships
LOAD CSV WITH HEADERS FROM 'file:///processed_ParkingMeters.csv' AS row
MATCH (meter:ParkingMeter {post_id: row.POST_ID})
WHERE row.supervisor_district <> ''
MERGE (district:Location {
    name: 'District ' + row.supervisor_district,
    type: 'SupervisorDistrict',
    supervisor_district_id: toInteger(row.supervisor_district)
})
MERGE (meter)-[:BELONGS_TO]->(district);

// Create or merge ParkingZone nodes and relationships
LOAD CSV WITH HEADERS FROM 'file:///processed_ParkingMeters.csv' AS row
MATCH (meter:ParkingMeter {post_id: row.POST_ID})
WHERE row.PARKING_ZONE <> ''
MERGE (zone:ParkingZone {zone_id: row.PARKING_ZONE})
MERGE (meter)-[:WITHIN]->(zone);

// Create relationships between streets and neighborhoods
LOAD CSV WITH HEADERS FROM 'file:///processed_ParkingMeters.csv' AS row
MATCH (street:Street {street_id: toFloat(row.STREET_ID)})
MATCH (neighborhood:Location {name: row.analysis_neighborhood, type: 'Neighborhood'})
WHERE row.analysis_neighborhood <> '' AND row.STREET_ID <> ''
MERGE (street)-[:WITHIN]->(neighborhood);

// Index creation for better performance
CREATE INDEX IF NOT EXISTS FOR (p:ParkingMeter) ON (p.latitude, p.longitude);
CREATE INDEX IF NOT EXISTS FOR (l:Location) ON (l.name);
CREATE INDEX IF NOT EXISTS FOR (s:Street) ON (s.street_name);
CREATE INDEX IF NOT EXISTS FOR (m:MeterType) ON (m.description);
CREATE INDEX IF NOT EXISTS FOR (z:ParkingZone) ON (z.zone_id);