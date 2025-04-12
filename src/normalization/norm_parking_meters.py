import pandas as pd
from typing import Dict

from src.core.base_normalization import BaseNormalization


class ParkingMetersNormalization(BaseNormalization):
    def normalize(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        # Vendors table
        vendors: pd.DataFrame = df[['METER_VENDOR', 'METER_MODEL']].drop_duplicates().reset_index(drop=True)
        vendors['vendor_id'] = vendors.index + 1
        vendors = vendors.rename(columns={
            'METER_VENDOR': 'meter_vendor',
            'METER_MODEL': 'meter_model'
        })
        # Reorder columns to have ID first
        vendors = vendors[['vendor_id', 'meter_vendor', 'meter_model']]

        # MeterTypes table
        meter_types: pd.DataFrame = df[['METER_TYPE', 'METER_TYPE_DESC']].drop_duplicates().reset_index(drop=True)
        meter_types['meter_type_id'] = meter_types.index + 1
        meter_type_orig_mapping = dict(zip(meter_types['METER_TYPE'], meter_types['meter_type_id']))
        meter_types = meter_types.rename(columns={
            'METER_TYPE_DESC': 'meter_type_desc'
        })
        # Remove *_orig column and reorder columns
        meter_types = meter_types[['meter_type_id', 'meter_type_desc']]

        # Locations table
        locations: pd.DataFrame = df[
            ['LONGITUDE', 'LATITUDE', 'GEO_POINT', 'LOCATION_GRID']].drop_duplicates().reset_index(drop=True)
        locations['location_id'] = locations.index + 1
        locations = locations.rename(columns={
            'LONGITUDE': 'longitude',
            'LATITUDE': 'latitude',
            'GEO_POINT': 'geo_point',
            'LOCATION_GRID': 'location_grid'
        })
        # Reorder columns to have ID first
        locations = locations[['location_id', 'longitude', 'latitude', 'geo_point', 'location_grid']]

        # Streets table
        streets: pd.DataFrame = df[['STREET_NAME', 'STREET_NUM', 'STREET_SEG_CTRLN_ID', 'BLOCKFACE_ID',
                                    'STREET_ID']].drop_duplicates().reset_index(drop=True)
        streets = streets.rename(columns={
            'STREET_NAME': 'street_name',
            'STREET_NUM': 'street_num',
            'STREET_SEG_CTRLN_ID': 'street_seg_ctrln_id',
            'BLOCKFACE_ID': 'blockface_id',
            'STREET_ID': 'street_id'
        })
        streets = streets[['street_id', 'street_name', 'street_num', 'street_seg_ctrln_id', 'blockface_id']]
        streets.dropna(subset=['street_id'], inplace=True)
        streets.drop_duplicates(subset=['street_id'], inplace=True)

        # Districts table
        districts: pd.DataFrame = df[[
            'PM_DISTRICT_ID', 'analysis_neighborhood', 'supervisor_district',
            'Analysis Neighborhoods', 'Neighborhoods', 'SF Find Neighborhoods',
            'Current Police Districts', 'Current Supervisor Districts'
        ]].drop_duplicates().reset_index(drop=True)
        districts['district_id'] = districts.index + 1
        districts = districts.rename(columns={
            'PM_DISTRICT_ID': 'pm_district_id',
            'Analysis Neighborhoods': 'analysis_neighborhoods_id',
            'Neighborhoods': 'neighborhoods_id',
            'SF Find Neighborhoods': 'sf_find_neighborhoods_id',
            'Current Police Districts': 'current_police_districts_id',
            'Current Supervisor Districts': 'current_supervisor_districts_id'
        })
        # Reorder columns to have ID first
        districts = districts[['district_id', 'pm_district_id', 'analysis_neighborhood', 'supervisor_district',
                               'analysis_neighborhoods_id', 'neighborhoods_id', 'sf_find_neighborhoods_id',
                               'current_police_districts_id', 'current_supervisor_districts_id']]

        # ParkingZones table
        parking_zones: pd.DataFrame = df[['JURISDICTION', 'PARKING_ZONE']].drop_duplicates().reset_index(drop=True)
        parking_zones['zone_id'] = parking_zones.index + 1
        zone_orig_mapping = dict(zip(parking_zones['PARKING_ZONE'], parking_zones['zone_id']))
        parking_zones = parking_zones.rename(columns={
            'JURISDICTION': 'jurisdiction',
        })
        # Remove *_orig column and reorder columns
        parking_zones = parking_zones[['zone_id', 'jurisdiction']]

        # StreetTypes table
        street_types: pd.DataFrame = df[['ON_OFFSTREET_TYPE', 'ON_OFFSTREET_TYPE_DESC']].drop_duplicates().reset_index(
            drop=True)
        street_types['street_type_id'] = street_types.index + 1
        street_type_orig_mapping = dict(zip(street_types['ON_OFFSTREET_TYPE'], street_types['street_type_id']))
        street_types = street_types.rename(columns={
            'ON_OFFSTREET_TYPE_DESC': 'street_type_desc'
        })
        # Remove *_orig column and reorder columns
        street_types = street_types[['street_type_id', 'street_type_desc']]

        # CollectionRoutes table
        collection_routes: pd.DataFrame = df[[
            'COLLECTION_ROUTE_DESC', 'COLLECTION_SUBROUTE',
            'COLLECTION_SUBROUTE_DESC', 'PMR_ROUTE',
            'COLLECTION_ROUTE'
        ]].drop_duplicates().reset_index(drop=True)
        collection_routes['route_id'] = collection_routes.index + 1
        route_orig_mapping = dict(zip(collection_routes['COLLECTION_ROUTE'], collection_routes['route_id']))
        collection_routes = collection_routes.rename(columns={
            'COLLECTION_ROUTE_DESC': 'collection_route_desc',
            'COLLECTION_SUBROUTE': 'collection_subroute',
            'COLLECTION_SUBROUTE_DESC': 'collection_subroute_desc',
            'PMR_ROUTE': 'pmr_route',
        })
        # Remove *_orig column and reorder columns
        collection_routes = collection_routes[['route_id', 'collection_route_desc',
                                               'collection_subroute', 'collection_subroute_desc', 'pmr_route']]

        # Create mapping dictionaries for foreign keys using the original columns
        vendor_map = dict(zip(
            vendors['meter_vendor'] + '-' + vendors['meter_model'],
            vendors['vendor_id']
        ))

        location_map = dict(zip(
            locations['longitude'].astype(str) + '-' + locations['latitude'].astype(str),
            locations['location_id']
        ))

        district_map = dict(zip(
            districts['pm_district_id'],
            districts['district_id']
        ))

        # Main ParkingMeters table
        parking_meters: pd.DataFrame = df[[
            'OBJECTID', 'POST_ID', 'MS_SPACE_NUM', 'ACTIVE_METER_FLAG',
            'REASON_CODE', 'CAP_COLOR', 'WORK_ORDER', 'METER_STATUS',
            'IS_ACTIVE', 'IS_TEMPORARY', 'shape'
        ]].copy()

        # Add foreign keys
        parking_meters['vendor_id'] = df.apply(
            lambda x: vendor_map.get(f"{x['METER_VENDOR']}-{x['METER_MODEL']}", None),
            axis=1
        )
        parking_meters['meter_type_id'] = df['METER_TYPE'].map(meter_type_orig_mapping)
        parking_meters['location_id'] = df.apply(
            lambda x: location_map.get(f"{x['LONGITUDE']}-{x['LATITUDE']}", None),
            axis=1
        )
        parking_meters['district_id'] = df['PM_DISTRICT_ID'].map(district_map)
        parking_meters['zone_id'] = df['PARKING_ZONE'].map(zone_orig_mapping)
        parking_meters['street_type_id'] = df['ON_OFFSTREET_TYPE'].map(street_type_orig_mapping)
        parking_meters['route_id'] = df['COLLECTION_ROUTE'].map(route_orig_mapping)
        parking_meters['street_id'] = df['STREET_ID']

        parking_meters = parking_meters.rename(columns={
            'OBJECTID': 'object_id',
            'POST_ID': 'post_id',
            'MS_SPACE_NUM': 'ms_space_num',
            'ACTIVE_METER_FLAG': 'active_meter_flag',
            'REASON_CODE': 'reason_code',
            'CAP_COLOR': 'cap_color',
            'WORK_ORDER': 'work_order',
            'METER_STATUS': 'meter_status',
            'IS_ACTIVE': 'is_active',
            'IS_TEMPORARY': 'is_temporary',
            'shape': 'shape'
        })
        parking_meters["is_active"] = parking_meters["is_active"].astype(int)
        parking_meters["is_temporary"] = parking_meters["is_temporary"].astype(int)

        # Reorder columns to have object_id first
        cols = ['object_id', 'post_id', 'ms_space_num', 'active_meter_flag', 'reason_code', 'cap_color', 'work_order',
                'meter_status', 'is_active', 'is_temporary', 'shape','meter_type_id', 'street_type_id',
                'location_id', 'district_id', 'street_id', 'route_id','vendor_id', 'zone_id']
        parking_meters = parking_meters[cols]

        return {
            'parking_meters': parking_meters,
            'vendors': vendors,
            'meter_types': meter_types,
            'locations': locations,
            'streets': streets,
            'districts': districts,
            'parking_zones': parking_zones,
            'street_types': street_types,
            'collection_routes': collection_routes
        }


if __name__ == "__main__":
    normalize_job = ParkingMetersNormalization(
        file_path="processed_ParkingMeters.csv",
        dataset_name="ParkingMeters",
    )
    normalize_job.run()