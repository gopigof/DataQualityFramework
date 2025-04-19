import pandas as pd
from typing import Dict

from src.core.base_normalization import BaseNormalization


class TrafficCrashesNormalization(BaseNormalization):
    def normalize(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        # Weather table
        weather: pd.DataFrame = df[['weather_1', 'weather_2']].drop_duplicates().reset_index(drop=True)
        weather['weather_id'] = weather.index + 1
        # Create mapping for foreign keys
        weather_map = dict(zip(
            weather['weather_1'].astype(str) + '-' + weather['weather_2'].astype(str),
            weather['weather_id']
        ))
        # Reorder columns to have ID first
        weather = weather[['weather_id', 'weather_1', 'weather_2']]

        # Road table
        road: pd.DataFrame = df[['road_surface', 'road_cond_1', 'road_cond_2',
                                 'lighting', 'control_device']].drop_duplicates().reset_index(drop=True)
        road['road_id'] = road.index + 1
        # Create mapping for foreign keys
        road_map = dict(zip(
            road['road_surface'].astype(str) + '-' + road['road_cond_1'].astype(str) + '-' +
            road['road_cond_2'].astype(str) + '-' + road['lighting'].astype(str) + '-' +
            road['control_device'].astype(str),
            road['road_id']
        ))
        # Reorder columns to have ID first
        road = road[['road_id', 'road_surface', 'road_cond_1', 'road_cond_2', 'lighting', 'control_device']]

        # Location table
        location: pd.DataFrame = df[['tb_latitude', 'tb_longitude', 'police_district',
                                     'intersection']].drop_duplicates().reset_index(drop=True)
        location['location_id'] = location.index + 1
        # Create mapping for foreign keys
        location_map = dict(zip(
            location['tb_latitude'].astype(str) + '-' + location['tb_longitude'].astype(str),
            location['location_id']
        ))
        # Convert intersection to boolean
        # location['is_intersection'] = location['intersection'].apply(
        #     lambda x: 1 if x and 'Intersection' in str(x) else 0
        # )
        # Remove original intersection column and reorder columns
        location = location.drop(['intersection'], axis=1)

        location.dropna(subset=["tb_latitude", "tb_longitude"], inplace=True)
        location = location[['location_id', 'tb_latitude', 'tb_longitude',
                             'police_district']]

        # Severity table
        severity: pd.DataFrame = df[
            ['collision_severity', 'number_killed', 'number_injured']].drop_duplicates().reset_index(drop=True)
        severity['severity_id'] = severity.index + 1
        # Calculate total casualties and boolean flags
        severity['total_casualties'] = severity['number_killed'].fillna(0) + severity['number_injured'].fillna(0)
        severity['has_fatality'] = (severity['number_killed'] > 0).astype(int)
        severity['has_injury'] = (severity['number_injured'] > 0).astype(int)
        # Add severity score (simple version - can be modified based on requirements)
        severity['severity_score'] = severity.apply(
            lambda x: 5 * x['number_killed'] + x['number_injured'], axis=1
        )
        # Create mapping for foreign keys
        severity_map = dict(zip(
            severity['collision_severity'].astype(str) + '-' +
            severity['number_killed'].astype(str) + '-' +
            severity['number_injured'].astype(str),
            severity['severity_id']
        ))
        # Reorder columns to have ID first
        severity = severity[['severity_id', 'collision_severity', 'number_killed', 'number_injured',
                             'total_casualties', 'has_fatality', 'has_injury', 'severity_score']]

        # Party table
        party: pd.DataFrame = df[['party_at_fault', 'party1_type', 'party1_dir_of_travel', 'party1_move_pre_acc',
                                  'party2_type', 'party2_dir_of_travel', 'party2_move_pre_acc', 'type_of_collision',
                                  'ped_action', 'point', 'mviw']].drop_duplicates().reset_index(drop=True)
        party['party_id'] = party.index + 1

        # Add boolean flags based on party types and collision information
        party['involves_pedestrian'] = party.apply(
            lambda x: 1 if ('Pedestrian' in str(x['party1_type']) or 'Pedestrian' in str(x['party2_type']) or
                            'Pedestrian' in str(x['type_of_collision']) or x[
                                'ped_action'] != 'No Pedestrian Involved') else 0,
            axis=1
        )
        party['involves_bicycle'] = party.apply(
            lambda x: 1 if ('Bicycle' in str(x['party1_type']) or 'Bicycle' in str(x['party2_type']) or
                            'Bicycle' in str(x['mviw'])) else 0,
            axis=1
        )
        party['involves_motorcycle'] = party.apply(
            lambda x: 1 if ('Motorcycle' in str(x['party1_type']) or 'Motorcycle' in str(x['party2_type']) or
                            'Motorcycle' in str(x['mviw'])) else 0,
            axis=1
        )
        party['involves_vehicle'] = party.apply(
            lambda x: 1 if ('Driver' in str(x['party1_type']) or 'Driver' in str(x['party2_type'])) else 0,
            axis=1
        )

        # Create mapping for foreign keys
        party_map = dict(zip(
            party['party_at_fault'].astype(str) + '-' +
            party['party1_type'].astype(str) + '-' +
            party['party2_type'].astype(str) + '-' +
            party['type_of_collision'].astype(str),
            party['party_id']
        ))
        # Reorder columns to have ID first
        party = party[['party_id', 'party_at_fault', 'party1_type', 'party1_dir_of_travel', 'party1_move_pre_acc',
                       'party2_type', 'party2_dir_of_travel', 'party2_move_pre_acc', 'type_of_collision',
                       'involves_pedestrian', 'involves_bicycle', 'involves_motorcycle', 'involves_vehicle',
                       'ped_action', 'point', 'mviw']]

        # Main Collision table
        collision: pd.DataFrame = df[[
            'unique_id', 'case_id_pkey', 'collision_date', 'collision_time',
            'accident_year', 'month', 'day_of_week', 'time_cat', 'hour_of_day',
        ]].copy()

        # Calculate additional time-related fields
        collision['hour_of_day'] = pd.to_datetime(collision['collision_time'], format='%H:%M:%S',
                                                  errors='coerce').dt.hour
        collision['time_of_day'] = collision['hour_of_day'].apply(
            lambda x: 'Morning' if 5 <= x < 12 else
            'Afternoon' if 12 <= x < 17 else
            'Evening' if 17 <= x < 21 else 'Night'
        )
        collision['season'] = collision['month'].apply(
            lambda x: 'Winter' if x in ['December', 'January', 'February'] else
            'Spring' if x in ['March', 'April', 'May'] else
            'Summer' if x in ['June', 'July', 'August'] else 'Fall'
        )

        # Add foreign keys
        collision['location_id'] = df.apply(
            lambda x: location_map.get(f"{x['tb_latitude']}-{x['tb_longitude']}", None),
            axis=1
        )
        collision['weather_id'] = df.apply(
            lambda x: weather_map.get(f"{x['weather_1']}-{x['weather_2']}", None),
            axis=1
        )
        collision['road_id'] = df.apply(
            lambda x: road_map.get(
                f"{x['road_surface']}-{x['road_cond_1']}-{x['road_cond_2']}-{x['lighting']}-{x['control_device']}",
                None),
            axis=1
        )
        collision['party_id'] = df.apply(
            lambda x: party_map.get(
                f"{x['party_at_fault']}-{x['party1_type']}-{x['party2_type']}-{x['type_of_collision']}", None),
            axis=1
        )
        collision['severity_id'] = df.apply(
            lambda x: severity_map.get(f"{x['collision_severity']}-{x['number_killed']}-{x['number_injured']}", None),
            axis=1
        )
        collision['hour_of_day'] = collision['hour_of_day'].astype('Int64')

        # Reorder columns to match the schema
        collision = collision[[
            'unique_id', 'case_id_pkey', 'collision_date', 'collision_time',
            'accident_year', 'month', 'day_of_week', 'time_cat', 'hour_of_day', 'time_of_day',
            'season',
            'location_id', 'weather_id', 'road_id', 'party_id', 'severity_id'
        ]]

        # Add collision_id as primary key
        collision['collision_id'] = collision.index + 1
        collision = collision[['collision_id'] + [col for col in collision.columns if col != 'collision_id']]
        # location.drop('location_id', axis=1, inplace=True)

        return {
            'collision': collision,
            'weather': weather,
            'road': road,
            'location': location,
            'severity': severity,
            'party': party
        }


if __name__ == "__main__":
    normalize_job = TrafficCrashesNormalization(
        file_path="processed_TrafficCrashes.csv",
        dataset_name="TrafficCrashes",
    )
    normalize_job.run()