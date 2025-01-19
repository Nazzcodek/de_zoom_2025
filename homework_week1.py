#!/usr/bin/env python
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def get_db_connection(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    
    return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


def load_taxi_data(engine, file, table_name):
    """Load taxi data in batches"""
    
    df_iter = pd.read_csv(file, iterator=True, chunksize=100000)
    
    # Get the first chunk
    df = next(df_iter)
    
    # Convert datetime columns
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    
    # Create table with correct schema
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    
    # Load first chunk
    df.to_sql(name=table_name, con=engine, if_exists='append')
    
    # Load remaining chunks
    while True:
        try:
            t_start = time()
            
            df = next(df_iter)
            
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            
            df.to_sql(name=table_name, con=engine, if_exists='append')
            
            t_end = time()
            
            print('inserted another chunk, took %.3f second' % (t_end - t_start))
            
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


def load_zones_data(engine, file, table_name):
    """Load zones lookup data"""
    zones_data = pd.read_csv(file)
    zones_data.to_sql(table_name, engine, if_exists='replace', index=False)
    print("Finished loading zones data")


def query_1_trip_distance_ranges(engine):
    """Count trips in different distance ranges for October 2019"""
    query = """
    SELECT 
        COUNT(CASE WHEN trip_distance <= 1 THEN 1 END) as trips_up_to_1_mile,
        COUNT(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 END) as trips_1_to_3_miles,
        COUNT(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 END) as trips_3_to_7_miles,
        COUNT(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 END) as trips_7_to_10_miles,
        COUNT(CASE WHEN trip_distance > 10 THEN 1 END) as trips_over_10_miles
    FROM green_taxi_trips
    WHERE lpep_pickup_datetime >= '2019-10-01'
    AND lpep_dropoff_datetime < '2019-11-01'
    """
    return pd.read_sql(query, engine)

def query_2_longest_trip_day(engine):
    """Find the pickup day with the longest trip distance"""
    query = """
    SELECT 
        DATE(lpep_pickup_datetime) as pickup_day,
        MAX(trip_distance) as longest_trip
    FROM green_taxi_trips
    GROUP BY DATE(lpep_pickup_datetime)
    ORDER BY longest_trip DESC
    LIMIT 1;
    """
    return pd.read_sql(query, engine)


def query_3_top_pickup_locations(engine):
    """Find pickup locations with over $13,000 in total amount for 2019-10-18"""
    query = """
    SELECT 
        z."Zone" as pickup_zone,
        SUM(total_amount) as total_amount
    FROM green_taxi_trips t
    JOIN taxi_zone_lookup z
        ON t."PULocationID" = z."LocationID" 
    WHERE DATE(lpep_pickup_datetime) = '2019-10-18'
    GROUP BY z."Zone"
    HAVING SUM(total_amount) > 13000
    ORDER BY total_amount DESC;
    """
    return pd.read_sql(query, engine)


def query_4_highest_tip_dropoff(engine):
    """Find dropoff zone with highest tip for East Harlem North pickups in October 2019"""
    query = """
    SELECT 
        dropoff."Zone" as dropoff_zone,
        MAX(tip_amount) as max_tip
    FROM green_taxi_trips t
    JOIN taxi_zone_lookup pickup
        ON t."PULocationID" = pickup."LocationID"
    JOIN taxi_zone_lookup dropoff
        ON t."DOLocationID" = dropoff."LocationID"
    WHERE 
        DATE(lpep_pickup_datetime) >= '2019-10-01'
        AND DATE(lpep_pickup_datetime) < '2019-11-01'
        AND pickup."Zone" = 'East Harlem North'
    GROUP BY dropoff."Zone"
    ORDER BY max_tip DESC
    LIMIT 1;
    """
    return pd.read_sql(query, engine)


def main(params):
    taxi_table_name = params.table_name
    zone_table_name = params.zone
    # Initialize database connection
    engine = get_db_connection(params)
    
    # Load data
    print("Loading taxi data...")
    load_taxi_data(engine, 'green_tripdata_2019-10.csv', taxi_table_name)
    
    print("\nLoading zones data...")
    load_zones_data(engine, 'taxi_zone_lookup.csv', zone_table_name)
    
    # Execute queries
    print("\n1. Trip distance ranges for October 2019:")
    print(query_1_trip_distance_ranges(engine))
    
    print("\n2. Pickup day with longest trip distance:")
    print(query_2_longest_trip_day(engine))
    
    print("\n3. Top pickup locations with over $13,000 in total amount:")
    print(query_3_top_pickup_locations(engine))
    
    print("\n4. Dropoff zone with highest tip from East Harlem North:")
    print(query_4_highest_tip_dropoff(engine))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--zone', required=True, help='name of the zone file')

    args = parser.parse_args()

    main(args)