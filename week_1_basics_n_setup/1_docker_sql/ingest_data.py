import os
import argparse
from time import time

import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url_trip_data = params.url_1
    url_zone_data = params.url_2

    trip_csv_name = 'yellow_tripdata_2021-01.csv'
    zone_csv_name = 'taxi+_zone_lookup.csv'

    os.system(f"wget {url_trip_data} -O {trip_csv_name}")

    os.system(f"wget {url_zone_data} -O {zone_csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_zone = pd.read_csv(zone_csv_name)
    df_zone.to_sql(name='taxi_zone', con=engine, if_exists='replace')

    df_iter = pd.read_csv(trip_csv_name,
                    infer_datetime_format = True,
                    parse_dates = [1, 2],
                    iterator = True,
                    chunksize = 100000)

    df = next(df_iter)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 
        t_start = time()

        df = next(df_iter)        

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url_1', required=True, help='url of the trip csv file')
    parser.add_argument('--url_2', required=True, help='url of the zone csv file')

    args = parser.parse_args()

    main(args)

