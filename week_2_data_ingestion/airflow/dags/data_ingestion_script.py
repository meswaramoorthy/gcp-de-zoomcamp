import os
from time import time

import pandas as pd
from sqlalchemy import create_engine

def my_callable_func(user, password, host, port, db, table_name, csv_file):
    print('Function Called')
    print(f'Table Name: {table_name} CSV file name: {csv_file}')

def ingest_callable_task(user, password, host, port, db, table_name, csv_file):
    print(f'Table Name: {table_name} CSV file name: {csv_file}')
    print(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print("Connection to the db established")

    t_start = time()

    df_iter = pd.read_csv(csv_file,
                    infer_datetime_format = True,
                    parse_dates = [1, 2],
                    iterator = True,
                    chunksize = 100000)
    df = next(df_iter)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')   # if table exists, drops and creates a new table
    df.to_sql(name=table_name, con=engine, if_exists='append')
    print(f'Inserted the 1st chunk of data. Took {time() - t_start} seconds')
    while True:
        t_start_1 = time()

        try:
            df = next(df_iter)
        except StopIteration:   # Catch exception when the dataframe iterator fails 
            print(f'Insertion complete. Took {time() - t_start} seconds')
            break
        
        df.to_sql(name=table_name, con=engine, if_exists='append')        
        print(f'Inserted another chunk of data. Took {time() - t_start_1} seconds')