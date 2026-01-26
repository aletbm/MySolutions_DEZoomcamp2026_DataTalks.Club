import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import os
import click

@click.command()
@click.option('--year', default=2025, help='Year of the data')
@click.option('--month', default=1, help='Month of the data')
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table-trips', default='green_taxi_data', help='Target table trips name')
@click.option('--target-table-zones', default='taxi_zone_lookup', help='Target table zones name')
@click.option('--chunksize', default=100_000, help='Chunk size for reading CSV')

def run(month, year, pg_user, pg_pass, pg_host, pg_port, pg_db, target_table_trips, target_table_zones, chunksize):
    url_trips = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"
    url_zones = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

    df_trips = pd.read_parquet(url_trips)
    df_trips.to_csv("{target_table_trips}.csv", index=False)
    del df_trips

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


    df_iter_trips = pd.read_csv(
        "{target_table_trips}.csv", 
        iterator=True, 
        chunksize=chunksize)
    
    df_iter_zones = pd.read_csv(
        url_zones, 
        iterator=True, 
        chunksize=chunksize)

    first = True

    for df_chunk in tqdm(df_iter_trips):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table_trips, 
                con=engine, 
                if_exists='replace'
                )
            first = False
        df_chunk.to_sql(
            name=target_table_trips, 
            con=engine, 
            if_exists='append'
            )

    os.remove("{target_table_trips}.csv")

    first = True

    for df_chunk in tqdm(df_iter_zones):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table_zones, 
                con=engine, 
                if_exists='replace'
                )
            first = False
        df_chunk.to_sql(
            name=target_table_zones, 
            con=engine, 
            if_exists='append'
            )

if __name__ == "__main__":
    run()

#uv run python ingest_data.py --year=2025 --month=11 --pg-user=root --pg-pass=root --pg-host=localhost --pg-port=5432 --pg-db=ny_taxi --target-table-trips=green_taxi_data --target-table-zones=taxi_zone_lookup --chunksize=100000