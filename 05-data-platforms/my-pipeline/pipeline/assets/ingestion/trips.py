"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: vendor_id
    type: integer
    description: Code indicating the provider that supplied the trip record.

  - name: pickup_datetime
    type: timestamp
    description: Date and time when the meter was engaged.

  - name: dropoff_datetime
    type: timestamp
    description: Date and time when the meter was disengaged.

  - name: passenger_count
    type: integer
    description: Number of passengers in the vehicle.

  - name: trip_distance
    type: double
    description: Trip distance in miles reported by the taximeter.

  - name: ratecode_id
    type: integer
    description: Final rate code in effect at the end of the trip.

  - name: store_and_fwd_flag
    type: string
    description: Indicates if the trip was stored and forwarded.

  - name: pu_location_id
    type: integer
    description: TLC Taxi Zone where the trip started.

  - name: do_location_id
    type: integer
    description: TLC Taxi Zone where the trip ended.

  - name: payment_type
    type: integer
    description: Numeric code representing payment method.

  - name: fare_amount
    type: double
    description: Time-and-distance fare calculated by the meter.

  - name: extra
    type: double
    description: Miscellaneous extras and surcharges.

  - name: mta_tax
    type: double
    description: Automatically triggered MTA tax.

  - name: tip_amount
    type: double
    description: Credit card tip amount (cash tips not included).

  - name: tolls_amount
    type: double
    description: Total tolls paid during the trip.

  - name: improvement_surcharge
    type: double
    description: Improvement surcharge assessed at flag drop.

  - name: total_amount
    type: double
    description: Total amount charged to passengers (excluding cash tips).

  - name: congestion_surcharge
    type: double
    description: NYS congestion surcharge.

  - name: airport_fee
    type: double
    description: Airport pickup fee (Yellow only; null for Green).

  - name: trip_type
    type: integer
    description: Street-hail or dispatch indicator (Green only; null for Yellow).

  - name: cbd_congestion_fee
    type: double
    description: MTA Congestion Relief Zone fee (starting 2025).

  - name: taxi_type
    type: string
    description: Taxi type (yellow or green).

  - name: extracted_at
    type: timestamp
    description: Timestamp when the record was extracted.

@bruin"""

import os
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import BytesIO

import pandas as pd
import requests

def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]

    vars_json = os.environ.get("BRUIN_VARS", "{}")
    vars = json.loads(vars_json)
    taxi_types = vars.get("taxi_types", ["yellow", "green"])

    start = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date)

    dfs = []
    cur = start
    while cur < end:
        year = cur.year
        month = cur.month
        for taxi in taxi_types:
            url = (
                "https://d37ci6vzurychx.cloudfront.net/trip-data/"
                f"{taxi}_tripdata_{year}-{month:02}.parquet"
            )
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            df = pd.read_parquet(BytesIO(resp.content), engine="pyarrow")

            df["taxi_type"] = taxi

            if taxi == "yellow":
                df.rename(
                    columns={
                        "tpep_pickup_datetime": "pickup_datetime",
                        "tpep_dropoff_datetime": "dropoff_datetime",
                        "VendorID": "vendor_id",
                        "RatecodeID": "ratecode_id",
                        "PULocationID": "pu_location_id",
                        "DOLocationID": "do_location_id",
                        "passenger_count": "passenger_count",
                        "trip_distance": "trip_distance",
                        "payment_type": "payment_type",
                        "fare_amount": "fare_amount",
                        "extra": "extra",
                        "mta_tax": "mta_tax",
                        "tip_amount": "tip_amount",
                        "tolls_amount": "tolls_amount",
                        "improvement_surcharge": "improvement_surcharge",
                        "total_amount": "total_amount",
                        "congestion_surcharge": "congestion_surcharge",
                        "airport_fee": "airport_fee",
                    },
                    inplace=True,
                )
                if "trip_type" not in df.columns:
                    df["trip_type"] = None
            elif taxi == "green":
                df.rename(
                    columns={
                        "lpep_pickup_datetime": "pickup_datetime",
                        "lpep_dropoff_datetime": "dropoff_datetime",
                        "VendorID": "vendor_id",
                        "RatecodeID": "ratecode_id",
                        "PULocationID": "pu_location_id",
                        "DOLocationID": "do_location_id",
                        "passenger_count": "passenger_count",
                        "trip_distance": "trip_distance",
                        "payment_type": "payment_type",
                        "fare_amount": "fare_amount",
                        "extra": "extra",
                        "mta_tax": "mta_tax",
                        "tip_amount": "tip_amount",
                        "tolls_amount": "tolls_amount",
                        "improvement_surcharge": "improvement_surcharge",
                        "total_amount": "total_amount",
                        "congestion_surcharge": "congestion_surcharge",
                        "trip_type": "trip_type",
                    },
                    inplace=True,
                )
                if "airport_fee" not in df.columns:
                  if "ehail_fee" in df.columns:
                      df.rename(columns={"ehail_fee": "airport_fee"}, inplace=True)
                  else:
                      df["airport_fee"] = None

            dfs.append(df)
        cur += relativedelta(months=1)

    if dfs:
        out = pd.concat(dfs, ignore_index=True)
        out["extracted_at"] = datetime.utcnow()
    else:
        out = pd.DataFrame()
    return out

