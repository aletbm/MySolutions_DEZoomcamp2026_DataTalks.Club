/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# staging.trips is a cleaned/normalized version of the raw taxi trips data.
name: staging.trips
# this asset is defined as plain SQL executed on DuckDB
type: duckdb.sql

depends:
  - ingestion.trips   # raw data pulled from external source
  - ingestion.payment_lookup  # lookup table for payment types

# incremental strategy run monthly using the pickup timestamp
# note: DuckDB only accepts 'date' or 'timestamp' here; we treat each
# run's start/end dates as month boundaries so data rolls up monthly.
materialization:
  type: table
  strategy: time_interval
  incremental_key: tpep_pickup_datetime   # use pickup as the partition key
  time_granularity: date                 # monthly intervals controlled by run window

# define columns in the staging layer so quality checks and lineage are explicit
columns:
  - name: vendor_id
    type: integer
    description: Taxi vendor legacy identifier
    nullable: false
    checks:
      - name: not_null
  - name: tpep_pickup_datetime
    type: timestamp
    description: pickup time (used as the incremental key)
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: tpep_dropoff_datetime
    type: timestamp
    description: dropoff time
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    description: number of passengers
    checks:
      - name: not_null
      - name: non_negative
  - name: trip_distance
    type: float
    description: trip distance in miles
    checks:
      - name: non_negative
  - name: ratecode_id
    type: integer
    description: rate code identifier
  - name: store_and_fwd_flag
    type: string
    description: flag indicating whether the trip record was held in vehicle memory before sending to the vendor
  - name: pu_location_id
    type: integer
    description: pickup location zone ID
  - name: do_location_id
    type: integer
    description: dropoff location zone ID
  - name: payment_type
    type: integer
    description: payment type code
  - name: payment_type_name
    type: string
    description: lookup value from payment_lookup table
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    checks:
      - name: non_negative
  - name: extra
    type: float
  - name: mta_tax
    type: float
  - name: tip_amount
    type: float
    checks:
      - name: non_negative
  - name: tolls_amount
    type: float
    checks:
      - name: non_negative
  - name: improvement_surcharge
    type: float
  - name: total_amount
    type: float
    checks:
      - name: non_negative
  - name: congestion_surcharge
    type: float
  - name: airport_fee
    type: float
  - name: taxi_type
    type: string
    description: color/type of taxi
  - name: extracted_at
    type: timestamp
    description: ingestion timestamp
  - name: lpep_pickup_datetime
    type: timestamp
    description: location pickup (for green taxis)
  - name: lpep_dropoff_datetime
    type: timestamp
    description: location dropoff (for green taxis)
  - name: trip_type
    type: integer
    description: green taxi trip type indicator

# a simple invariant: we should always have at least one row in the staging table
custom_checks:
  - name: row_count_positive
    description: Ensure the table is not empty
    query: SELECT COUNT(*) > 0 FROM staging.trips
    value: 1

@bruin */

-- staging query applies normalization, deduplication, and joins
-- filter by the same window used by the time_interval materialization
WITH base AS (
    SELECT
        t.*,
        pl.payment_type_name
    FROM ingestion.trips AS t
    LEFT JOIN ingestion.payment_lookup AS pl
        ON t.payment_type = pl.payment_type_id
    WHERE t.tpep_pickup_datetime >= '{{ start_datetime }}'
      AND t.tpep_pickup_datetime < '{{ end_datetime }}'
      -- filter out obvious bad data according to our quality rules
      AND t.passenger_count IS NOT NULL
      AND t.passenger_count >= 0
      AND t.fare_amount >= 0
      AND t.tip_amount >= 0
      AND t.tolls_amount >= 0
      AND t.total_amount >= 0
),
-- drop duplicates based on all trip fields (ignore extracted_at since it varies)
dedup AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY
                       vendor_id,
                       tpep_pickup_datetime,
                       tpep_dropoff_datetime,
                       passenger_count,
                       trip_distance,
                       ratecode_id,
                       store_and_fwd_flag,
                       pu_location_id,
                       do_location_id,
                       payment_type,
                       fare_amount,
                       extra,
                       mta_tax,
                       tip_amount,
                       tolls_amount,
                       improvement_surcharge,
                       total_amount,
                       congestion_surcharge,
                       airport_fee,
                       taxi_type,
                       lpep_pickup_datetime,
                       lpep_dropoff_datetime,
                       trip_type
                   ORDER BY 1
               ) AS rn
        FROM base
    )
    WHERE rn = 1
)

SELECT *
FROM dedup;

-- bruin run my-pipeline/pipeline/assets/staging/trips.sql --config-file "./my-pipeline/.bruin.yml" --start-date 2022-01-01 --end-date   2022-02-01 --workers 1