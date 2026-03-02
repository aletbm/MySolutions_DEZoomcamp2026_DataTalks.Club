/* @bruin

name: ny_taxi.staging_trips
type: bq.sql

depends:
  - ny_taxi.ingestion_trips
  - ny_taxi.ingestion_payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: date

columns:
  - name: vendor_id
    type: integer
    description: Taxi vendor legacy identifier
    nullable: false
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: Pickup time (used as the incremental key)
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Dropoff time
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    description: Number of passengers
    checks:
      - name: not_null
      - name: non_negative
  - name: trip_distance
    type: float
    description: Trip distance in miles
    checks:
      - name: non_negative
  - name: ratecode_id
    type: integer
    description: Rate code identifier
  - name: store_and_fwd_flag
    type: string
    description: Flag indicating whether the trip record was held in vehicle memory before sending to the vendor
  - name: pu_location_id
    type: integer
    description: Pickup location zone ID
  - name: do_location_id
    type: integer
    description: Dropoff location zone ID
  - name: payment_type
    type: integer
    description: Payment type code
  - name: payment_type_name
    type: string
    description: Lookup value from payment_lookup table
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
  - name: trip_type
    type: integer
    description: Trip type indicator (Green taxis only; null for Yellow)
  - name: taxi_type
    type: string
    description: Color/type of taxi (yellow or green)
  - name: extracted_at
    type: timestamp
    description: Ingestion timestamp

custom_checks:
  - name: row_count_positive
    description: Ensure the table is not empty
    query: SELECT COUNT(*) > 0 FROM {{ var.staging_dataset }}.staging_trips
    value: 1

@bruin */

WITH base AS (
    SELECT
        t.vendor_id,
        t.pickup_datetime,
        t.dropoff_datetime,
        t.passenger_count,
        t.trip_distance,
        t.ratecode_id,
        t.store_and_fwd_flag,
        t.pu_location_id,
        t.do_location_id,
        t.payment_type,
        pl.payment_type_name,
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.total_amount,
        t.congestion_surcharge,
        t.airport_fee,
        t.trip_type,
        t.taxi_type,
        t.extracted_at,
        -- string versions of float columns for BigQuery PARTITION BY compatibility
        CAST(t.trip_distance AS STRING)        AS trip_distance_str,
        CAST(t.fare_amount AS STRING)          AS fare_amount_str,
        CAST(t.extra AS STRING)                AS extra_str,
        CAST(t.mta_tax AS STRING)              AS mta_tax_str,
        CAST(t.tip_amount AS STRING)           AS tip_amount_str,
        CAST(t.tolls_amount AS STRING)         AS tolls_amount_str,
        CAST(t.improvement_surcharge AS STRING) AS improvement_surcharge_str,
        CAST(t.total_amount AS STRING)         AS total_amount_str,
        CAST(t.congestion_surcharge AS STRING) AS congestion_surcharge_str,
        CAST(t.airport_fee AS STRING)          AS airport_fee_str
    FROM {{ var.ingestion_dataset }}.ingestion_trips AS t
    LEFT JOIN {{ var.ingestion_dataset }}.ingestion_payment_lookup AS pl
        ON t.payment_type = pl.payment_type_id
    WHERE t.pickup_datetime >= '{{ start_datetime }}'
      AND t.pickup_datetime < '{{ end_datetime }}'
      AND t.passenger_count IS NOT NULL
      AND t.passenger_count >= 0
      AND t.fare_amount >= 0
      AND t.tip_amount >= 0
      AND t.tolls_amount >= 0
      AND t.total_amount >= 0
),
dedup AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY
                       vendor_id,
                       pickup_datetime,
                       dropoff_datetime,
                       passenger_count,
                       trip_distance_str,
                       ratecode_id,
                       store_and_fwd_flag,
                       pu_location_id,
                       do_location_id,
                       payment_type,
                       fare_amount_str,
                       extra_str,
                       mta_tax_str,
                       tip_amount_str,
                       tolls_amount_str,
                       improvement_surcharge_str,
                       total_amount_str,
                       congestion_surcharge_str,
                       airport_fee_str,
                       trip_type,
                       taxi_type
                   ORDER BY extracted_at DESC
               ) AS rn
        FROM base
    )
    WHERE rn = 1
)

SELECT
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    payment_type_name,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    trip_type,
    taxi_type,
    extracted_at
FROM dedup