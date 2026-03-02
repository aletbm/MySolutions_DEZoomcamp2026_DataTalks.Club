/* @bruin

name: ny_taxi.trips_report
type: bq.sql
connection: bigquery-default

depends:
  - ny_taxi.staging_trips

materialization:
  type: view

columns:
  - name: trip_date
    type: date
    description: Calendar date of pickup
    primary_key: true
  - name: taxi_type
    type: string
    description: Color/type of taxi
    primary_key: true
  - name: trip_count
    type: bigint
    description: Number of trips on that date for the taxi type
    checks:
      - name: non_negative
  - name: avg_fare
    type: float
    description: Average fare amount
    checks:
      - name: non_negative

@bruin */

SELECT
    CAST(pickup_datetime AS DATE) AS trip_date,
    taxi_type,
    COUNT(*) AS trip_count,
    AVG(fare_amount) AS avg_fare
FROM {{ var.staging_dataset }}.staging_trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2