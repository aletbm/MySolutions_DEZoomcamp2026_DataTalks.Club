/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

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
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2