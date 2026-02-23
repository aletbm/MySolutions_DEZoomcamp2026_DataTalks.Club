/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# daily summary of trips by taxi type
name: reports.trips_report
# executed on DuckDB
type: duckdb.sql

depends:
  - staging.trips

# materialize as a view to avoid transactional DDL errors
materialization:
  type: view
# using a view sidesteps the COMMIT parser bug seen with tables

# columns of the report table
columns:
  - name: trip_date
    type: date
    description: calendar date of pickup
    primary_key: true
  - name: taxi_type
    type: string
    description: color/type of taxi
    primary_key: true
  - name: trip_count
    type: bigint
    description: number of trips on that date for the taxi type
    checks:
      - name: non_negative
  - name: avg_fare
    type: float
    description: average fare amount
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
    CAST(tpep_pickup_datetime AS date) AS trip_date,
    taxi_type,
    COUNT(*) AS trip_count,
    AVG(fare_amount) AS avg_fare
FROM staging.trips
WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2

-- bruin run my-pipeline/pipeline/assets/reports/trips_report.sql --config-file "./my-pipeline/.bruin.yml" --start-date 2022-01-01 --end-date   2022-02-01 --workers 1