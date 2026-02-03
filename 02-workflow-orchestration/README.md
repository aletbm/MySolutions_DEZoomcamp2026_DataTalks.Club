# Kestra + BigQuery Taxi Data – Quiz Resolution README

## Context

This README documents how I solved each question of the quiz related to: Workflow orchestration with Kestra

The solution combines:
- Kestra flows with schedule triggers and backfills
- Loading CSV data into BigQuery
- SQL queries executed in BigQuery to validate row counts and file properties

## Running Kestra locally with Docker Compose

Kestra is executed locally using Docker Compose. PostgreSQL is used as the metadata backend for Kestra.

To start all services:

```
docker-compose up -d
```

Once running:

+ Kestra UI is available at http://localhost:8080

+ PgAdmin is available at http://localhost:8085

## Workflow overview

The workflow taxi_flow performs the following steps:

1. Triggered monthly using Schedule triggers for yellow and green taxis

2. Downloads the corresponding CSV file for a given month

3. Logs the uncompressed file size

4. Uploads the CSV to Google Cloud Storage

5. Creates external tables in BigQuery

6. Loads data into partitioned BigQuery tables

7. Merges data using a deterministic unique_row_id

8. Purges local execution files

---

## 1. Uncompressed file size – Yellow Taxi (2020-12)

**Question:**
Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size?

**How it was solved:**
- Executed the Kestra flow for:
  - taxi = yellow
  - year = 2020
  - month = 12
- Inspected the output of the `extract` task
- Checked the generated file:
  - `yellow_tripdata_2020-12.csv`
- Used Kestra execution logs to see the uncompressed size

**Answer:**
128.3 MiB


---

## 2. Rendered value of the `file` variable

**Question:**
What is the rendered value of the variable `file` when:
- taxi = green
- year = 2020
- month = 04

**Template used:**
```
{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv
```

**Rendered result:**
green_tripdata_2020-04.csv


---

## 3. Total rows – Yellow Taxi data (year 2020)

**How it was solved:**
- All Yellow Taxi CSV files for 2020 were loaded into BigQuery
- Used a wildcard table or consolidated dataset
- Executed a COUNT query

**Example BigQuery query:**
```
SELECT COUNT(*) 
FROM `project.dataset.yellow_tripdata`
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2020;
```

**Answer:**
24,648,499 rows


---

## 4. Total rows – Green Taxi data (year 2020)

**How it was solved:**
- Green Taxi CSV files for 2020 were loaded using Kestra backfills
- Data stored in BigQuery
- Executed a COUNT query on the full year

**Example BigQuery query:**
```
SELECT COUNT(*) 
FROM `project.dataset.green_tripdata`
WHERE EXTRACT(YEAR FROM lpep_pickup_datetime) = 2020;
```

**Answer:**
1,734,051 rows


---

## 5. Rows – Yellow Taxi March 2021

**How it was solved:**
- Loaded only the March 2021 Yellow Taxi CSV
- Queried the corresponding table in BigQuery

**Example BigQuery query:**
```
SELECT COUNT(*) 
FROM `project.dataset.yellow_tripdata`
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2021
  AND EXTRACT(MONTH FROM tpep_pickup_datetime) = 3;
```

**Answer:**
1,925,152 rows


---

## 6. Schedule trigger timezone configuration

**Question:**
How would you configure the timezone to New York in a Schedule trigger?

**How it was solved:**
- Reviewed Kestra Schedule Trigger documentation
- Identified the correct IANA timezone format
- Verified best practice for timezone-safe scheduling

**Correct configuration:**
Add a timezone property set to `America/New_York` in the Schedule trigger configuration


---

## Additional Notes

- Backfills were used in Kestra to process historical data for all months
- Each execution dynamically rendered file names using input variables
- BigQuery was used as the source of truth for row counts and validation
- All SQL queries were executed after successful data ingestion