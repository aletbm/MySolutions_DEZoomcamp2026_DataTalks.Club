@echo off
for /f "tokens=1,2 delims==" %%a in (.env) do set %%a=%%b

bruin run pipeline/pipeline.yml ^
  --end-date 2019-02-01 ^
  --full-refresh ^
  --workers 1 ^
  --environment production ^
  --var "ingestion_dataset=\"%BQ_PROJECT%.%BQ_INGESTION_DATASET%\"" ^
  --var "staging_dataset=\"%BQ_PROJECT%.%BQ_STAGING_DATASET%\"" ^
  --var "reports_dataset=\"%BQ_PROJECT%.%BQ_REPORTS_DATASET%\""