FROM python:3.13.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin

WORKDIR /code

ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml uv.lock .python-version ingest_data.py ./

RUN uv sync --locked

ENTRYPOINT ["python", "ingest_data.py"]

#docker run -it --rm -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -e TZ=America/Buenos_Aires -v ny_taxi_postgres_data:/var/lib/postgresql -p 5432:5432 postgres:18
#docker build -t taxi_ingest:v001 .