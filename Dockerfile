FROM python:3.11-slim

WORKDIR /airbyte/integration_code

ENV PYTHONUNBUFFERED=1 \
    PYTHONWARNINGS=ignore

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN adduser --disabled-password --gecos "" --uid 1000 airbyte && \
    mkdir -p /airbyte && \
    chown -R airbyte:airbyte /airbyte

USER airbyte:airbyte

ENTRYPOINT ["python", "-m", "source_google_play_console"]
