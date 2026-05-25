FROM python:3.11-slim

WORKDIR /airbyte/integration_code

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "-m", "source_google_play_console"]
