FROM python:3.10
WORKDIR /repo

RUN mkdir -p /tmp/chrome-dir
RUN apt-get update && apt-get install -y curl unzip && \
curl -Lo "/tmp/chromedriver-linux64.zip" "https://storage.googleapis.com/chrome-for-testing-public/126.0.6478.126/linux64/chromedriver-linux64.zip" && \
curl -Lo "/tmp/chrome-headless-shell-linux64.zip" "https://storage.googleapis.com/chrome-for-testing-public/126.0.6478.126/linux64/chrome-headless-shell-linux64.zip" && \
unzip /tmp/chromedriver-linux64.zip -d /opt/ && \
unzip /tmp/chrome-headless-shell-linux64.zip -d /opt/

RUN apt-get update && \
apt-get install -y libnss3 libdbus-1-3 libatk1.0-0 libatk-bridge2.0 libxcomposite1 libxdamage1 libxrandr2 libxkbcommon-x11-dev libgbm-dev libasound2

COPY . .
RUN pip install poetry

RUN poetry config virtualenvs.create false && poetry lock && poetry install --no-root
RUN pip install uvicorn[standard]
CMD celery -A src.Orchestrator.worker worker -Q dat-worker-q
