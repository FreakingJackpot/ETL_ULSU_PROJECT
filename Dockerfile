FROM python:3.10.0-slim-bullseye
LABEL authors="jackpot"

RUN mkdir -p /home/app

RUN addgroup --system app && adduser --system --group app

ENV HOME=/home/app
ENV APP_DIR=/home/app/web
RUN mkdir APP_DIR
WORKDIR $APP_DIR

COPY requirements.txt .
RUN  pip install --upgrade pip && pip install  --no-cache-dir -r requirements.txt

COPY . $APP_DIR

RUN chown -R app:app $APP_DIR

USER app