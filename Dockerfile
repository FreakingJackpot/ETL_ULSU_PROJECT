FROM python:3.10.0-slim-bullseye
LABEL authors="jackpot"

RUN mkdir -p /home/app

RUN addgroup --system app && adduser --system --group app

ENV HOME=/home/app
ENV APP_DIR=/home/app/web
RUN mkdir $APP_DIR
WORKDIR $APP_DIR

#Установка зависимостей Python
COPY requirements.txt .
RUN  pip install --upgrade pip && pip install  --no-cache-dir -r requirements.txt

#Копирование и настройка скрипта ожидания подъема PostgreSQL
COPY ./entrypoint.sh .
RUN sed -i 's/\r$//g' $APP_DIR/entrypoint.sh
RUN chmod +x $APP_DIR/entrypoint.sh

#Копирование кода приложения Django
COPY . $APP_DIR

RUN chown -R app:app $APP_DIR

USER app

#Ожидание подключения к postgres
ENTRYPOINT ["$APP_DIR/entrypoint.sh"]




