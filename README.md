# Проект сбора и представления данных по коронавирусу по дисциплине "технологии хранения и обработки больших объемов информации"

## Описание

Система собирает из открытых источников данных (stopcoronavirus, gogov, csv файл, внешняя база данных (данные yandex
datalens и OWID)), обрабатывает их и предоставляет данные через API.

## Состав группы и их вклад в систему:

* Сергеев Сергей - выбор технологий, поиск и анализ источников данных, обработка данных, загрузка данных из gogov,
  настройка docker и компонентов системы, разработка и реализация API, написание тестов
* Абросимов Алексей - поиск и анализ источников данных, написание документации, обработка данных
* Бексаев Илья (moroz12112) - поиск и анализ источников данных, загрузка данных из stopcorona, обработка данных,
  написание документации и тестов
* Александр Верховский (KittoNoKatsu) - загрузка данных из csv, написание документации и тестов
* Анастасия Бирюкова (nastyaqw) - бывший участник, поиск и анализ источников данных, формирование данных для внешней
  базы данных

## Сроки разработки - 17.10.2023 - 20.01.2024

## Стек

* Docker
* Docker-compose
* Python 3.8
* Django
* Airflow
* Postgres
* Prometheus
* Grafana
* Loki

## Минимальные требования

12 гб оперативной памяти

## Инструкция по запуску с помощью docker-compose

### main

1. Создать .env файл по шаблону env.example и заполнить его
2. В config.yml, в папке alertmanager вставить токен telepush в конец запроса
3. Собрать и запустить Docker-compose
4. Если не выставлен AIRFLOW_UID или в логах ошибка о доступе к папке dags, то разрешить чтение и запуск папки
   ailfow/dags и файлов внутри
5. Создайте суперпользователя Django:  docker exec -it web python manage.py createsuperuser
6. Создайте админа Airflow:  docker exec -it airflow airflow users create -e EMAIL -f FIRSTNAME -l
   LASTNAME [-p PASSWORD] -r ROLE -u USERNAME
7. По порту 8081 включить даги (etl_data_full,etl_data_latest могут упасть при первом запуске, т.к зависят от
   etl_legacy_data)
8. По порту 1337, перейти на страницу /admin. Выбрать модель DatasetInfo и настроить информацию о датасетах

### dev

1. Создать .env файл по шаблону env.example и заполнить его
3. Собрать и запустить Docker-compose
4. Если не выставлен AIRFLOW_UID или в логах ошибка о доступе к папке dags, то разрешить чтение и запуск папки
   ailfow/dags и файлов внутри
5. Создайте суперпользователя Django:  docker exec -it web python manage.py createsuperuser
6. Создайте админа Airflow:  docker exec -it airflow airflow users create -e EMAIL -f FIRSTNAME -l
   LASTNAME [-p PASSWORD] -r ROLE -u USERNAME
7. По порту 8081 включить даги (etl_data_full,etl_data_latest могут упасть при первом запуске, т.к зависят от
   etl_legacy_data)
8. По порту 1337, перейти на страницу /admin. Выбрать модель DatasetInfo и настроить информацию о датасетах

Для получения схемы api перейти на http://0.0.0.0:1337/api/schema/, для визуализации
swagger http://0.0.0.0:1337/api/schema/swagger-ui/
Порты для подключения:

* web - 1337
* airflow - 8081
* grafana - 3100
* prometheus - 9090
* flower - 5555

## Ссылки

* [Документация](https://outstanding-baroness-6ee.notion.site/bdeb0406ef6f42748856a3a0488540b6?v=d63b4d65bcd14a92b854816e87292016)
* [YouGile](https://ru.yougile.com/board/wanl81vnzqpg)
