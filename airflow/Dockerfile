FROM apache/airflow:2.7.3
USER airflow
COPY ./airflow/log_config.py /opt/airflow/config/log_config.py

COPY requirements.txt .
COPY airflow-requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt -r airflow-requirements.txt

COPY ./app /app
ENTRYPOINT ["/bin/bash"]