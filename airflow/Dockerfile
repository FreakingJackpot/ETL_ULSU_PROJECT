FROM apache/airflow:2.7.3
USER ROOT
COPY ./airflow/requirements.txt airflow-requirements.txt
COPY ./app/requirements.txt main-requirements.txt
USER airflow
RUN pip install --no-cache-dir --user -r airflow-requirements.txt -r main-requirements.txt
ENTRYPOINT ["/bin/bash"]