FROM apache/airflow:2.8.1
COPY dags /opt/airflow/dags
COPY requirements.txt /opt/airflow/requirements.txt
COPY airflow.cfg /opt/airflow/airflow.cfg

USER airflow
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /opt/airflow/requirements.txt





