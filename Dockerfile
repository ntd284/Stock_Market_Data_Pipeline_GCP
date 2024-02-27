FROM apache/airflow:2.8.1
COPY dags /opt/airflow/dags
COPY requirements.txt /opt/airflow/requirements.txt
COPY airflow.cfg /opt/airflow/airflow.cfg

USER airflow
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /opt/airflow/requirements.txt
USER root
RUN sudo apt-get update
RUN sudo apt install -y default-jdk
RUN sudo apt-get install apt-transport-https ca-certificates gnupg curl sudo
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN sudo apt-get update && sudo apt-get install google-cloud-cli




