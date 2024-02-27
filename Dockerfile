# RUN sudo apt-get install openjdk-8-jdk-headless -qq 
# RUN sudo apt-get install scala -y
# RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
# RUN tar -xvzf spark-3.5.0-bin-hadoop3.tgz
# RUN mv spark-3.5.0-bin-hadoop3  /mnt/spark
FROM apache/airflow:2.7.1
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
RUN pip install --upgrade oauth2client
# COPY credential.json /opt/airflow/credential.json
# COPY dags/files /opt/airflow/dags/files