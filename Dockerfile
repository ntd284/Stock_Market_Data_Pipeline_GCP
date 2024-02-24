# RUN sudo apt-get install openjdk-8-jdk-headless -qq 
# RUN sudo apt-get install scala -y
# RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
# RUN tar -xvzf spark-3.5.0-bin-hadoop3.tgz
# RUN mv spark-3.5.0-bin-hadoop3  /mnt/spark
FROM puckel/docker-airflow:1.10.4
COPY dags /usr/local/airflow/dags