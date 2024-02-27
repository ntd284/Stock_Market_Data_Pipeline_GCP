# Stock market Data Pipeline

## Architecture
![](doc/images/etlarchitecture.png)

Pipeline Consists of various modules:

- [Vietnam Stock Market data loader using Python](https://github.com/thinh-vu/vnstock/blob/beta/docs/README-en.md)
- Airflow Modules Management
- Bigquery Warehouse Module
- Google Cloud Storage Module
- Dataproc Module
- Pub/sub Module

<b>Overview</b>

Data is captured in real time from the goodreads API using the Goodreads Python wrapper (View usage - Fetch Data Module). The data collected from the goodreads API is stored on local disk and is timely moved to the Landing Bucket on AWS S3. ETL jobs are written in spark and scheduled in airflow to run every 10 minutes.

