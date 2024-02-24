from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
# from key.keys import token


spark = (
        SparkSession
        .builder
        .appName("ReadFromGCS")
        .config("spark.jars", "https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar,https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")
        .getOrCreate()
)

bucket_name="datastocks123"
gcs_path=f"gs://{bucket_name}/oneyear.csv"

schema_stock_info=(
        StructType
        (
            [
                    StructField("time"                      , DateType()   , True),
                    StructField("open"                      , DoubleType()   , True),
                    StructField("high"                      , DoubleType()   , True),
                    StructField("low"                       , DoubleType()   , True),
                    StructField("close"                     , DoubleType()   , True),
                    StructField("volume"                    , DoubleType()   , True),
                    StructField("ticker"                    , StringType()   , True),                        
            ]
        )
)
stock_info=(
        spark
                .read
                .option('header','true')
                .schema(schema_stock_info)
                .csv(gcs_path)
)
# stock_info.printSchema()
stock_info.createOrReplaceTempView("stock_info")
query = spark.sql("""   
                        SELECT time,ticker,close,min_volume FROM
                        (
                        SELECT *, MAX(diff_close) OVER(PARTITION BY ticker) AS max_diff_close,
                        MIN(volume) OVER(PARTITION BY ticker) AS min_volume
                        FROM
                        (
                        SELECT *,

                            LAG(close) OVER(
                                PARTITION BY ticker
                                ORDER BY time
                            ) AS lag_close,

                            ABS(ROUND((close-LAG(close) OVER(
                                PARTITION BY ticker
                                ORDER BY time

                            ))*100/LAG(close) OVER(
                                PARTITION BY ticker
                                ORDER BY time
                            ),2)) AS diff_close,

                            FIRST_VALUE(close) OVER(
                                PARTITION BY ticker 
                                ORDER BY time
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_value,

                            LAST_VALUE(close) OVER(
                                PARTITION BY ticker 
                                ORDER BY time
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_value,

                            (LAST_VALUE(close) OVER(
                                PARTITION BY ticker 
                                ORDER BY time
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - FIRST_VALUE(close) OVER(
                                PARTITION BY ticker 
                                ORDER BY time
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS trending
                            FROM stock_info 
                        )
                        )
                        WHERE max_diff_close < 10 AND trending > 0 AND min_volume > 1000
                    """)
query.show()
# query.write.format("bigquery").option("credentialsFile",f"{token.dictionary_file()}/credentials.json").option("temporaryGcsBucket","gs://datastocks123/").option("table", "just-shell-415015.datastocks123.vnstock_3M").mode('overwrite').save()

query.write.format("bigquery").option("temporaryGcsBucket","datastocks123").option("table", "just-shell-415015.datastocks123.vnstock_3M").mode('overwrite').save()
