import sys
import random
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

# WARNING
# Deprecated, please use jdbc_to_gcs_hash_key.py


def main(argv):
    # workaround to keep both versions of debussy_framework working
    # using the same script
    # v3 of framework dont have connection data at dag construction time
    # and delegates this job to the script
    if len(argv) == 7:
        extract_data_using_secret(*argv)
    if len(argv) == 8:
        extract_data(*argv)


def extract_data_using_secret(
    driver, jdbc_url, secret_uri, database_table, query, load_timestamp, output_uri
):
    from google.cloud.secretmanager import SecretManagerServiceClient

    secret_manager_client = SecretManagerServiceClient()
    secret_service_response = secret_manager_client.access_secret_version(
        name=secret_uri
    )
    secret = secret_service_response.payload.data.decode("UTF-8")
    db_conn_data = json.loads(secret)
    jdbc_url = jdbc_url.format(**db_conn_data)
    user = db_conn_data["user"]
    password = db_conn_data["password"]
    extract_data(
        driver=driver,
        jdbc_url=jdbc_url,
        user=user,
        password=password,
        database_table=database_table,
        query=query,
        load_timestamp=load_timestamp,
        output_uri=output_uri,
    )


def extract_data(
    driver, jdbc_url, user, password, database_table, query, load_timestamp, output_uri
):
    spark = (
        SparkSession.builder.master("yarn")
        .appName(f"extract-{database_table}")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )
    sc.setLogLevel("WARN")

    def random_hex():
        return "%030x" % random.randrange(16**30)

    random_hex = F.udf(random_hex, T.StringType())
    ingestion_ts = datetime.strptime(load_timestamp, "%Y%m%dT%H%M%S")

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("query", query)
        .option("driver", driver)
        .option("user", user)
        .option("password", password)
        .load()
    )

    df = df.withColumn(
        "METADATA",
        F.struct(
            F.lit(ingestion_ts).alias("IngestionDate"),
            F.lit(ingestion_ts).alias("UpdateDate"),
            random_hex().alias("Id"),
        ),
    )

    df.write.format("parquet").mode("overwrite").save(output_uri)


if __name__ == "__main__":
    main(sys.argv[1:])
