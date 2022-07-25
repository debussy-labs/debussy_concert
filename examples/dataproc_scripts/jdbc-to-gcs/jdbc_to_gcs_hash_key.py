import sys
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main(argv):
    extract_data_using_secret(*argv)


def extract_data_using_secret(
    driver,
    jdbc_url,
    secret_uri,
    database_table,
    query,
    load_timestamp,
    output_uri
):
    from google.cloud.secretmanager import SecretManagerServiceClient

    secret_manager_client = SecretManagerServiceClient()
    secret_service_response = secret_manager_client.access_secret_version(name=secret_uri)
    secret = secret_service_response.payload.data.decode("UTF-8")
    db_conn_data = json.loads(secret)
    jdbc_url = jdbc_url.format(**db_conn_data)
    user = db_conn_data['user']
    password = db_conn_data['password']
    extract_data(
        driver=driver,
        jdbc_url=jdbc_url,
        user=user,
        password=password,
        database_table=database_table,
        query=query,
        output_uri=output_uri,
    )


def extract_data(
    driver,
    jdbc_url,
    user,
    password,
    database_table,
    query,
    output_uri
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

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("query", query)
        .option("driver", driver)
        .option("user", user)
        .option("password", password)
        .load()
    )

    df = df.withColumn("_hash_key", F.md5(F.concat_ws("", *df.columns)))

    df.write.format("parquet").mode("overwrite").save(output_uri)


if __name__ == "__main__":
    args = sys.argv[1:]
    main(args)
