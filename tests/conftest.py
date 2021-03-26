import os
import configparser
import pytest

try:
    import pyspark
except:
    import findspark
    findspark.init('/Users/ashisprasad/homebrew/Cellar/apache-spark/3.1.1/libexec')
    import pyspark

from pyspark.sql import SparkSession

environment="${environment}"
aws_region="${aws_region_name}"
audit_table_name="${audit_table_name}"
audit_table_hash_key="${audit_table_hash_key}"
audit_table_range_key="${audit_table_range_key}"
audit_table_data_product_name="${audit_table_data_product_name}"
aws_secret_name="${aws_secret_name}"
published_database_name="${published_database_name}"
assume_role_within_acct_arn="${assume_role_within_acct_arn}"
assume_role_outside_acct_arn="${assume_role_outside_acct_arn}"
log_path="${log_path}"
s3_published_bucket="${s3_published_bucket}"
domain_name="${domain_name}"
e2e_test_folder="${e2e_test_folder}"
url="${url}"
In_Progress_Status="In-Progress"
Completed_Status="Completed"
Failed_Status="Failed"
TESTING="testing"

@pytest.fixture(scope="session")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = TESTING
    os.environ["AWS_SECRET_ACCESS_KEY"] = TESTING
    os.environ["AWS_SECURITY_TOKEN"] = TESTING
    os.environ["AWS_SESSION_TOKEN"] = TESTING

@pytest.fixture(scope="session")
def spark():
    os.environ[
        "PYSPARK_SUBMIT_ARGS"
    ] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" --conf spark.jars.ivySettings=/root/ivysettings.xml pyspark-shell'

    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
    spark = (
        SparkSession.builder.master("local")
            .appName("adg-test")
            .config("spark.local.dir", "spark-temp")
            .enableHiveSupport()
            .getOrCreate()
    )

    # Setup spark to use s3, and point it to the moto server.
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", "mock")
    hadoop_conf.set("fs.s3a.secret.key", "mock")
    hadoop_conf.set("fs.s3a.endpoint", "http://127.0.0.1:5000")

    spark.sql("create database if not exists test_db")
    yield spark
    print("Stopping Spark")
    spark.stop()

@pytest.fixture(scope="session")
def mock_config():
    config = configparser.ConfigParser()
    config = {"environment" : environment,
              "aws_region" : aws_region,
              "audit_table_name" : audit_table_name,
              "audit_table_hash_key" : audit_table_hash_key,
              "audit_table_range_key" : audit_table_range_key,
              "audit_table_data_product_name" : audit_table_data_product_name,
              "aws_secret_name" : aws_secret_name,
              "published_database_name" : published_database_name,
              "assume_role_within_acct_arn" : assume_role_within_acct_arn,
              "assume_role_outside_acct_arn" : assume_role_outside_acct_arn,
              "log_path" : log_path,
              "s3_published_bucket" : s3_published_bucket,
              "domain_name": domain_name,
              "e2e_test_folder": e2e_test_folder,
              "url" : url,
              "In_Progress_Status": In_Progress_Status,
              "Completed_Status": Completed_Status,
              "Failed_Status": Failed_Status,
              }
    return config
