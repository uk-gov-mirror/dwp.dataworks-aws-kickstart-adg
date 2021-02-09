import sys
import re
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from common.utils import log_end_of_batch

def get_spark_session(job_name, module_name):
    spark = (
        SparkSession.builder.master("yarn")
            .appName(f"{job_name}-{module_name}")
            .enableHiveSupport()
            .getOrCreate()
    )
    spark.conf.set("spark.scheduler.mode", "FAIR")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark

def read_csv_with_inferschema(logger, spark, sts_token, path, run_id, processing_dt, args, config):
    try:
        logger.info("Setting the temp credentials for accessing the remote bucket")

        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.awsCredentialsProvider", "org.apache.hadoop.fs.s3.TemporaryAWSCredentialsProvider")
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", sts_token["Credentials"]["AccessKeyId"])
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", sts_token["Credentials"]["SecretAccessKey"])
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.awsSessionToken", sts_token["Credentials"]["SessionToken"])

        logger.info("Reading the data from remote bucket and creating the dataframe")

        df = spark.read.format("csv")\
              .option("header", True)\
              .option("inferSchema", True)\
              .option("multiline", True)\
              .load(path)
        
        return df
    
    except BaseException as ex:
        logger.error(
            "Problem while reading file for Correlation Id: %s because of error %s",
            args.correlation_id,
            str(ex)
        )
        log_end_of_batch(logger, run_id, processing_dt, args, config, status=config["DEFAULT"]["Failed_Status"])
        sys.exit(-1)


def writer_parquet(logger, spark, df, path, run_id, processing_dt, args, config):
    try:
        logger.info("Resetting the hadoop configuration")

        spark.sparkContext._jsc.hadoopConfiguration().unset("fs.s3.awsCredentialsProvider")
        spark.sparkContext._jsc.hadoopConfiguration().unset("fs.s3.awsAccessKeyId")
        spark.sparkContext._jsc.hadoopConfiguration().unset("fs.s3.awsSecretAccessKey")
        spark.sparkContext._jsc.hadoopConfiguration().unset("fs.s3.awsSessionToken")

        logger.info("Writing the records to destination bucket")

        df.write\
          .mode('Overwrite')\
          .partitionBy("date_uploaded")\
          .parquet(path)
        return True
    except Exception as e:
        logger.error("Problem in writing the files for correlation_id %s for run id %s due to error %s", 
        args.correlation_id,
        run_id,
        str(e))
        log_end_of_batch(logger, run_id, processing_dt, args, config, status=config["DEFAULT"]["Failed_Status"])
        sys.exit(-1)


def get_hive_schema(logger, df, run_id, processing_dt, args, config):
    try:
        schema = ',\n'.join([f"{item[0]} {item[1]}" for item in df.dtypes if "date_uploaded" not in item])
        return schema
    except BaseException as e:
        logger.error("Error while generating the schema for correlation id %s because of %s",
        args.correlation_id,
        str(e))
        log_end_of_batch(logger, run_id, processing_dt, args, config, status=config["DEFAULT"]["Failed_Status"])
        sys.exit(-1)
        
def create_hive_tables_on_published(logger, spark, collection_name, df, path, run_id, processing_dt, args, config):
    try:
        logger.info("Creating metastore db while processing correlation_id %s",args.correlation_id)
        create_db_query = f"""CREATE DATABASE IF NOT EXISTS {config["DEFAULT"]["published_database_name"]}"""
        spark.sql(create_db_query)
        
        schema = get_hive_schema(logger, df, run_id, processing_dt, args, config)
        src_hive_table=config["DEFAULT"]["published_database_name"]+"."+collection_name
        logger.info("Creating hive table for : %s for correlation id: %s",src_hive_table,args.correlation_id)
        src_hive_drop_query = f"DROP TABLE IF EXISTS {src_hive_table}"
        src_hive_create_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {src_hive_table} (
        {schema}
        ) STORED AS PARQUET 
        PARTITIONED BY (date_uploaded DATE)
        LOCATION '{path}'
        """
        spark.sql(src_hive_drop_query)
        spark.sql(src_hive_create_query)
        
        return True
        
    except BaseException as ex:
        logger.error("Problem with creating Hive tables for correlation Id: %s because of error: %s",args.correlation_id, str(ex))
        log_end_of_batch(logger, run_id, processing_dt, args, config, status=config["DEFAULT"]["Failed_Status"])
        sys.exit(-1)

def transformation(logger, spark, df, run_id, processing_dt, args, config):
    try:
        renamed_df = df.select([F.col(col).alias(re.sub("[^0-9a-zA-Z$]+", "", col)) for col in df.columns])
        add_new_col_df = renamed_df.withColumn("date_uploaded", F.lit(processing_dt))
        return add_new_col_df

    except BaseException as ex:
        logger.error("Problem while applying transformation for correlation Id: %s because of error: %s",args.correlation_id, str(ex))
        log_end_of_batch(logger, run_id, processing_dt, args, config, status=config["DEFAULT"]["Failed_Status"])
        sys.exit(-1)

