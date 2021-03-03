import sys
import re
import boto3
import pyspark
from pyspark.sql import SparkSession, Catalog
from pyspark.sql import functions as F
from pyspark.sql.types import *
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

def check_database_exists(catalog, database_name):
    if [True for database in catalog.listDatabases() if database_name in database.name]:
        return True
    return False

def check_table_exists(catalog, table_name, database_name):
    if [True for table in catalog.listTables(database_name) if table_name in table.name]:
        return True
    return False

def convert_to_spark_schema(logger, datatype):
    try:
        conversion_mapping = {
            "string" : "StringType",
            "boolean" : "BooleanType",
            "timestamp" : "TimestampType",
            "integer" : "IntegerType",
            "bigint" : "LongType",
            "double" : "DoubleType",
            "decimal" : "DoubleType",
            "text" : "StringType"
        }
        return conversion_mapping[datatype]

    except BaseException as ex:
        logger.error("Problem while getting compatible schema for spark for error %s", str(e))
        sys.exit(-1)


def get_old_schema(logger, spark, schema, database_name, table_name):
    try:
        catalog = Catalog(spark)
        schema=StructType([StructField(key, eval(convert_to_spark_schema(logger, value))(), True) for key, value in schema.items()])
        if check_database_exists(catalog, database_name):
            if check_table_exists(catalog, table_name, database_name):
                logger.info("Hive table already exists. extracting the latest schema from hive table")
                latest_hive_schema_df = spark.sql(f"select * from {database_name}.{table_name} limit 1")
                schema = latest_hive_schema_df.schema
        return schema

    except BaseException as e:
        logger.error("Error while extracting old schema due %s", str(e))
        sys.exit(-1)

def csv_extraction(logger, spark, path, run_id, processing_dt, args, config, sts_token):
    try:
        if sts_token is not None:
            spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.awsCredentialsProvider", "org.apache.hadoop.fs.s3.TemporaryAWSCredentialsProvider")
            spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", sts_token["Credentials"]["AccessKeyId"])
            spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", sts_token["Credentials"]["SecretAccessKey"])
            spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.awsSessionToken", sts_token["Credentials"]["SessionToken"])

        df = spark.read.format("csv")\
                  .option("header", True)\
                  .option("inferSchema", True)\
                  .option("multiline", True) \
                  .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
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

def get_formatted_schema(df):
    return [(field.name.lower(), field.dataType) for field in df.schema.fields]

def get_update_dataframe(logger, new_schema_df, diff_list):
    try:
        for column in diff_list:
            if column[0] not in new_schema_df.columns:
                logger.warn(f"{column[0]} has been removed in new schema, adding it with default value to maintain backward compatibility")
                new_schema_df = new_schema_df.withColumn(column[0], F.lit(None).cast(column[1]))
            else:
                logger.warn(f"{column[0]} datatype has changed to  {column[1]}. resetting to the old datatype to maintain backward compatibility")
                new_schema_df = new_schema_df.withColumn(f"{column[0]}_1", F.col(column[0]).cast(column[1])) \
                                             .withColumn("error_desc_1", F.when(F.col(f"{column[0]}_1").isNull(),
                                                                                F.concat(F.lit(f"Type Conversion for column {column[0]}. The source value is: "),
                                                                                F.col(f"{column[0]}"))) \
                                                                        .otherwise(None)) \
                                             .withColumn("error_desc", F.when(F.col("error_desc").isNull(), F.array(F.col("error_desc_1"))) \
                                                                        .otherwise(F.concat(F.col("error_desc"), F.array(F.col("error_desc_1"))))) \
                                            .drop(column[0], "error_desc_1") \
                                            .withColumnRenamed(f"{column[0]}_1", column[0])

    except BaseException as e:
        logger.error("error occurred while updating the dataframe because of error: %s", str(e))
        sys.exit(-1)

    return new_schema_df

def get_evolved_schema(logger, old_schema_df, new_schema_df):
    try:
        old_schema, new_schema = get_formatted_schema(old_schema_df), get_formatted_schema(new_schema_df)
        diff_list = list(set(old_schema) - set(new_schema))
        if diff_list:
            update_df = get_update_dataframe(logger, new_schema_df, diff_list)
            return update_df
        logger.info("No change in schema. Skipping the schema evolution logic")
        return new_schema_df
    except BaseException as ex:
        logger.error("Problem while evolving the schema %s", str(ex))
        sys.exit(-1)


def transformation(logger, spark, schema, source_df, run_id, processing_dt, args, config):
    try:

        new_df = source_df.select([F.col(col).alias(re.sub("[^0-9a-zA-Z$]+", " ", col).strip().replace(" ", "_").lower()) for col in source_df.columns])
        new_df = new_df.withColumn("date_uploaded", F.lit(processing_dt)) \
                       .withColumn("error_desc", F.lit(None).cast(ArrayType(StringType())))
        old_df = spark.createDataFrame([], schema)
        evolved_df = get_evolved_schema(logger, old_df, new_df)

        return evolved_df

    except BaseException as ex:
        logger.error("Problem while applying transformation because of error: %s", str(ex))
        log_end_of_batch(logger, run_id, processing_dt, args, config, status=config["DEFAULT"]["Failed_Status"])
        sys.exit(-1)

def writer_parquet(logger, spark, df, path, run_id, processing_dt, args, config):
    try:

        spark.sparkContext._jsc.hadoopConfiguration().unset("fs.s3.awsCredentialsProvider")
        spark.sparkContext._jsc.hadoopConfiguration().unset("fs.s3.awsAccessKeyId")
        spark.sparkContext._jsc.hadoopConfiguration().unset("fs.s3.awsSecretAccessKey")
        spark.sparkContext._jsc.hadoopConfiguration().unset("fs.s3.awsSessionToken")

        df.write\
          .mode('Overwrite')\
          .partitionBy("date_uploaded")\
          .parquet(path)
        return True

    except BaseException as e:
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
        spark.sql(f"MSCK REPAIR TABLE {src_hive_table}")
        return True
        
    except BaseException as ex:
        logger.error("Problem with creating Hive tables for correlation Id: %s because of error: %s",args.correlation_id, str(ex))
        log_end_of_batch(logger, run_id, processing_dt, args, config, status=config["DEFAULT"]["Failed_Status"])
        sys.exit(-1)


def clean_up_published_bucket(logger, spark, args, config, collections):
    try:
        logger.info("Getting the cleanup not required prefix!!!")
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(config['DEFAULT']['s3_published_bucket'])
        for collection in collections:
            prefix_name=f'data/{config["DEFAULT"]["published_database_name"]}/non-pii/{collection}/'
            deleteObj = bucket.objects.filter(Prefix=prefix_name).delete()
            src_hive_table=config["DEFAULT"]["published_database_name"]+"."+collection
            catalog=Catalog(spark)
            if check_table_exists(catalog, table_name=collection, database_name=config["DEFAULT"]["published_database_name"]):
                spark.sql(f"DROP TABLE {src_hive_table}")
            logger.info("folder deleted: %s", deleteObj)

    except BaseException as ex:
        logger.error("error while deleting the data %s", str(ex))
        sys.exit(-1)
