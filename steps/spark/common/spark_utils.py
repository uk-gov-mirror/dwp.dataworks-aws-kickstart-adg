import sys
import re
import boto3

from datetime import datetime, timedelta

import pyspark
from pyspark.sql import SparkSession, Catalog
from pyspark.sql import functions as F
from pyspark.sql.types import *


def get_spark_session(logger, job_name, module_name, **kwargs):
    try:
        spark = (
            SparkSession.builder.master("yarn")
                .appName(f'{job_name}-{module_name}')
                .enableHiveSupport()
                .getOrCreate()
        )
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    except BaseException as ex:
        logger.error("Failed to generate the spark session because of error : %s ", str(ex))
        sys.exit(-1)

    return spark

def check_database_exists(catalog, database_name):
    if [True for database in catalog.listDatabases() if database_name in database.name]:
        return True
    return False

def check_table_exists(catalog, table_name, database_name):
    if [True for table in catalog.listTables(database_name) if table_name == table.name]:
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
        logger.error("Failed to get compatible schema for spark becuase of error: %s", str(e))
        sys.exit(-1)


def get_old_schema(logger, spark, schema, database_name, table_name):
    try:
        catalog = Catalog(spark)
        logger.info(schema)
        schema=StructType([StructField(key, eval(convert_to_spark_schema(logger, value))(), True) for key, value in schema.items()])
        if check_database_exists(catalog, database_name):
            if check_table_exists(catalog, table_name, database_name):
                logger.info("Hive table already exists. extracting the latest schema from hive table")
                latest_hive_schema_df = spark.sql(f"select * from {database_name}.{table_name} limit 1")
                schema = latest_hive_schema_df.schema
        return schema

    except BaseException as ex:
        logger.error("Failed to get old schema because of error: %s", str(ex))
        sys.exit(-1)

def source_extraction(logger, spark, path, sts_token, source_type):
    try:
        if sts_token is not None:
            spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.awsCredentialsProvider", "org.apache.hadoop.fs.s3.TemporaryAWSCredentialsProvider")
            spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", sts_token["Credentials"]["AccessKeyId"])
            spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", sts_token["Credentials"]["SecretAccessKey"])
            spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.awsSessionToken", sts_token["Credentials"]["SessionToken"])

        if source_type.lower() == "csv":
            df = spark.read\
                      .option("header", True)\
                      .option("inferSchema", True)\
                      .option("multiline", True) \
                      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
                      .format("csv") \
                      .load(path)

        if source_type.lower() == "json":
            rdd = spark.sparkContext.parallelize(path)
            df = spark.read\
                      .option("multiline", True)\
                      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")\
                      .json(rdd)

    except BaseException as ex:
        logger.error(
            "Failed to read the source file into spark dataframe because of error: %s", str(ex)
        )

    return df

def get_formatted_schema(df):
    return {field.name.lower(): field.dataType for field in df.schema.fields}

def get_update_dataframe(logger, new_schema_df, old_schema, new_schema):
    try:
        for column, datatype in old_schema.items():
            if column not in new_schema:
                logger.warning(f"{column} has been removed in new schema, adding it with default value to maintain backward compatibility")
                new_schema_df = new_schema_df.withColumn(column, F.lit(None).cast(old_schema[column]))
            elif new_schema[column] != old_schema[column]:
                new_schema_df = new_schema_df.withColumn(f"{column}_1", F.col(column).cast(old_schema[column])) \
                                             .withColumn("error_desc_1", F.when(F.col(f"{column}_1").isNull(),
                                                                                F.concat(F.lit(f"Type Conversion for column {column}. The source value is: "),
                                                                                F.col(f"{column}"))) \
                                                                        .otherwise(None)) \
                                             .withColumn("error_desc", F.when(F.col("error_desc").isNull(), F.array(F.col("error_desc_1"))) \
                                                                        .otherwise(F.concat(F.col("error_desc"), F.array(F.col("error_desc_1"))))) \
                                            .drop(column, "error_desc_1") \
                                            .withColumnRenamed(f"{column}_1", column)

    except BaseException as e:
        logger.error("Failed to update the dataframe because of error: %s", str(e))
        sys.exit(-1)

    return new_schema_df

def get_evolved_schema(logger, old_schema_df, new_schema_df):
    try:
        old_schema, new_schema = get_formatted_schema(old_schema_df), get_formatted_schema(new_schema_df)
        if old_schema != new_schema:
            update_df = get_update_dataframe(logger, new_schema_df, old_schema, new_schema)
            return update_df

        logger.info("No change in schema. Skipping the schema evolution logic")
        return new_schema_df

    except BaseException as ex:
        logger.error("Failed to get evolved schema because of error: %s", str(ex))
        sys.exit(-1)


def transformation(logger, spark, source_df, processing_dt, initial_spark_schemas, config, collection):
    try:
        if config["module_name"] == "vacancy":
            new_df = source_df.select([F.col(col).alias(re.sub("[^0-9a-zA-Z$]+", " ", col).strip().replace(" ", "_").lower()) for col in source_df.columns])
            new_df = new_df.withColumn("date_uploaded", F.lit(datetime.strftime(processing_dt, "%Y-%m-%d"))) \
                           .withColumn("error_desc", F.lit(None).cast(ArrayType(StringType())))

            schema=get_old_schema(logger,
                                  spark,
                                  schema=initial_spark_schemas[collection],
                                  database_name=config["published_database_name"],
                                  table_name=collection)
            old_df = spark.createDataFrame([], schema)
            evolved_df = get_evolved_schema(logger, old_df, new_df)

        elif config["module_name"] in ("application", "payment") :
            new_df = source_df.select([F.col(col).alias(col[0].lower() + re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), col[1:])) for col in source_df.columns])
            new_df = new_df.withColumn("date_uploaded", F.lit(datetime.strftime(processing_dt, "%Y-%m-%d"))) \
                           .withColumn("error_desc", F.lit(None).cast(ArrayType(StringType())))\
                           .withColumn("row_hash_id", F.sha2(F.concat_ws("||", *new_df.columns), 256))

            old_schema=get_old_schema(logger,
                                  spark,
                                  schema=initial_spark_schemas[collection],
                                  database_name=config["published_database_name"],
                                  table_name=collection)

            old_df = spark.createDataFrame([], old_schema)
            evolved_df = get_evolved_schema(logger, old_df, new_df)


    except BaseException as ex:
        logger.error("Failed to transformation the source dataframe because of error: %s", str(ex))
        sys.exit(-1)

    return evolved_df

def writer_parquet(logger, spark, df, path):
    try:

        spark.sparkContext._jsc.hadoopConfiguration().unset("fs.s3.awsCredentialsProvider")
        spark.sparkContext._jsc.hadoopConfiguration().unset("fs.s3.awsAccessKeyId")
        spark.sparkContext._jsc.hadoopConfiguration().unset("fs.s3.awsSecretAccessKey")
        spark.sparkContext._jsc.hadoopConfiguration().unset("fs.s3.awsSessionToken")

        df.write\
          .mode('Overwrite')\
          .partitionBy("date_uploaded")\
          .parquet(path)

    except BaseException as e:
        logger.error("Failed to write the transformed dataframe because of error: %s",str(e))

def get_hive_schema(logger, df):
    try:
        schema = ',\n'.join([f"{item[0]} {item[1]}" for item in df.dtypes if "date_uploaded" not in item])
        return schema

    except BaseException as e:
        logger.error("Failed to generate hive schema because of error: %s",
        str(e))
        
def create_hive_tables_on_published(
        logger, spark, collection_name, df, path, config):

    try:
        create_db_query = f"""CREATE DATABASE IF NOT EXISTS {config["published_database_name"]}"""
        spark.sql(create_db_query)

        schema = get_hive_schema(logger, df)

        src_hive_table=config["published_database_name"]+"."+collection_name
        src_hive_drop_query = f"DROP TABLE IF EXISTS {src_hive_table}"
        spark.sql(src_hive_drop_query)

        src_hive_create_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {src_hive_table} (
        {schema}
        ) STORED AS PARQUET 
        PARTITIONED BY (date_uploaded DATE)
        LOCATION '{path}'
        """
        spark.sql(src_hive_create_query)

        spark.sql(f"MSCK REPAIR TABLE {src_hive_table}")

    except BaseException as ex:
        logger.error("Failed to create Hive tables because of error: %s", str(ex))
        sys.exit(-1)


def clean_up_published_bucket(
        logger, spark, collections, s3_published_bucket,
        published_database_name, module_name, correlation_id, **kwargs
    ):
    try:
        logger.info("Clean up the old files generated on data location")
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(s3_published_bucket)
        for collection in collections:
            prefix_name=f'data/{published_database_name}/{collection}/'
            deleteObj = bucket.objects.filter(Prefix=prefix_name).delete()
            logger.info("folder deleted: %s", deleteObj)

            logger.info("Drop existing Hive Metastore table")
            src_hive_table=published_database_name+"."+collection
            catalog=Catalog(spark)
            if check_database_exists(catalog, database_name=published_database_name):
                if check_table_exists(catalog, table_name=collection, database_name=published_database_name):
                    spark.sql(f"DROP TABLE {src_hive_table}")

    except BaseException as ex:
        logger.error(f"Failed to clean-up for {module_name} with {correlation_id} because of error: {str(ex)}")
        sys.exit(-1)
