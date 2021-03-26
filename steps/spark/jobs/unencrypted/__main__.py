import importlib
import json
import sys
import os
from datetime import datetime, timedelta, date

def execute(logger, spark,  keys, s3_client, processing_dt, run_id, sts_token, collection, initial_spark_schemas, config):

    try:
        logger.info(f"Importing the required libraries")
        utils = importlib.import_module("common.utils")
        spark_utils = importlib.import_module("common.spark_utils")

        logger.info(f"Extract Data from the file")
        path =  f's3://{config["s3_src_bucket"]}/{keys[-1]}'
        source_df = spark_utils.source_extraction(
                logger, spark, path, sts_token, source_type="csv")

        logger.info(f"Apply transformation of the data")
        transformed_df = spark_utils.transformation(
                    logger, spark, source_df, processing_dt, initial_spark_schemas,
                    config, collection)

        logger.info("Write data into required folder")
        destination_bucket = config['s3_published_bucket']
        domain_name=config["published_database_name"]
        destination_folder = f"data/{domain_name}/{collection}/"
        destination_path = f"s3://{destination_bucket}/{destination_folder}"
        spark_utils.writer_parquet(
                    logger, spark, transformed_df, destination_path)

        logger.info("Create hive table on the data")
        spark_utils.create_hive_tables_on_published(
                    logger, spark, collection, transformed_df, destination_path, config)

        logger.info("Tag objects based on rbac model")
        utils.tag_objects(
            logger, destination_bucket, destination_folder, config, table=collection, access_pii='false' )

    except BaseException as ex:
        utils.log_end_of_batch(
            logger, hash_id=config["correlation_id"],
            processing_dt=datetime.strftime(processing_dt, "%Y-%m-%d"), run_id=run_id, status=config["Failed_Status"], **config)
        logger.error("Process has failed because of error: %s", str(ex))
        sys.exit(-1)
