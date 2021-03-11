import importlib
import sys
import os
from datetime import datetime, timedelta, date

def execute(
        logger, spark,  source_path, processing_dt, run_id, config,
        sts_token, secrets_response, collection, module_name, published_database_name, s3_published_bucket, **kwargs):

    try:
        logger.info("Import required libraries")
        utils = importlib.import_module("common.utils")
        spark_utils = importlib.import_module("common.spark_utils")

        logger.info("Extract data from source")
        source_df = spark_utils.csv_extraction(
            logger, spark, source_path, processing_dt, run_id, config, sts_token)

        logger.info("Extract old schema")
        schema= spark_utils.get_old_schema(
            logger,spark,schema=secrets_response["initial_spark_schema"][module_name][collection],
            database_name=published_database_name,table_name=collection)

        logger.info("Transform to existing dataset")
        df_with_transformation = spark_utils.transformation(
            logger, spark, schema, source_df, run_id, processing_dt, config)

        logger.info(f"Load the file to destination bucket")
        destination_bucket = s3_published_bucket
        domain_name = published_database_name
        destination_folder = f"data/{domain_name}/non-pii/{collection}/"
        destination_path = f"s3://{destination_bucket}/{destination_folder}"

        spark_utils.writer_parquet(
            logger, spark, df_with_transformation, destination_path, run_id, processing_dt, config)
                

        logger.info(f"Create hive tables")
        spark_utils.create_hive_tables_on_published(
            logger, spark, collection, df_with_transformation, destination_path, run_id, processing_dt, config)

        logger.info(f"add the tag to objects based on RBAC model")
        utils.tag_objects(logger, destination_bucket, destination_folder, config, collection, access_pii='false')

    except BaseException as ex:
        logger.error(f"Problem while processing the data for {module_name} because of above error")
        sys.exit(-1)

