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

        encrypted_key_content = utils.get_bodyfor_key(logger, keys[-1], s3_client, bucket=config["s3_src_bucket"])

        logger.info(f'extract the metadata of the file {config["correlation_id"]}')
        metadata = utils.get_metadatafor_key(logger, key=keys[-1], s3_client=s3_client, bucket=config["s3_src_bucket"])
        ciphertext, datakeyencryptionkeyid, iv = metadata["ciphertext"], metadata["datakeyencryptionkeyid"], metadata["iv"]

        logger.info("Extracting the plain text key")
        plain_text_key = utils.get_plaintext_key_calling_dks(
                logger, ciphertext, datakeyencryptionkeyid, config)

        logger.info("Decrypt the data")
        decrypted_key_content = json.loads(utils.decrypt(logger, plain_text_key, iv, encrypted_key_content))

        logger.info("Extracting PII and NON-PII fields")
        fields_classifications = utils.get_pii_non_pii_fields(logger, decrypted_key_content)
        config["pii_fields"], config["non_pii_fields"] = fields_classifications[0], fields_classifications[1]

        logger.info("Convert the json record into spark dataframe")
        path = decrypted_key_content["data"]
        source_df = spark_utils.source_extraction(
            logger, spark, path, sts_token, source_type="json")

        destination_bucket = config['s3_published_bucket']
        domain_name=config["published_database_name"]

        if config["pii_fields"]:
            logger.info("Apply transformation for pii data")
            pii_transformed_df = spark_utils.transformation(
                logger, spark, source_df, processing_dt, initial_spark_schemas,
                config, collection=f"{collection}_pii", pii=True)

            logger.info("write pii data into required folder")
            destination_folder = f"data/{domain_name}/pii/{collection}/"
            destination_path = f"s3://{destination_bucket}/{destination_folder}"
            spark_utils.writer_parquet(
                logger, spark, pii_transformed_df, destination_path)

            logger.info("create hive table on pii data")
            spark_utils.create_hive_tables_on_published(
                logger, spark, f"{collection}_pii", pii_transformed_df, destination_path, config)

            logger.info("tag pii objects based on rbac model")
            utils.tag_objects(
                logger, destination_bucket, destination_folder, config, table=f"{collection}_pii", access_pii='false' )

        if config["non_pii_fields"]:
            logger.info("Apply transformation for non-pii data")
            non_pii_transformed_df = spark_utils.transformation(
                logger, spark, source_df, processing_dt, initial_spark_schemas,
                    config, collection=f"{collection}_non_pii", pii=False)

            logger.info("write non-pii data into required folder")
            destination_folder = f"data/{domain_name}/non-pii/{collection}/"
            destination_path = f"s3://{destination_bucket}/{destination_folder}"
            spark_utils.writer_parquet(
                logger, spark, non_pii_transformed_df, destination_path)

            logger.info("create hive table on non-pii data")
            spark_utils.create_hive_tables_on_published(
                    logger, spark, f"{collection}_non_pii", non_pii_transformed_df, destination_path, config)

            logger.info("tag non-pii objects based on rbac model")
            utils.tag_objects(
                logger, destination_bucket, destination_folder, config, table=f"{collection}_non_pii", access_pii='false' )

    except BaseException as ex:
        utils.log_end_of_batch(
            logger, hash_id=config["correlation_id"],
            processing_dt=datetime.strftime(processing_dt, "%Y-%m-%d"), run_id=run_id, status=config["Failed_Status"], **config)
        logger.error("Process has failed because of error: %s", str(ex))
        sys.exit(-1)
