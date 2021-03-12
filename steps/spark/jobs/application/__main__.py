import importlib
import json
import sys
import os
from datetime import datetime, timedelta, date

def execute(logger, spark,  keys, s3_client, processing_dt, run_id, sts_token, config):

    try:
        logger.info(f"Importing the required libraries")
        utils = importlib.import_module("common.utils")
        spark_utils = importlib.import_module("common.spark_utils")

        logger.info(f"Extract Data from the file")
        for key in keys:
            encrypted_key_content = utils.get_bodyfor_key(logger, key, s3_client, bucket=config["s3_src_bucket"])

            logger.info(f'extract the metadata of the file {config["correlation_id"]}')
            metadata = utils.get_metadatafor_key(logger, key=key, s3_client=s3_client, bucket=config["s3_src_bucket"])
            ciphertext, datakeyencryptionkeyid, iv = metadata["ciphertext"], metadata["datakeyencryptionkeyid"], metadata["iv"]

            logger.info("Extracting the plain text key")
            plain_text_key = utils.get_plaintext_key_calling_dks(
                logger, ciphertext, datakeyencryptionkeyid, config)

            logger.info("Decrypt the data")
            decrypted_key_content =  utils.decrypt(logger, plain_text_key, iv, encrypted_key_content)

            logger.info("Extracting PII and NON-PII fields")
            PII_Fields, Non_Fields = utils.get_pii_non_pii_fields(logger, decrypted_key_content)

            logger.info("Convert the json record into spark dataframe")
            path = f's3://{config["s3_src_bucket"]}/{keys[-1]}'
            source_df = spark_utils.source_extraction(
                logger, spark, path, sts_token, source_type="json")

            source_df.printScheam()

            source_df.show(truncate=False)

    except BaseException as ex:
        utils.log_end_of_batch(
            logger, hash_id=config["correlation_id"],
            processing_dt=datetime.strftime(processing_dt, "%Y-%m-%d"), run_id=run_id, status=config["Failed_Status"], **config)

