import os
import sys
import importlib
import time
import configparser
from datetime import datetime, timedelta


__author__ = "Team Goldeneye"


def main(config):

    try:
        start_time = time.perf_counter()
        status = {}

        log = importlib.import_module("common.logger")
        utils = importlib.import_module("common.utils")
        spark_utils = importlib.import_module("common.spark_utils")

        logger = log.setup_logging(
            log_level=os.environ["ADG_LOG_LEVEL"].upper() if "ADG_LOG_LEVEL" in os.environ else "INFO",
            log_path=config["log_path"]
         )

        logger.info("Extract the runtime arguments passed with the process")
        args = utils.get_parameters(logger)

        logger.info("Update the configurations with run time parameter")
        config = utils.update_runtime_args_to_config(logger, args, config)

        logger.info(f'Import the job libraries for module {config["module_name"]} with correlation id: {config["correlation_id"]}')
        try:
            jobs = importlib.import_module('jobs.%s.__main__' % config["module_name"])

        except Exception as e:
            logger.error(
                "Issue while importing the module. Please check the spelling or module has not been configured yet. The error is %s". str(e)
            )
            sys.exit(1)

        logger.info(f'Extract the collection names for module {config["module_name"]} with correlation id: {config["correlation_id"]}')
        secrets_response = utils.retrieve_secrets(logger,**config)
        collections = utils.get_collections(logger, secrets_response, **config)

        logger.info(f'Create Spark Session for module {config["module_name"]} with correlation id: {config["correlation_id"]}')
        spark = spark_utils.get_spark_session(logger, **config)

        logger.info(f'Set the start and end point of the loop')
        processing_dt = datetime.strptime(config["start_date"], "%Y-%m-%d")
        end_dt = datetime.strptime(config["end_date"], "%Y-%m-%d")

        if config["clean_up_flag"] == "true":
            logger.warn("Clean up flag has been set to true, process will delete all history before starting the process")
            spark_utils.clean_up_published_bucket(logger, spark, collections, **config)

        while processing_dt <= end_dt:

            logger.info(f'update the start of batch entry into audit table {config["audit_table_name"]} for module {config["module_name"]} with correlation id: {config["correlation_id"]}')
            run_id = utils.get_log_start_of_batch(
                logger, hash_id=config["correlation_id"], processing_dt=datetime.strftime(processing_dt, "%Y-%m-%d"),
                status=config["In_Progress_Status"], **config)

            logger.info(f'starting the process for each collection for given module {config["module_name"]} with correlation id {config["correlation_id"]}')
            for collection in collections:

                logger.info("set the prefix name and other parameters requied for processing")
                s3_prefix = f"{processing_dt.date()}_{collection}"
                config["s3_src_bucket"] = utils.get_source_bucket_name(logger, secrets_response, **config)
                sts_token = utils.get_sts_token(logger, **config)

                if config["e2e_test_flag"] == "true":
                    logger.warn("End 2 End test flag has been set. This will look for file within local bucket")
                    config["s3_src_bucket"]=config['s3_published_bucket']
                    s3_prefix=os.path.join(config['e2e_test_folder'], f"{processing_dt.date()}_{collection}")
                    sts_token=None

                logger.info(f'get the list of files for {s3_prefix} in the {config["s3_src_bucket"]} for given module {config["module_name"]} with correlation id {config["correlation_id"]}')
                s3_client = utils.get_s3_client(sts_token)
                keys = utils.get_list_keys_for_prefix(logger, s3_client, s3_prefix, s3_bucket=config["s3_src_bucket"])

                if keys:
                    logger.info(f'Execute the job for given module {config["module_name"]} with correlation id {config["correlation_id"]}')
                    jobs.execute(logger, spark, keys, s3_client, config)

                else:
                    logger.warn(f'the file {s3_prefix} does not exits in the {config["s3_src_bucket"]} for given module {config["module_name"]} with correlation id {config["correlation_id"]}')
                    status["Processing_Dt"].append((datetime.strftime(processing_dt, "%Y-%m-%d"), f"File Not Found for {s3_prefix}"))
                    continue

            logger.info(f"adding the complete/failed status to audit table for correlation_id {args.correlation_id} with run id {run_id}")

            if status is None:
                logger.info(f'Update audit table  for given module {config["module_name"]} with correlation id {config["correlation_id"]}')
                utils.log_end_of_batch(
                    logger, hash_id=config["correlation_id"], processing_dt=datetime.strftime(processing_dt, "%Y-%m-%d"),
                    run_id=run_id, status=config["Completed_Status"], **config)

                utils.log_end_of_batch(
                    logger, hash_id=f'{config["correlation_id"]}_{run_id}', processing_dt=datetime.strftime(processing_dt, "%Y-%m-%d"),
                    run_id=1, status=config["Completed_Status"], **config)

                processing_dt = processing_dt + timedelta(days=1)

            else:
                for item in status["Processing_Dt"]:
                    utils.log_end_of_batch(
                        logger, hash_id=f'{config["correlation_id"]}_{run_id}',
                        processing_dt=item[1], run_id=1, status=config["Failed_Status"], **config)

                utils.log_end_of_batch(
                    logger, hash_id=config["correlation_id"],
                    processing_dt=processing_dt, run_id=run_id, status=config["Failed_Status"], **config)

                logger.info('Process is complete with exception in %s seconds' % round(end_time - start_time))
                logger.error(f"looks like 1 or more files are not present in the source bucket.Aborting the spark process")
                sys.exit(1)

        end_time = time.perf_counter()
        logger.info('Process is complete without exception in %s seconds' % round(end_time - start_time))

    except BaseException as ex:
        logger.error("Main Process Failed because of error: %s ", str(ex))
        sys.exit(1)


if __name__ == '__main__':
    config = configparser.ConfigParser()

    config = {"environment" : "${environment}",
              "aws_region" : "${aws_region_name}",
              "audit_table_name" : "${audit_table_name}",
              "audit_table_hash_key" : "${audit_table_hash_key}",
              "audit_table_range_key" : "${audit_table_range_key}",
              "audit_table_data_product_name" : "${audit_table_data_product_name}",
              "aws_secret_name" : "${aws_secret_name}",
              "published_database_name" : "${published_database_name}",
              "assume_role_within_acct_arn" : "${assume_role_within_acct_arn}",
              "assume_role_outside_acct_arn" : "${assume_role_outside_acct_arn}",
              "log_path" : "${log_path}",
              "s3_published_bucket" : "${s3_published_bucket}",
              "domain_name": "${domain_name}",
              "e2e_test_folder": "${e2e_test_folder}",
              "url" : "${url}",
              "In_Progress_Status": "In-Progress",
              "Completed_Status": "Completed",
              "Failed_Status": "Failed",
              }

    main(config)

