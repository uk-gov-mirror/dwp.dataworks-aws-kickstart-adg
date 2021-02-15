import importlib
import sys
from datetime import datetime, timedelta, date

def execute(logger, spark, args, config):
    
    logger.info(f"Importing the required libraries")
    
    utils = importlib.import_module("common.utils")
    spark_utils = importlib.import_module("common.spark_utils")
    test_datagen = importlib.import_module("datagen.__main__")

    logger.info(f"Extracting the collection names correlation id {args.correlation_id}")
    secrets_response = utils.retrieve_secrets(config["DEFAULT"]["aws_secret_name"])
    collections = utils.get_collections(logger, secrets_response, args.correlation_id, args.module_name)

    logger.info(f"checking for cleanup flag. It has been mark as {args.clean_up_flg}")

    if args.clean_up_flg:
        utils.clean_up_s3_prefix(logger, config['DEFAULT']['s3_published_bucket'], f"{config['DEFAULT']['domain_name']}/non-pii/")


    logger.info(f"Calculating the start and end date of catchup mode for correlation id {args.correlation_id}")

    if args.start_dt:
        processing_dt = datetime.strptime(args.start_dt, "%Y-%m-%d")
        end_dt = datetime.strptime(args.end_dt, "%Y-%m-%d").date()
    else:
        processing_dt = utils.get_last_process_dt(logger, args, config)
        end_dt = date.today()

    while processing_dt.date() <= end_dt:

        logger.info("The processing date is %s", processing_dt)
        
        logger.info(f"Make start entry to audit table for correlation id {args.correlation_id}")

        processing_dt_str = datetime.strftime(processing_dt, "%Y-%m-%d")

        run_id = utils.get_log_start_of_batch(logger, processing_dt_str, args, config)

        status = []

        for collection in collections:
            
            s3_prefix = f"{processing_dt.date()}_{collection}"
            s3_client = utils.get_client('s3')
            s3_remote_bucket = utils.get_source_bucket_name(logger, secrets_response, args.correlation_id, args.module_name)
            
            logger.info(f"checking the file {s3_prefix} exits in the {s3_remote_bucket} for correlation id {args.correlation_id}")
            
            sts_token = utils.get_sts_token(logger, config)
            keys = utils.get_list_keys_for_prefix(sts_token, s3_remote_bucket, s3_prefix)
            
            if keys:
                logger.info(f"the file {s3_prefix} exits in the {s3_remote_bucket} for correlation id {args.correlation_id}")
                path =  f"s3://{s3_remote_bucket}/{keys[-1]}"
                df = spark_utils.read_csv_with_inferschema(logger, spark, sts_token, path, run_id, processing_dt_str, args, config)
            else:
                logger.warn(f"the file {s3_prefix} does not exits in the {s3_remote_bucket} for correlation id {args.correlation_id}")
                status.append(False)
                continue

            logger.info(f"Apply Transformation to the sourced dataframe for {args.correlation_id}")

            df_with_transformation = spark_utils.transformation(logger, spark, df, run_id, processing_dt_str, args, config)

            logger.info(f"writing the file to destination bucket for correlation id {args.correlation_id}")
                
            destination_bucket = config['DEFAULT']['s3_published_bucket']
            domain_name=config['DEFAULT']['domain_name']
            destination_folder = f"{domain_name}/non-pii/{collection}/"
            destination_path   = f"s3://{destination_bucket}/{destination_folder}"
                
            response = spark_utils.writer_parquet(logger, spark, df_with_transformation, destination_path, run_id, processing_dt_str, args, config)
                
            if response:
                logger.info(f"data loaded into in the destination_bucket for correlation_id {args.correlation_id}")


            logger.info(f"creating the hive table on the destination_path for correlation_id {args.correlation_id} with run id {run_id}")
                
            response = spark_utils.create_hive_tables_on_published(logger, 
                                                                    spark,
                                                                    collection,
                                                                    df_with_transformation,
                                                                    destination_path,
                                                                    run_id,
                                                                    processing_dt_str,
                                                                    args,
                                                                    config)
            if response:
                logger.info(f"the hive table created successfully on the destination_path for correlation_id {args.correlation_id} with run id {run_id}")

            logger.info(f"adding the tag to objects {args.correlation_id} with run id {run_id}")

            utils.tag_objects(logger, destination_bucket, destination_folder, config, collection, access_pii='false' )

        logger.info(f"adding the complete/failed status to audit table for correlation_id {args.correlation_id} with run id {run_id}")

        if all(status):
            utils.log_end_of_batch(logger, run_id, processing_dt_str, args, config, status=config["DEFAULT"]["Completed_Status"])

        else:
            utils.log_end_of_batch(logger, run_id, processing_dt_str, args, config, status=config["DEFAULT"]["Failed_Status"])
            logger.error(f"looks like 1 or more files are not present in the source bucket.Aborting the spark process")
            raise Exception("One or more file not found in the source bucket with proccessing date as %s", processing_dt_str)

        logger.info(f"Process is complete for correlation_id {args.correlation_id} with run_id {run_id}")
        
        processing_dt = processing_dt + timedelta(days=1)
