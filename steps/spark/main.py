import os
import importlib
import time
import configparser

from common.logger import setup_logging
from common.utils import get_parameters, get_config
from common.spark_utils import get_spark_session

__author__ = "Team Goldeneye"

if __name__ == '__main__':

    config = configparser.ConfigParser()

    config["DEFAULT"] = {"environment" : "${environment}",
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
                         "sns_monitoring_topic" : "${sns_monitoring_topic}",
                         "domain_name": "${domain_name}",
                         "In_Progress_Status": "In-Progress",
                         "Completed_Status": "Completed",
                         "Failed_Status": "Failed"}

    the_logger = setup_logging(
        log_level=os.environ["ADG_LOG_LEVEL"].upper() if "ADG_LOG_LEVEL" in os.environ else "INFO",
        log_path=config["DEFAULT"]["log_path"]
    )    

    args = get_parameters(the_logger)
    
    the_logger.info("Processing spark job for correlation id: %s" % args.correlation_id)
    
    spark = get_spark_session(args.job_name, args.module_name)
    
    the_logger.info("Created the spark session for correlation id: %s" % args.correlation_id)
    
    the_logger.info("Import the module %s to run" % args.correlation_id)
    
    try:

        job = importlib.import_module('jobs.%s.__main__' % args.module_name)
    except Exception as e:
        the_logger.error(
            "Issue while importing the module. Please check the spelling or module has not been configured yet. The error is %s". str(e)
            )
    
    start_time = time.perf_counter()
    
    job.execute(the_logger, 
                spark, 
                args,
                config)
    
    end_time = time.perf_counter()
    
    the_logger.info('Process is complete in %s seconds' % round(end_time - start_time))

    spark.stop()
    
