import json
import argparse
import boto3
import sys
import configparser
import ast
from datetime import datetime, timedelta, date
from boto3.dynamodb.conditions import Key

def get_config(path):
    config = configparser.ConfigParser()
    config.read(path)
    return config

def get_parameters(logger):
    parser = argparse.ArgumentParser(
        description="Receive args provided to spark submit job"
    )

    parser.add_argument("--correlation_id", type=str, required=False, default="kickstart_vacancy_analytical_dataset_generation")
    parser.add_argument("--job_name", type=str, required=False, default="kicstart")
    parser.add_argument("--module_name", type=str, required=False, default="vacancy")
    parser.add_argument("--start_dt", type=str, required=False, default="")
    parser.add_argument("--end_dt", type=str, required=False, default="")
    parser.add_argument("--clean_up_flg", type=bool, required=False, default=False)
    parser.add_argument("--e2e_test_flg", type=bool, required=False, default=False)

    args, unrecognized_args = parser.parse_known_args()
    return args

def get_client(service_name):
    client = boto3.client(service_name)
    return client

def get_resource(service_name, region):
    return boto3.resource(service_name, region_name=region)

def retrieve_secrets(secret_name):
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)['SecretString']
    response_dict = ast.literal_eval(response)
    return response_dict

def get_collections(logger, secrets_response, correlation_id, module_name):
        try:
            collections = secrets_response["collections"][module_name]
            return collections
        except:
            logger.error(f"Problem with collections list for correlation Id: {correlation_id}")
            sys.exit(-1)

def get_source_bucket_name(logger, secrets_response, correlation_id, module_name):
    try:
        bucket_name = secrets_response["src_bucket_names"][module_name]
        return bucket_name
    except Exception as ex:
        logger.error(f"Problem in getting bucket name for correlation Id: {correlation_id} because of error {str(ex)}")
        sys.exit(-1)

def get_log_start_of_batch(logger, processing_dt, args, config):

    logger.info("Updating Audit table with start status for correlation id %s", args.correlation_id)
    dynamodb=get_resource("dynamodb", region=config["DEFAULT"]["aws_region"])
    table=dynamodb.Table(config["DEFAULT"]["audit_table_name"])
    run_id = 1
    try:
        response = table.query(
            KeyConditionExpression=Key(config["DEFAULT"]["audit_table_hash_key"]).eq(args.correlation_id),
            ScanIndexForward=False            
        )
        if not response['Items']:
            put_item(table, run_id, processing_dt, args, config, status=config["DEFAULT"]["In_Progress_Status"])
        else:
            run_id=response["Items"][0]["Run_Id"] + 1
            put_item(table, run_id, processing_dt, args, config, status=config["DEFAULT"]["In_Progress_Status"])
    
    except BaseException as ex:
        logger.error("Problem updating audit table status for correlation id : %s %s",
        args.correlation_id,str(ex))
        sys.exit(-1)
    
    return run_id

def put_item(table, run_id, processing_dt, args, config, status):
    table.put_item(
        Item={
            config["DEFAULT"]["audit_table_hash_key"]: args.correlation_id,
            config["DEFAULT"]["audit_table_range_key"]: config["DEFAULT"]["audit_table_data_product_name"],
            "Date": processing_dt,
            "Run_Id": run_id,
            "Status": status
        }
    )
    

def get_sts_token(logger, config):
    try:
        region=config["DEFAULT"]["aws_region"]
        sts_within_acct = boto3.client('sts',
                                       region_name=region,
                                       endpoint_url=f'https://sts.{region}.amazonaws.com')
        sts_token_within_acct = sts_within_acct.assume_role(
            RoleArn = config["DEFAULT"]["assume_role_within_acct_arn"],
            RoleSessionName = "Assume_Role_within_Account",
        )
        sts_outside_acct = boto3.client('sts',
            aws_access_key_id=sts_token_within_acct['Credentials']['AccessKeyId'],
            aws_secret_access_key=sts_token_within_acct['Credentials']['SecretAccessKey'],
            aws_session_token=sts_token_within_acct['Credentials']['SessionToken'],
            region_name=region,
            endpoint_url=f'https://sts.{region}.amazonaws.com'
        )
        sts_token_outside_acct = sts_outside_acct.assume_role(
            RoleArn=config['DEFAULT']['assume_role_outside_acct_arn'],
            RoleSessionName = "Assume_Role_outside_Account"
        )
    
        return sts_token_outside_acct

    except Exception as ex:
        logger.error("Failed to generated the STS token because of error %s", str(ex))
        sys.exit(-1)

def get_list_keys_for_prefix(s3_bucket, s3_prefix,sts_token):
    keys = []
    s3_client = boto3.client('s3')
    if sts_token is not None:
        s3_client = boto3.client('s3',
            aws_access_key_id=sts_token['Credentials']['AccessKeyId'],
            aws_secret_access_key=sts_token['Credentials']['SecretAccessKey'],
            aws_session_token=sts_token['Credentials']['SessionToken']
    )
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                keys.append(obj["Key"])
    if s3_prefix in keys:
        keys.remove(s3_prefix)
    
    return keys

def log_end_of_batch(logger, run_id, processing_dt, args, config, status):
    
    logger.info("Updating audit table with end status for correlation_id %s", args.correlation_id)
    
    try:
        dynamodb = get_resource("dynamodb", region=config["DEFAULT"]["aws_region"])
        data_pipeline_metadata = config['DEFAULT']['audit_table_name']
        table = dynamodb.Table(data_pipeline_metadata)
        put_item(table, run_id, processing_dt, args, config, status)
    
    except BaseException as ex:
        logger.error("Problem updating audit table end status for correlation id: %s and run id: %s %s",
        args.correlation_id,
        run_id,
        str(ex)
        )
        sys.exit(-1)

def tag_objects(logger, s3_publish_bucket, prefix, config, table, access_pii='false'):
    try:
        s3_client=boto3.client('s3')
        for key in s3_client.list_objects(Bucket=s3_publish_bucket, Prefix=prefix)["Contents"]:
            s3_client.put_object_tagging(
                Bucket=s3_publish_bucket,
                Key=key["Key"],
                Tagging={"TagSet": [{"Key": "pii", "Value": access_pii},
                                    {"Key": "db", "Value": config["DEFAULT"]["published_database_name"]},
                                    {"Key": "table", "Value": table}]},
            )
    except BaseException as e:
        logger.error("Issue while tagging the s3 objects because of error %s", str(e))
        sys.exit(-1)

def get_last_process_dt(logger, args, config):
    logger.info("Getting the last process dt from audit table for correlation id %s", args.correlation_id)
    dynamodb=get_resource("dynamodb", region=config["DEFAULT"]["aws_region"])
    table=dynamodb.Table(config["DEFAULT"]["audit_table_name"])
    process_dt=""
    try:
        response = table.query(
            KeyConditionExpression=Key(config["DEFAULT"]["audit_table_hash_key"]).eq(args.correlation_id),
            ScanIndexForward=False
        )

        if not response['Items']:
            process_dt = datetime.now() - timedelta(days=1)
        elif response["Items"][0]['Status'].lower() == "completed":
            process_dt = datetime.strptime(response["Items"][0]["Date"], "%Y-%m-%d") + timedelta(days=1)
        else:
            process_dt = datetime.strptime(response["Items"][0]["Date"], "%Y-%m-%d")

        logger.info("Process Date is %s", process_dt)

        return process_dt

    except BaseException as ex:
        logger.error("Problem updating audit table end status for correlation id: %s: %s",
                     args.correlation_id,
                     str(ex)
                     )
        sys.exit(-1)
