import json
import re
import base64
import argparse
import boto3
import sys
import configparser
import ast
from datetime import datetime, timedelta, date
from boto3.dynamodb.conditions import Key

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from Crypto.Cipher import AES
from Crypto.Util import Counter


def get_config(logger, path):
    try:
        config = configparser.ConfigParser()
        config.read(path)

        return config

    except BaseException as ex:
        logger.error("Error while exporting the configs because of error %s", str(ex))
        sys.exit(1)

def get_parameters(logger):
    try:
        parser = argparse.ArgumentParser(
            description="Receive args provided to spark submit job"
        )

        parser.add_argument("--module_name", type=str, required=False, default="")
        parser.add_argument("--start_dt", type=str, required=False, default="")
        parser.add_argument("--end_dt", type=str, required=False, default="")
        parser.add_argument("--clean_up_flg", type=str, required=False, default="false")
        parser.add_argument("--e2e_test_flg", type=str, required=False, default="false")


        args, unrecognized_args = parser.parse_known_args()
        if unrecognized_args:
            logger.warning("Found unknown parameters during runtime %s", str(unrecognized_args))

    except BaseException as ex:
        logger.error("Failed to get runtime parameters because of error: %s ", str(ex))
        sys.exit(-1)

    return args

def update_runtime_args_to_config(logger, args, config):
    try:
        if args.module_name.lower() not in ("vacancy", "application", "payment"):
            raise Exception("module name can be either vacancy, application or payment and re-submit the jobs using appropiate choice")
        else:
            config["module_name"] = args.module_name.lower()
            config["correlation_id"] = f"kickstart_{args.module_name}_analytical_dataset_generation"
            config["job_name"] = "kickstart"
            config["encryption_type"] = "unencrypted" if args.module_name.lower() == "vacancy" else "encrypted"

        if args.start_dt == "":
            config["start_date"] = datetime.strftime(get_last_process_dt(logger, **config), "%Y-%m-%d")
        else:
            config["start_date"] = args.start_dt

        if args.end_dt == "":
            config["end_date"] = datetime.strftime(datetime.now(), "%Y-%m-%d")
        else:
            config["end_date"] = args.end_dt

        if args.clean_up_flg == "":
            logger.warning("clean_up_flg Passed as blank, setting this variable to False as default")
            config["clean_up_flag"] = "false"
        else:
            config["clean_up_flag"] = args.clean_up_flg.lower()

        if args.e2e_test_flg == "":
            logger.warning("clean_up_flg Passed as blank, setting this variable to False as default")
            config["e2e_test_flag"] = "false"
        else:
            config["e2e_test_flag"] = args.e2e_test_flg.lower()

    except BaseException as ex:
        logger.error("Failed to update the config file because of error: %s ", str(ex))
        sys.exit(-1)

    return config


def get_client(service_name):
    client = boto3.client(service_name)
    return client

def get_s3_client(logger, sts_token=None):
    try:

        s3_client =  boto3.client('s3')
        if sts_token is not None:
            s3_client = boto3.client('s3',
                                 aws_access_key_id=sts_token['Credentials']['AccessKeyId'],
                                 aws_secret_access_key=sts_token['Credentials']['SecretAccessKey'],
                                 aws_session_token=sts_token['Credentials']['SessionToken']
                                 )
    except BaseException as ex:
        logger.error("S3 client creation failed due to error : %s ", str(ex))
        sys.exit(1)

    return s3_client


def get_resource(service_name, region):
    return boto3.resource(service_name, region_name=region)

def retrieve_secrets(logger, aws_secret_name, **kwargs):
    try:
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager")
        response = client.get_secret_value(SecretId=aws_secret_name)['SecretString']
        response_dict = ast.literal_eval(response)

    except BaseExeption as ex:
        logger.error("Failed while getting secrets from secret manager because of error: %s ", str(ex))
        sys.exit(1)

    return response_dict

def get_collections(logger, secrets_response, module_name, **kwargs):
        try:
            collections = secrets_response["collections"][module_name]

        except BaseException as ex:
            logger.error(f"Failed to get secrets because of error: %s", str(ex))
            sys.exit(-1)

        return collections

def get_source_bucket_name(logger, secrets_response, module_name, **kwargs):
    try:
        bucket_name = secrets_response["src_bucket_names"][module_name]
        return bucket_name
    except BaseException as ex:
        logger.error(f"Failed to get source bucket name because of error: %s", str(ex))
        sys.exit(-1)

def get_log_start_of_batch(
        logger, aws_region, audit_table_name, audit_table_hash_key,
        audit_table_range_key, audit_table_data_product_name,
        hash_id, processing_dt, status, **kwargs
    ):
    try:
        dynamodb=get_resource("dynamodb", region=aws_region)
        table=dynamodb.Table(audit_table_name)
        run_id = 1
        response = table.query(
            KeyConditionExpression=Key(audit_table_hash_key).eq(hash_id),
            ScanIndexForward=False            
        )
        if response['Items']:
            run_id=response["Items"][0]["Run_Id"] + 1

        put_item(
                table, audit_table_hash_key, audit_table_range_key,
                audit_table_data_product_name, hash_id, run_id, processing_dt, status)
    
    except BaseException as ex:
        logger.error("Failed to get update dynamodb table with start entry because of error: %s", str(ex))
        sys.exit(-1)
    
    return run_id

def put_item(
        table, audit_table_hash_key, audit_table_range_key,
        audit_table_data_product_name, hash_id, run_id, processing_dt, status):
    table.put_item(
        Item={
            audit_table_hash_key: hash_id,
            audit_table_range_key: audit_table_data_product_name,
            "Date": processing_dt,
            "Run_Id": run_id,
            "Status": status
        }
    )
    

def get_sts_token(
        logger, aws_region, assume_role_within_acct_arn, assume_role_outside_acct_arn, **kwargs):
    try:
        region=aws_region
        sts_within_acct = boto3.client('sts',
                                       region_name=region,
                                       endpoint_url=f'https://sts.{region}.amazonaws.com')
        sts_token_within_acct = sts_within_acct.assume_role(
            RoleArn = assume_role_within_acct_arn,
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
            RoleArn=assume_role_outside_acct_arn,
            RoleSessionName = "Assume_Role_outside_Account"
        )


    except Exception as ex:
        logger.error("Failed to generated the STS token because of error %s", str(ex))
        sys.exit(1)

    return sts_token_outside_acct

def get_list_keys_for_prefix(logger, s3_client, s3_prefix, s3_bucket):
    try:
        keys = []
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    keys.append(obj["Key"])
        if s3_prefix in keys:
            keys.remove(s3_prefix)

    except BaseException as ex:
        logger.error(f"Failed to extract the list of keys because of error: %s", str(ex))
        sys.exit(-1)

    return keys

def log_end_of_batch(
        logger, aws_region, audit_table_name, audit_table_hash_key,
        audit_table_range_key, audit_table_data_product_name,
        hash_id, processing_dt, run_id, status, **kwargs):
    try:
        dynamodb = get_resource("dynamodb", region=aws_region)
        table = dynamodb.Table(audit_table_name)
        put_item(
            table, audit_table_hash_key, audit_table_range_key,
            audit_table_data_product_name, hash_id, run_id, processing_dt, status)
    
    except BaseException as ex:
        logger.error("Failed to update the dynamodb table with final entry because of error %s",str(ex))
        sys.exit(1)

def tag_objects(logger, s3_publish_bucket, prefix, config, table, access_pii='false'):
    try:
        s3_client=boto3.client('s3')
        for key in s3_client.list_objects(Bucket=s3_publish_bucket, Prefix=prefix)["Contents"]:
            s3_client.put_object_tagging(
                Bucket=s3_publish_bucket,
                Key=key["Key"],
                Tagging={"TagSet": [{"Key": "pii", "Value": access_pii},
                                    {"Key": "db", "Value": config["published_database_name"]},
                                    {"Key": "table", "Value": table}]},
            )
    except BaseException as e:
        logger.error("Failed to tag s3 objects because of error %s", str(e))
        sys.exit(-1)

def get_last_process_dt(
        logger, aws_region, audit_table_name, audit_table_hash_key, correlation_id, **kwargs
    ):
    dynamodb=get_resource("dynamodb", region=aws_region)
    table=dynamodb.Table(audit_table_name)
    process_dt=""
    try:
        response = table.query(
            KeyConditionExpression=Key(audit_table_hash_key).eq(correlation_id),
            ScanIndexForward=False
        )

        if not response['Items']:
            process_dt = datetime.now() - timedelta(days=1)
        elif response["Items"][0]['Status'].lower() == "completed":
            process_dt = datetime.strptime(response["Items"][0]["Date"], "%Y-%m-%d") + timedelta(days=1)
        else:
            process_dt = datetime.strptime(response["Items"][0]["Date"], "%Y-%m-%d")

        logger.info("Last Process Date is %s", process_dt)
        return process_dt

    except BaseException as ex:
        logger.error("Failed to get last process date because of error: %s",str(ex))
        sys.exit(-1)

def get_bodyfor_key(logger, key, s3_client, bucket):
    try:
        s3_object = s3_client.get_object(Bucket=bucket, Key=key)

    except BaseException as ex:
        logger.error("Failed to read the content of the file because of error: %s ", str(ex))
        sys.exit(-1)

    return s3_object["Body"].read()

def get_metadatafor_key(logger, key, s3_client, bucket):
    try:
        s3_object = s3_client.get_object(Bucket=bucket, Key=key)
        iv = s3_object["Metadata"]["iv"]
        ciphertext = s3_object["Metadata"]["ciphertext"]
        datakeyencryptionkeyid = s3_object["Metadata"]["datakeyencryptionkeyid"]
        metadata = {
            "iv": iv,
            "ciphertext": ciphertext,
            "datakeyencryptionkeyid": datakeyencryptionkeyid,
        }
    except BaseException as ex:
        logger.error("Failed to extract metadata of the file because of error: %s ", str(ex))
        sys.exit(-1)

    return metadata

def get_plaintext_key_calling_dks(
        logger, encryptedkey, keyencryptionkeyid, config
):
    try:
        keys_map={}
        key = call_dks(logger, encryptedkey, keyencryptionkeyid, **config)
        keys_map[encryptedkey] = key
    except BaseException as ex:
        logger.error("Failed to get plain text key because of error: %s ", str(ex))

    return key

def call_dks(logger, cek, kek, url, correlation_id, **kwargs):
    try:
        url = url
        params = {"keyId": kek, "correlationId": correlation_id}
        result = retry_requests().post(
            url,
            params=params,
            data=cek,
            cert=(
                "/etc/pki/tls/certs/private_key.crt",
                "/etc/pki/tls/private/private_key.key",
            ),
            verify="/etc/pki/ca-trust/source/anchors/analytical_ca.pem",
        )
        content = result.json()

    except BaseException as ex:
        logger.error(
            "Failed to call DKS service because of error: %s ", str(ex)
        )
        sys.exit(1)

    return content["plaintextDataKey"]

def retry_requests(retries=10, backoff=1):
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=frozenset(['POST'])
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    requests_session = requests.Session()
    requests_session.mount("https://", adapter)
    requests_session.mount("http://", adapter)
    return requests_session

def decrypt(logger, plain_text_key, iv_key, data):
    try:
        iv_int = int(base64.b64decode(iv_key).hex(), 16)
        ctr = Counter.new(AES.block_size * 8, initial_value=iv_int)
        aes = AES.new(base64.b64decode(plain_text_key), AES.MODE_CTR, counter=ctr)
        decrypted = aes.decrypt(data)
    except BaseException as ex:
        logger.error(
            "Failed to decrypt the content because of error: %s",str(ex),
        )
        sys.exit(-1)

    return decrypted.decode("utf-8")

def get_pii_non_pii_fields(logger, data):
    try:
        fields = data["fields"]
        pii_fields, non_pii_fields = [],[]
        for field in fields:
            if str(field["pii"]).lower() == "true":
                pii_fields.append(field["fieldName"][0].lower() + re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), field["fieldName"][1:]))
            elif str(field["pii"]).lower() == "false":
                non_pii_fields.append(field["fieldName"][0].lower() + re.sub(r'(?!^)[A-Z]', lambda x: '_' + x.group(0).lower(), field["fieldName"][1:]))

    except BaseException as ex:
        logger.error("Failed to generated pii and non pii fields because of error: %s ", str(ex))
        sys.exit(1)
    return [pii_fields, non_pii_fields]

def get_initial_spark_schemas(logger,secrets_response, module_name, **kwargs):
    try:
        return secrets_response["initial_spark_schema"][module_name]
    except BaseException as ex:
        logger.error("Process failed at this step because of error: %s", str(ex))
        sys.exit(-1)
