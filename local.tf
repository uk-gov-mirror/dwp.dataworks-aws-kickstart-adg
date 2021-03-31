locals {
  emr_cluster_name      = "kickstart-analytical-dataset-generator"
  master_instance_type  = "m5.2xlarge"
  master_instance_count = 1
  core_instance_type    = "m5.2xlarge"
  core_instance_count   = 1
  task_instance_type    = "m5.2xlarge"
  task_instance_count   = 0
  dks_port              = 8443
  secret_name           = "/kickstart/adg"

  env_certificate_bucket = "dw-${local.environment}-public-certificates"
  dks_endpoint           = data.terraform_remote_state.crypto.outputs.dks_endpoint[local.environment]

  crypto_workspace = {
    management-dev = "management-dev"
    management     = "management"
  }

  management_workspace = {
    management-dev = "default"
    management     = "management"
  }

  management_account = {
    development = "management-dev"
    qa          = "management-dev"
    integration = "management-dev"
    preprod     = "management"
    production  = "management"
  }

  root_dns_name = {
    development = "dev.dataworks.dwp.gov.uk"
    qa          = "qa.dataworks.dwp.gov.uk"
    integration = "int.dataworks.dwp.gov.uk"
    preprod     = "pre.dataworks.dwp.gov.uk"
    production  = "dataworks.dwp.gov.uk"
  }

  kickstart_adg_emr_lambda_schedule = {
    development = "0 5 * * ? 2029"
    qa          = "0 5 * * ? 2029"
    integration = "0 19 * * ? 2029"
    preprod     = "0 5 * * ? 2029"
    production  = "0 5 * * ? *"
  }

  kickstart_adg_log_level = {
    development = "DEBUG"
    qa          = "DEBUG"
    integration = "DEBUG"
    preprod     = "INFO"
    production  = "INFO"
  }

  kickstart_adg_version = {
    development = "0.0.1"
    qa          = "0.0.1"
    integration = "0.0.1"
    preprod     = "0.0.1"
    production  = "0.0.1"
  }

  amazon_region_domain = "${data.aws_region.current.name}.amazonaws.com"
  endpoint_services    = ["dynamodb", "ec2", "ec2messages", "glue", "kms", "logs", "monitoring", ".s3", "s3", "secretsmanager", "ssm", "ssmmessages", "sts"]
  no_proxy             = "169.254.169.254,${join(",", formatlist("%s.%s", local.endpoint_services, local.amazon_region_domain))}"

  ebs_emrfs_em = {
    EncryptionConfiguration = {
      EnableInTransitEncryption = false
      EnableAtRestEncryption    = true
      AtRestEncryptionConfiguration = {

        S3EncryptionConfiguration = {
          EncryptionMode             = "CSE-Custom"
          S3Object                   = "s3://${data.terraform_remote_state.management_artefact.outputs.artefact_bucket.id}/emr-encryption-materials-provider/encryption-materials-provider-all.jar"
          EncryptionKeyProviderClass = "uk.gov.dwp.dataworks.dks.encryptionmaterialsprovider.DKSEncryptionMaterialsProvider"
        }
        LocalDiskEncryptionConfiguration = {
          EnableEbsEncryption       = true
          EncryptionKeyProviderType = "AwsKms"
          AwsKmsKey                 = aws_kms_key.kickstart_adg_ebs_cmk.arn
        }
      }
    }
  }

  keep_cluster_alive = {
    development = true
    qa          = false
    integration = false
    preprod     = false
    production  = false
  }

  step_fail_action = {
    development = "CONTINUE"
    qa          = "TERMINATE_CLUSTER"
    integration = "TERMINATE_CLUSTER"
    preprod     = "TERMINATE_CLUSTER"
    production  = "TERMINATE_CLUSTER"
  }

  cw_agent_namespace             = "/app/kickstart_analytical_dataset_generator"
  cw_agent_log_group_name        = "/app/kickstart_analytical_dataset_generator"
  cw_agent_bootstrap_loggrp_name = "/app/kickstart_analytical_dataset_generator/bootstrap_actions"
  cw_agent_steps_loggrp_name     = "/app/kickstart_analytical_dataset_generator/step_logs"
  cw_agent_yarnspark_loggrp_name = "/app/kickstart_analytical_dataset_generator/yarn-spark_logs"
  cw_agent_e2e_loggroup_name     = "/app/kickstart_analytical_dataset_generator/e2e_logs"

  cw_agent_metrics_collection_interval = 60

  s3_log_prefix = "emr/kickstart_analytical_dataset_generator"

  data_pipeline_metadata = data.terraform_remote_state.internal_compute.outputs.data_pipeline_metadata_dynamo.name

  published_db = "uc_kickstart"

  kickstart_adg_prefix = {
    development = "kickstart-analytical-dataset"
    qa          = "kickstart-analytical-dataset"
    integration = "kickstart-analytical-dataset"
    preprod     = "kickstart-analytical-dataset"
    production  = "kickstart-analytical-dataset"
  }

  kickstart_adg_retention_days = {
    development = 1
    qa          = 1
    integration = 1
    preprod     = 20
    production  = 20
  }

  env_prefix = {
    development = "dev."
    qa          = "qa."
    stage       = "stg."
    integration = "int."
    preprod     = "pre."
    production  = ""
  }

  environment_mapping = {
    development = "development"
    qa          = "qa"
    integration = "qa2"
    preprod     = "test"
    production  = "prod"
  }

  hive_data_location = "data"

}
