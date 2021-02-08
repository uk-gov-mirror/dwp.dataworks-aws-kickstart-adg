resource "aws_emr_security_configuration" "ebs_emrfs_em" {
  name          = "kickstart_adg_ebs_emrfs"
  configuration = jsonencode(local.ebs_emrfs_em)
}

resource "aws_s3_bucket_object" "cluster" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "emr/kickstart_adg/cluster.yaml"
  content = templatefile("${path.module}/cluster_config/cluster.yaml.tpl",
    {
      s3_log_bucket          = data.terraform_remote_state.security-tools.outputs.logstore_bucket.id
      s3_log_prefix          = local.s3_log_prefix
      ami_id                 = var.emr_ami_id
      service_role           = aws_iam_role.kickstart_adg_emr_service.arn
      instance_profile       = aws_iam_instance_profile.kickstart_analytical_dataset_generator.arn
      security_configuration = aws_emr_security_configuration.ebs_emrfs_em.id
      emr_release            = var.emr_release[local.environment]
    }
  )
}

resource "aws_s3_bucket_object" "instances" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "emr/kickstart_adg/instances.yaml"
  content = templatefile("${path.module}/cluster_config/instances.yaml.tpl",
    {
      keep_cluster_alive  = local.keep_cluster_alive[local.environment]
      add_master_sg       = aws_security_group.kickstart_adg_common.id
      add_slave_sg        = aws_security_group.kickstart_adg_common.id
      subnet_ids          = join(",", data.terraform_remote_state.internal_compute.outputs.kickstart_adg_subnet.ids)
      master_sg           = aws_security_group.kickstart_adg_master.id
      slave_sg            = aws_security_group.kickstart_adg_slave.id
      service_access_sg   = aws_security_group.kickstart_adg_emr_service.id
      instance_type       = var.emr_instance_type[local.environment]
      core_instance_count = var.emr_core_instance_count[local.environment]
    }
  )
}

resource "aws_s3_bucket_object" "steps" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "emr/kickstart_adg/steps.yaml"
  content = templatefile("${path.module}/cluster_config/steps.yaml.tpl",
    {
      s3_config_bucket  = data.terraform_remote_state.common.outputs.config_bucket.id
      action_on_failure = local.step_fail_action[local.environment]
    }
  )
}

# See https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
locals {
  spark_num_cores_per_node            = var.emr_num_cores_per_core_instance[local.environment] - 1
  spark_num_nodes                     = local.core_instance_count + local.task_instance_count + local.master_instance_count
  spark_executor_cores                = var.num_cores_per_executor[local.environment]
  spark_total_avaliable_cores         = local.spark_num_cores_per_node * local.spark_num_nodes
  spark_total_avaliable_executors     = ceil(local.spark_total_avaliable_cores/local.spark_executor_cores) - 1
  spark_num_executors_per_instance    = ceil(local.spark_total_avaliable_executors/local.spark_num_nodes)
  spark_executor_total_memory         = floor(var.ram_memory_per_node[local.environment]/local.spark_num_executors_per_instance) - 10
  spark_executor_memoryOverhead       = ceil(local.spark_executor_total_memory * 0.10)
  spark_executor_memory               = floor(local.spark_executor_total_memory - local.spark_executor_memoryOverhead)
  spark_driver_memory                 = 1
  spark_driver_cores                  = 1
  spark_default_parallelism           = local.spark_num_executors_per_instance * local.spark_executor_cores * 2
  spark_kyro_buffer                   = var.spark_kyro_buffer[local.environment]
}

data "aws_secretsmanager_secret" "rds_aurora_secrets" {
  provider = aws
  name     = "metadata-store-kickstart-adg-writer"
}


resource "aws_s3_bucket_object" "configurations" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "emr/kickstart_adg/configurations.yaml"
  content = templatefile("${path.module}/cluster_config/configurations.yaml.tpl",
    {
      s3_log_bucket                       = data.terraform_remote_state.security-tools.outputs.logstore_bucket.id
      s3_log_prefix                       = local.s3_log_prefix
      s3_published_bucket                 = data.terraform_remote_state.common.outputs.published_bucket.arn
      proxy_no_proxy                      = replace(replace(local.no_proxy, ",", "|"), ".s3", "*.s3")
      proxy_http_host                     = data.terraform_remote_state.internal_compute.outputs.internet_proxy.host
      proxy_http_port                     = data.terraform_remote_state.internal_compute.outputs.internet_proxy.port
      proxy_https_host                    = data.terraform_remote_state.internal_compute.outputs.internet_proxy.host
      proxy_https_port                    = data.terraform_remote_state.internal_compute.outputs.internet_proxy.port
      spark_executor_cores                = local.spark_executor_cores
      spark_executor_memory               = local.spark_executor_memory
      spark_executor_memoryOverhead       = local.spark_executor_memoryOverhead
      spark_driver_memory                 = local.spark_driver_memory
      spark_driver_cores                  = local.spark_driver_cores
      spark_executor_instances            = local.spark_num_executors_per_instance
      spark_default_parallelism           = local.spark_default_parallelism
      spark_kyro_buffer                   = local.spark_kyro_buffer
      hive_metsatore_username             = var.metadata_store_adg_writer_username
      hive_metastore_pwd                  = data.aws_secretsmanager_secret.rds_aurora_secrets.name
      hive_metastore_endpoint             = data.terraform_remote_state.adg.outputs.hive_metastore.rds_cluster.endpoint
      hive_metastore_database_name        = data.terraform_remote_state.adg.outputs.hive_metastore.rds_cluster.database_name
      }
  )
}

