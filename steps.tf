data "archive_file" "spark_steps" {
  type        = "zip"
  source_dir  = "${path.module}/steps/spark"
  output_path = "${path.module}/temp/jobs.zip"
}

resource "aws_s3_bucket_object" "spark_steps_py_files" {
  bucket  = data.terraform_remote_state.common.outputs.config_bucket.id
  key     = "component/kickstart-analytical-dataset-generation/steps/spark/jobs.zip"
  source  = data.archive_file.spark_steps.output_path
  etag    = filemd5(data.archive_file.spark_steps.output_path)
}

resource "aws_s3_bucket_object" "spark_steps_main" {
  bucket  = data.terraform_remote_state.common.outputs.config_bucket.id
  key     = "component/kickstart-analytical-dataset-generation/steps/spark/main.py"
  content = templatefile("${path.module}/steps/spark/main.py",
    {
      environment                   = local.environment
      aws_region_name               = var.region
      audit_table_name              = "data_pipeline_metadata"
      audit_table_hash_key          = "Correlation_Id"
      audit_table_range_key         = "Run_Id"
      audit_table_data_product_name = "KICKSTART-ADG"
      aws_secret_name               = local.secret_name
      published_database_name       = local.published_db
      assume_role_within_acct_arn   = aws_iam_role.dw_ksr_s3_readonly.arn
      assume_role_outside_acct_arn  = format("arn:aws:iam::%s:role/%s", lookup(local.source_acc_nos, lookup(local.environment_mapping, local.environment)), var.source_assume_role_name)
      log_path                      = "/var/log/kickstart_adg/generate-analytical-dataset.log"
      s3_published_bucket           = data.terraform_remote_state.adg.outputs.published_bucket.id
      sns_monitoring_topic          = data.terraform_remote_state.security-tools.outputs.sns_topic_london_monitoring.arn
      domain_name                   = local.kickstart_adg_prefix[local.environment]
    }
  )
}
