resource "aws_s3_bucket_object" "spark_steps_common_init" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/kickstart-analytical-dataset-generation/steps/spark/common/__init__.py"
  content = file("${path.module}/steps/spark/common/__init__.py")
}

resource "aws_s3_bucket_object" "spark_steps_common_logger" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/kickstart-analytical-dataset-generation/steps/spark/common/logger.py"
  content = file("${path.module}/steps/spark/common/logger.py")
}

resource "aws_s3_bucket_object" "spark_steps_common_spark_utils" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/kickstart-analytical-dataset-generation/steps/spark/common/spark_utils.py"
  content = file("${path.module}/steps/spark/common/spark_utils.py")
}

resource "aws_s3_bucket_object" "spark_steps_common_utils" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/kickstart-analytical-dataset-generation/steps/spark/common/utils.py"
  content = file("${path.module}/steps/spark/common/utils.py")
}

resource "aws_s3_bucket_object" "spark_steps_jobs_init" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/kickstart-analytical-dataset-generation/steps/spark/jobs/__init__.py"
  content = file("${path.module}/steps/spark/jobs/__init__.py")
}

resource "aws_s3_bucket_object" "spark_steps_jobs_vacancy_init" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/kickstart-analytical-dataset-generation/steps/spark/jobs/vacancy/__init__.py"
  content = file("${path.module}/steps/spark/jobs/vacancy/__init__.py")
}

resource "aws_s3_bucket_object" "spark_steps_jobs_vacancy_main" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/kickstart-analytical-dataset-generation/steps/spark/jobs/vacancy/__main__.py"
  content = file("${path.module}/steps/spark/jobs/vacancy/__main__.py")
}

resource "aws_s3_bucket_object" "spark_steps_jobs_application_init" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/kickstart-analytical-dataset-generation/steps/spark/jobs/application/__init__.py"
  content = file("${path.module}/steps/spark/jobs/vacancy/__init__.py")
}

resource "aws_s3_bucket_object" "spark_steps_jobs_application_main" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/kickstart-analytical-dataset-generation/steps/spark/jobs/application/__main__.py"
  content = file("${path.module}/steps/spark/jobs/vacancy/__main__.py")
}

resource "aws_s3_bucket_object" "spark_steps_main" {
  bucket = data.terraform_remote_state.common.outputs.config_bucket.id
  key    = "component/kickstart-analytical-dataset-generation/steps/spark/main.py"
  content = templatefile("${path.module}/steps/spark/main.py",
    {
      environment                   = local.environment
      aws_region_name               = var.region
      audit_table_name              = "data_pipeline_metadata"
      audit_table_hash_key          = "Correlation_Id"
      audit_table_range_key         = "DataProduct"
      audit_table_data_product_name = "KICKSTART-ADG"
      aws_secret_name               = local.secret_name
      published_database_name       = local.published_db
      assume_role_within_acct_arn   = aws_iam_role.dw_ksr_s3_readonly.arn
      assume_role_outside_acct_arn  = format("arn:aws:iam::%s:role/%s", lookup(local.source_acc_nos, lookup(local.environment_mapping, local.environment)), var.source_assume_role_name)
      log_path                      = "/var/log/kickstart_adg/generate-analytical-dataset.log"
      s3_published_bucket           = data.terraform_remote_state.common.outputs.published_bucket.id
      domain_name                   = local.kickstart_adg_prefix[local.environment],
      e2e_test_folder               = "kickstart-e2e-tests"
      url                           = format("%s/datakey/actions/decrypt", data.terraform_remote_state.crypto.outputs.dks_endpoint[local.environment])
    }
  )
}
