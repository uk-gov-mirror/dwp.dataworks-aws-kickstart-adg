data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "kickstart_dataset_generator_write_data" {
  statement {
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]

    resources = [
      data.terraform_remote_state.common.outputs.published_bucket.arn
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:Get*",
      "s3:List*",
      "s3:DeleteObject*",
      "s3:Put*",
    ]

    resources = [
      "${data.terraform_remote_state.common.outputs.published_bucket.arn}/kickstart-analytical-dataset/*",
      "${data.terraform_remote_state.common.outputs.published_bucket.arn}/kickstart-metrics/*",
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt",
      "kms:GenerateDataKey",
      "kms:DescribeKey"
    ]

    resources = [
      "${data.terraform_remote_state.common.outputs.published_bucket_cmk.arn}",
    ]
  }
}

resource "aws_iam_policy" "kickstart_analytical_dataset_generator_write_data" {
  name        = "KickstartAnalyticalDatasetGeneratorWriteData"
  description = "Allow writing of Kickstart Analytical Dataset files"
  policy      = data.aws_iam_policy_document.kickstart_dataset_generator_write_data.json
}

data "aws_secretsmanager_secret" "kickstart_adg_secret" {
  name = local.secret_name
}

data "aws_iam_policy_document" "kickstart_analytical_dataset_secretsmanager" {
  statement {
    effect = "Allow"

    actions = [
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      data.aws_secretsmanager_secret.kickstart_adg_secret.arn
    ]
  }
}

data "aws_iam_policy_document" "kickstart_assume_role_policy" {
  statement {
    sid       = "KickstartCrossAccountAssumeRole"
    effect    = "Allow"
    actions   = ["sts:AssumeRole"]
    resources = ["arn:aws:iam::*:role/kickstart_s3_readonly"]
  }

}

resource "aws_iam_policy" "kickstart_assume_role_policy" {

  name        = "kickstart_assume_role"
  description = "This policy gives access to assume role on kickstart_s3_readonly role."
  policy      = data.aws_iam_policy_document.kickstart_assume_role_policy.json
}

resource "aws_iam_policy" "kickstart_analytical_dataset_secretsmanager" {
  name        = "KickstartDatasetGeneratorSecretsManager"
  description = "Allow reading of ADG config values"
  policy      = data.aws_iam_policy_document.kickstart_analytical_dataset_secretsmanager.json
}

resource "aws_iam_role" "kickstart_analytical_dataset_generator" {
  name               = "kickstart_analytical_dataset_generator"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_instance_profile" "kickstart_analytical_dataset_generator" {
  name = "kickstart_adg_jobflow_role"
  role = aws_iam_role.kickstart_analytical_dataset_generator.id
}

resource "aws_iam_role_policy_attachment" "kickstart_assume_role_attachment" {
  role       = aws_iam_role.kickstart_adg_emr_service.name
  policy_arn = aws_iam_policy.kickstart_assume_role_policy.arn
}

resource "aws_iam_role_policy_attachment" "ec2_for_ssm_attachment" {
  role       = aws_iam_role.kickstart_analytical_dataset_generator.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2RoleforSSM"
}

resource "aws_iam_role_policy_attachment" "amazon_ssm_managed_instance_core" {
  role       = aws_iam_role.kickstart_analytical_dataset_generator.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "kickstart_analytical_dataset_generator_ebs_cmk" {
  role       = aws_iam_role.kickstart_analytical_dataset_generator.name
  policy_arn = aws_iam_policy.kickstart_analytical_dataset_ebs_cmk_encrypt.arn
}

resource "aws_iam_role_policy_attachment" "kickstart_analytical_dataset_generator_write_data" {
  role       = aws_iam_role.kickstart_analytical_dataset_generator.name
  policy_arn = aws_iam_policy.kickstart_analytical_dataset_generator_write_data.arn
}

resource "aws_iam_role_policy_attachment" "kickstart_analytical_dataset_generator_acm" {
  role       = aws_iam_role.kickstart_analytical_dataset_generator.name
  policy_arn = aws_iam_policy.kickstart_analytical_dataset_acm.arn
}

resource "aws_iam_role_policy_attachment" "emr_kickstart_analytical_dataset_secretsmanager" {
  role       = aws_iam_role.kickstart_analytical_dataset_generator.name
  policy_arn = aws_iam_policy.kickstart_analytical_dataset_secretsmanager.arn
}

data "aws_iam_policy_document" "kickstart_analytical_dataset_generator_write_logs" {
  statement {
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]

    resources = [
      data.terraform_remote_state.security-tools.outputs.logstore_bucket.arn,
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject*",
      "s3:PutObject*",

    ]

    resources = [
      "${data.terraform_remote_state.security-tools.outputs.logstore_bucket.arn}/${local.s3_log_prefix}",
    ]
  }
}

resource "aws_iam_policy" "kickstart_analytical_dataset_generator_write_logs" {
  name        = "KickstartAnalyticalDatasetGeneratorWriteLogs"
  description = "Allow writing of Analytical Dataset logs"
  policy      = data.aws_iam_policy_document.kickstart_analytical_dataset_generator_write_logs.json
}

resource "aws_iam_role_policy_attachment" "kickstart_analytical_dataset_generator_write_logs" {
  role       = aws_iam_role.kickstart_analytical_dataset_generator.name
  policy_arn = aws_iam_policy.kickstart_analytical_dataset_generator_write_logs.arn
}

data "aws_iam_policy_document" "kickstart_analytical_dataset_generator_read_config" {
  statement {
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]

    resources = [
      data.terraform_remote_state.common.outputs.config_bucket.arn,
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "${data.terraform_remote_state.common.outputs.config_bucket.arn}/*",
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
    ]

    resources = [
      "${data.terraform_remote_state.common.outputs.config_bucket_cmk.arn}",
    ]
  }
}

resource "aws_iam_policy" "kickstart_analytical_dataset_generator_read_config" {
  name        = "KickstartAnalyticalDatasetGeneratorReadConfig"
  description = "Allow reading of Analytical Dataset config files"
  policy      = data.aws_iam_policy_document.kickstart_analytical_dataset_generator_read_config.json
}

resource "aws_iam_role_policy_attachment" "kickstart_analytical_dataset_generator_read_config" {
  role       = aws_iam_role.kickstart_analytical_dataset_generator.name
  policy_arn = aws_iam_policy.kickstart_analytical_dataset_generator_read_config.arn
}

data "aws_iam_policy_document" "kickstart_analytical_dataset_generator_read_artefacts" {
  statement {
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]

    resources = [
      data.terraform_remote_state.management_artefact.outputs.artefact_bucket.arn,
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject*",
    ]

    resources = [
      "${data.terraform_remote_state.management_artefact.outputs.artefact_bucket.arn}/*",
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
    ]

    resources = [
      data.terraform_remote_state.management_artefact.outputs.artefact_bucket.cmk_arn,
    ]
  }
}

resource "aws_iam_policy" "kickstart_analytical_dataset_generator_read_artefacts" {
  name        = "KickstartAnalyticalDatasetGeneratorReadArtefacts"
  description = "Allow reading of Analytical Dataset software artefacts"
  policy      = data.aws_iam_policy_document.kickstart_analytical_dataset_generator_read_artefacts.json
}

resource "aws_iam_role_policy_attachment" "kickstart_analytical_dataset_generator_read_artefacts" {
  role       = aws_iam_role.kickstart_analytical_dataset_generator.name
  policy_arn = aws_iam_policy.kickstart_analytical_dataset_generator_read_artefacts.arn
}

data "aws_iam_policy_document" "kickstart_analytical_dataset_generator_write_dynamodb" {
  statement {
    effect = "Allow"

    actions = [
      "dynamodb:*",
    ]

    resources = [
      "arn:aws:dynamodb:${var.region}:${local.account[local.environment]}:table/${local.data_pipeline_metadata}"
    ]
  }
}

resource "aws_iam_policy" "kickstart_analytical_dataset_generator_write_dynamodb" {
  name        = "KickstartAnalyticalDatasetGeneratorDynamoDB"
  description = "Allows read and write access to ADG's EMRFS DynamoDB table"
  policy      = data.aws_iam_policy_document.kickstart_analytical_dataset_generator_write_dynamodb.json
}

resource "aws_iam_role_policy_attachment" "kickstart_analytical_dataset_generator_dynamodb" {
  role       = aws_iam_role.kickstart_analytical_dataset_generator.name
  policy_arn = aws_iam_policy.kickstart_analytical_dataset_generator_write_dynamodb.arn
}

data "aws_iam_policy_document" "kickstart_analytical_dataset_generator_metadata_change" {
  statement {
    effect = "Allow"

    actions = [
      "ec2:ModifyInstanceMetadataOptions",
    ]

    resources = [
      "arn:aws:ec2:${var.region}:${local.account[local.environment]}:instance/*",
    ]
  }
}

resource "aws_iam_policy" "kickstart_analytical_dataset_generator_metadata_change" {
  name        = "KickstartAnalyticalDatasetGeneratorMetadataOptions"
  description = "Allow editing of Metadata Options"
  policy      = data.aws_iam_policy_document.kickstart_analytical_dataset_generator_metadata_change.json
}

resource "aws_iam_role_policy_attachment" "kickstart_analytical_dataset_generator_metadata_change" {
  role       = aws_iam_role.kickstart_analytical_dataset_generator.name
  policy_arn = aws_iam_policy.kickstart_analytical_dataset_generator_metadata_change.arn
}

resource "aws_iam_policy" "kickstart_analytical_dataset_generator_sns_alerts" {
  name        = "KickstartAnalyticalDatasetGeneratorSnsAlerts"
  description = "Allow ADG to publish SNS alerts"
  policy      = data.aws_iam_policy_document.kickstart_adg_sns_topic_policy_for_alert.json
}

resource "aws_iam_role_policy_attachment" "kickstart_analytical_dataset_generator_sns_alerts" {
  role       = aws_iam_role.kickstart_analytical_dataset_generator.name
  policy_arn = aws_iam_policy.kickstart_analytical_dataset_generator_sns_alerts.arn
}

data "aws_iam_policy_document" "kickstart_adg_sns_topic_policy_for_alert" {
  statement {
    sid = "TriggerKickstartAdgSNS"

    actions = [
      "SNS:Publish"
    ]

    effect = "Allow"

    resources = [
      data.terraform_remote_state.security-tools.outputs.sns_topic_london_monitoring.arn
    ]
  }
}
