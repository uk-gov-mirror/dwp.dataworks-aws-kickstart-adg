
data "aws_iam_policy_document" "kickstart_adg_ebs_cmk" {
  statement {
    sid     = "EnableIAMPermissionsCI"
    effect  = "Allow"

    principals {
      identifiers = [data.aws_iam_role.ci.arn]
      type        = "AWS"
    }

    actions   = [
      "kms:Create*",
      "kms:Describe*",
      "kms:Enable*",
      "kms:List*",
      "kms:Put*",
      "kms:Update*",
      "kms:Revoke*",
      "kms:Disable*",
      "kms:Get*",
      "kms:Delete*",
      "kms:ScheduleKeyDeletion",
      "kms:CancelKeyDeletion"
    ]
    resources = ["*"]
  }

  statement {
    sid     = "EnableIAMPermissionsAdministrator"
    effect  = "Allow"

    principals {
      identifiers = [data.aws_iam_role.administrator.arn]
      type        = "AWS"
    }

    actions   = [
      "kms:Create*",
      "kms:Describe*",
      "kms:Enable*",
      "kms:List*",
      "kms:Put*",
      "kms:Update*",
      "kms:Revoke*",
      "kms:Disable*",
      "kms:Get*",
      "kms:Delete*",
      "kms:ScheduleKeyDeletion",
      "kms:CancelKeyDeletion"
    ]
    resources = ["*"]
  }

  statement {
    sid    = "DenyCIEncryptDecrypt"
    effect = "Deny"

    principals {
      type        = "AWS"
      identifiers = [data.aws_iam_role.ci.arn]
    }

    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ImportKeyMaterial",
      "kms:ReEncryptFrom",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "EnableAWSConfigManagerScanForSecurityHub"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = [data.aws_iam_role.aws_config.arn]
    }

    actions = [
      "kms:Describe*",
      "kms:Get*",
      "kms:List*"
    ]

    resources = ["*"]
  }

  statement {
    sid    = "EnableIAMPermissionsAnalyticDatasetGen"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.kickstart_adg_emr_service.arn, aws_iam_role.kickstart_analytical_dataset_generator.arn]
    }

    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey"
    ]

    resources = ["*"]

  }

  statement {
    sid    = "AllowADGServiceGrant"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.kickstart_adg_emr_service.arn, aws_iam_role.kickstart_analytical_dataset_generator.arn]
    }

    actions = ["kms:CreateGrant"]

    resources = ["*"]

    condition {
      test     = "Bool"
      variable = "kms:GrantIsForAWSResource"
      values   = ["true"]
    }
  }

}

resource "aws_kms_key" "kickstart_adg_ebs_cmk" {
  description             = "Encrypts Kickstart ADG EBS volumes"
  deletion_window_in_days = 7
  is_enabled              = true
  enable_key_rotation     = true
  policy                  =  data.aws_iam_policy_document.kickstart_adg_ebs_cmk.json

  # ProtectsSensitiveData = "True" - the ADG cluster decrypts sensitive data
  # that it reads from HBase. It can potentially spill this to disk if it can't
  # hold it all in memory, which is likely given the size of the dataset.
  tags = merge(
    local.common_tags,
    {
      Name                  = "kickstart_adg_ebs_cmk"
      ProtectsSensitiveData = "True"
    }
  )
}

resource "aws_kms_alias" "kickstart_adg_ebs_cmk" {
  name          = "alias/kickstart_adg_ebs_cmk"
  target_key_id = aws_kms_key.kickstart_adg_ebs_cmk.key_id
}

data "aws_iam_policy_document" "kickstart_analytical_dataset_ebs_cmk_encrypt" {
  statement {
    effect = "Allow"

    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey",
    ]

    resources = [aws_kms_key.kickstart_adg_ebs_cmk.arn]
  }

  statement {
    effect = "Allow"

    actions = ["kms:CreateGrant"]

    resources = [aws_kms_key.kickstart_adg_ebs_cmk.arn]
    condition {
      test     = "Bool"
      variable = "kms:GrantIsForAWSResource"
      values   = ["true"]
    }
  }
}

resource "aws_iam_policy" "kickstart_analytical_dataset_ebs_cmk_encrypt" {
  name        = "KickstartAnalyticalDatasetGeneratorEbsCmkEncrypt"
  description = "Allow encryption and decryption using the kickstart Analytical Dataset Generator EBS CMK"
  policy      = data.aws_iam_policy_document.kickstart_analytical_dataset_ebs_cmk_encrypt.json
}
