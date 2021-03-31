data "aws_iam_policy_document" "dw_ksr_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type = "AWS"
      identifiers = [aws_iam_role.kickstart_analytical_dataset_generator.arn,
      aws_iam_role.kickstart_adg_emr_service.arn]
    }

    principals {
      identifiers = ["ec2.amazonaws.com"]
      type        = "Service"
    }

    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "kickstart_cross_acc_policy" {
  statement {
    sid       = "KickstartCrossAccountAssumeRole"
    effect    = "Allow"
    actions   = ["sts:AssumeRole"]
    resources = [format("arn:aws:iam::%s:role/%s", lookup(local.source_acc_nos, lookup(local.environment_mapping, local.environment)), var.source_assume_role_name)]
  }

}

resource "aws_iam_policy" "kickstart_cross_acct_policy" {

  name        = "kickstart_cross_acc_assume_role"
  description = "This policy gives access to kickstart_s3_readonly role to assume cross account role. Please note this policy can only be used by kickstart_s3_readonly role to gain access, as other account has only trusted kickstart_s3_readonly role to establish the connectivity from dataworks"
  policy      = data.aws_iam_policy_document.kickstart_cross_acc_policy.json
}

resource "aws_iam_role" "dw_ksr_s3_readonly" {
  name               = "kickstart_s3_readonly"
  description        = "This is an IAM role which assumes role from UC side to gain temporary read access on S3 bucket for kickstart data"
  assume_role_policy = data.aws_iam_policy_document.dw_ksr_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "kickstart_cross_acct_attachment" {
  role       = aws_iam_role.dw_ksr_s3_readonly.name
  policy_arn = aws_iam_policy.kickstart_cross_acct_policy.arn
}

