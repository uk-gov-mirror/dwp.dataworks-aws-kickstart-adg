data "aws_iam_policy_document" "dw_ksr_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "dw_ksr_s3_readonly" {
  name               = "kickstart_s3_readonly"
  description        = "This is an IAM role which assumes role from UC side to gain temporary read access on S3 bucket for kickstart data"
  assume_role_policy = data.aws_iam_policy_document.dw_ksr_assume_role.json
  tags               = local.common_tags
}

