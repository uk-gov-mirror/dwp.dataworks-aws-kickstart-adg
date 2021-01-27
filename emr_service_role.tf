data "aws_iam_policy_document" "emr_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "kickstart_adg_emr_service" {
  name               = "kickstart_adg_emr_service_role"
  assume_role_policy = data.aws_iam_policy_document.emr_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "emr_attachment" {
  role       = aws_iam_role.kickstart_adg_emr_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role_policy_attachment" "kickstart_adg_emr_service_ebs_cmk" {
  role       = aws_iam_role.kickstart_adg_emr_service.name
  policy_arn = aws_iam_policy.kickstart_analytical_dataset_ebs_cmk_encrypt.arn
}
