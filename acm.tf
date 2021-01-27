resource "aws_acm_certificate" "kickstart-analytical-dataset-generator" {
  certificate_authority_arn = data.terraform_remote_state.aws_certificate_authority.outputs.root_ca.arn
  domain_name               = "kickstart-analytical-dataset-generator.${local.env_prefix[local.environment]}${local.dataworks_domain_name}"

  options {
    certificate_transparency_logging_preference = "ENABLED"
  }
}

data "aws_iam_policy_document" "kickstart_analytical_dataset_acm" {
  statement {
    effect = "Allow"

    actions = [
      "acm:ExportCertificate",
    ]

    resources = [
      aws_acm_certificate.kickstart-analytical-dataset-generator.arn
    ]
  }
}

resource "aws_iam_policy" "kickstart_analytical_dataset_acm" {
  name        = "ACMExportKickstartDatasetGeneratorCert"
  description = "Allow export of Dataset Generator certificate"
  policy      = data.aws_iam_policy_document.kickstart_analytical_dataset_acm.json
}
