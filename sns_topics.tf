resource "aws_sns_topic" "trigger_kickstart_adg_sns" {
  name = "trigger_kickstart_adg_process"

  tags = merge(
    local.common_tags,
    {
      "Name" = "trigger_kickstart_adg_sns"
    },
  )
}

data "aws_iam_policy_document" "kickstart_adg_publish_for_trigger" {
  statement {
    sid     = "TriggerKickstartAdgSNS"
    actions = ["SNS:Publish"]
    effect  = "Allow"

    principals {
      identifiers = ["events.amazonaws.com"]
      type        = "Service"
    }
    resources = [aws_sns_topic.trigger_kickstart_adg_sns.arn]
  }
}

resource "aws_sns_topic_policy" "default" {
  arn    = aws_sns_topic.trigger_kickstart_adg_sns.arn
  policy = data.aws_iam_policy_document.kickstart_adg_publish_for_trigger.json
}
