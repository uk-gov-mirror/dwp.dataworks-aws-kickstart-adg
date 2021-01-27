resource "aws_cloudwatch_event_rule" "kickstart_adg_terminated_with_errors_rule" {
  name          = "kickstart_adg_terminated_with_errors_rule"
  description   = "Sends failed message to slack when adg cluster terminates with errors"
  event_pattern = <<EOF
{
  "source": [
    "aws.emr"
  ],
  "detail-type": [
    "EMR Cluster State Change"
  ],
  "detail": {
    "state": [
      "TERMINATED_WITH_ERRORS"
    ],
    "name": [
      "kickstart-analytical-dataset-generator"
    ]
  }
}
EOF
}

resource "aws_cloudwatch_metric_alarm" "kickstart_adg_failed_with_errors" {
  alarm_name                = "kickstart_adg_failed_with_errors"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "TriggeredRules"
  namespace                 = "AWS/Events"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "1"
  alarm_description         = "This metric monitors cluster termination with errors"
  insufficient_data_actions = []
  alarm_actions             = [data.terraform_remote_state.security-tools.outputs.sns_topic_london_monitoring.arn]
  dimensions = {
    RuleName = aws_cloudwatch_event_rule.kickstart_adg_terminated_with_errors_rule.name
  }
  tags = merge(
    local.common_tags,
    {
      Name              = "kickstart_failed_with_errors",
      notification_type = "Error"
      severity          = "Critical"
    },
  )
}

resource "aws_cloudwatch_event_rule" "kickstart_adg_success" {
  name          = "kickstart_adg_success"
  description   = "checks that all steps complete"
  event_pattern = <<EOF
{
  "source": [
    "aws.emr"
  ],
  "detail-type": [
    "EMR Cluster State Change"
  ],
  "detail": {
    "state": [
      "TERMINATED"
    ],
    "name": [
      "kickstart-analytical-dataset-generator"
    ],
    "stateChangeReason": [
      "{\"code\":\"ALL_STEPS_COMPLETED\",\"message\":\"Steps completed\"}"
    ]
  }
}
EOF
}


resource "aws_cloudwatch_metric_alarm" "kickstart_adg_success" {
  alarm_name                = "kickstart_adg_completed_all_steps"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  metric_name               = "TriggeredRules"
  namespace                 = "AWS/Events"
  period                    = "60"
  statistic                 = "Sum"
  threshold                 = "1"
  alarm_description         = "Monitoring adg completion"
  insufficient_data_actions = []
  alarm_actions             = [data.terraform_remote_state.security-tools.outputs.sns_topic_london_monitoring.arn]
  dimensions = {
    RuleName = aws_cloudwatch_event_rule.kickstart_adg_success.name
  }
  tags = merge(
    local.common_tags,
    {
      Name              = "kickstart_completed_all_steps",
      notification_type = "Information",
      severity          = "Critical"
    },
  )
}
