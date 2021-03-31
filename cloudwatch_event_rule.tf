resource "aws_cloudwatch_event_rule" "kickstart_adg_sns_topic_schedule" {
  name                = "kickstart_adg_trigger_message_sns"
  description         = "Triggers Kickstart ADG process by publishing the message to sns topic"
  schedule_expression = format("cron(%s)", local.kickstart_adg_emr_lambda_schedule[local.environment])
}

resource "aws_cloudwatch_event_target" "kickstart_adg_sns_topic_target" {
  rule      = aws_cloudwatch_event_rule.kickstart_adg_sns_topic_schedule.name
  target_id = "kickstart_adg_trigger_message_sns"
  arn       = aws_sns_topic.trigger_kickstart_adg_sns.arn
  input     = <<DOC
{
"additional_step_args": {
  "submit-job-vacancy": ["--module_name", "vacancy"],
  "submit-job-application": ["--module_name", "application"],
  "submit-job-payment": ["--module_name", "payment"]
}
}
DOC
}
