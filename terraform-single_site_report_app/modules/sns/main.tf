resource "aws_sns_topic" "lambda_failure_notifications" {
  name = var.sns_topic_name
}

resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.lambda_failure_notifications.arn
  protocol  = "email"
  endpoint  = var.email_address
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors_alarm" {
  alarm_name          = var.alarm_name
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = var.evaluation_periods
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = var.alarm_period
  statistic           = "Sum"
  threshold           = var.error_threshold
  alarm_description   = "Triggered when lambda has errors"
  alarm_actions       = [aws_sns_topic.lambda_failure_notifications.arn]

  dimensions = {
    FunctionName = var.lambda_function_name
  }
}