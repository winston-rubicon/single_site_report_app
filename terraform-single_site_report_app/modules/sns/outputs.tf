output "sns_topic_arn" {
  description = "ARN of the SNS topic"
  value       = aws_sns_topic.lambda_failure_notifications.arn
}