variable "sns_topic_name" {
  description = "Name for the SNS topic"
  type        = string
  default = "single_site_report_topic"
}

variable "email_address" {
  description = "Email address for SNS notifications"
  type        = string
  default = "winston@rubicon-analytics.com"
}

variable "alarm_name" {
  description = "Name for the CloudWatch alarm"
  type        = string
  default = "single_site_report_alarm"
}

variable "lambda_function_name" {
  description = "Name of the Lambda function to monitor"
  type        = string
  default = "triggerEcsTask"
}

variable "evaluation_periods" {
  description = "Number of periods to evaluate for the alarm"
  type        = number
  default     = 1
}

variable "alarm_period" {
  description = "Duration in seconds to evaluate for the alarm"
  type        = number
  default     = 300
}

variable "error_threshold" {
  description = "Number of errors required to trigger the alarm"
  type        = number
  default     = 1
}
