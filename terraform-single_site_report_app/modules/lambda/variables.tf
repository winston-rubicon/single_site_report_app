variable "s3_bucket_name"{
    description = "Name of S3 bucket to trigger Single Site Report Generation"
    type = string
    default = "ncs-washindex-single-site-reports-815867481426"
}

variable "ecs_cluster_arn" {
  description = "ARN of the ECS cluster where tasks will be run"
  type        = string
}