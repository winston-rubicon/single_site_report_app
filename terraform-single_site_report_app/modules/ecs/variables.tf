variable "aws_region" {
  description = "The AWS region to deploy your resources in"
  type        = string
  default     = "us-east-1"
}

variable "vpc_id" {
  description = "The ID of the VPC"
  type        = string
}

variable "subnet_ids" {
  description = "The IDs of the subnets that the ECS tasks will be launched in"
  type = list(string)
}

variable "ecr_repository_url" {
  description = "The URL of the ECR repository that hosts your Docker image"
  type        = string
}

variable "ecr_repository_arn" {
  description = "The ARN of the ECR repository"
  type        = string
}

variable "security_group_id" {
  description = "The ID of the security group that allows all outbound traffic"
  type        = string
}