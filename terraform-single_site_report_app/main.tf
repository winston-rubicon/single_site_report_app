provider "aws" {
  # replace with your preferred AWS region
  region = "us-east-1"
}

data "aws_vpc" "selected" {
  id = "vpc-09603ff12c1a05fd1" 
}

data "aws_subnets" "selected" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.selected.id]
  }
}

data "aws_security_group" "selected" {
  filter {
    name   = "vpc-id"
    values =  [data.aws_vpc.selected.id]
  }
}

# module "network" {
#   source = "./modules/network"
# }

module "ecr" {
  source = "./modules/ecr"
  ecs_task_execution_role_arn = module.ecs.ecs_task_execution_role_arn
}

module "ecs" {
  source = "./modules/ecs"
  vpc_id = data.aws_vpc.selected.id
  subnet_ids = data.aws_subnets.selected.ids
  ecr_repository_url = "${module.ecr.repository_url}:latest"
  ecr_repository_arn = module.ecr.repository_arn
  security_group_id = data.aws_security_group.selected.id
}

resource "aws_sqs_queue" "single_site_report_queue" {
  name                      = "single_site_report_queue"
  delay_seconds             = 5
  max_message_size          = 2048
  message_retention_seconds = 86400
  receive_wait_time_seconds = 5
}

resource "aws_sns_topic" "single_site_report_topic" {
  name = "single_site_report-topic"
}
