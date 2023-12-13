provider "aws" {
  # replace with your preferred AWS region
  region = "us-east-1"
}

resource "aws_vpc" "my_vpc" {
  cidr_block = "10.0.0.0/24"

  tags = {
    Name = "MyVPC"
  }
}

resource "aws_subnet" "subnet_1" {
  vpc_id            = aws_vpc.my_vpc.id
  cidr_block        = "10.0.0.0/25"  # Adjust CIDR block as needed
  availability_zone = "us-east-1a"   # Adjust AZ as needed

  tags = {
    Name = "MySubnet1"
  }
}

resource "aws_subnet" "subnet_2" {
  vpc_id            = aws_vpc.my_vpc.id
  cidr_block        = "10.0.0.128/25"  # Adjust CIDR block as needed
  availability_zone = "us-east-1b"     # Adjust AZ as needed

  tags = {
    Name = "MySubnet2"
  }
}

resource "aws_internet_gateway" "my_vpc_igw" {
  vpc_id = aws_vpc.my_vpc.id

  tags = {
    Name = "my-vpc-internet-gateway"
  }
}

resource "aws_route_table" "public_route_table" {
  vpc_id = aws_vpc.my_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.my_vpc_igw.id
  }

  tags = {
    Name = "public-route-table"
  }
}

# Associate the public route table with your public subnets
resource "aws_route_table_association" "public_subnet_1_association" {
  subnet_id      = aws_subnet.subnet_1.id
  route_table_id = aws_route_table.public_route_table.id
}

resource "aws_route_table_association" "public_subnet_2_association" {
  subnet_id      = aws_subnet.subnet_2.id
  route_table_id = aws_route_table.public_route_table.id
}


# data "aws_vpc" "selected" {
#   # id = "vpc-09603ff12c1a05fd1" 
#   # id = "vpc-05801cab419f25388"

# }

# data "aws_subnets" "selected" {
#   filter {
#     name   = "vpc-id"
#     # values = [data.aws_vpc.selected.id]
#     values = [aws_vpc.my_vpc.id]
#   }
# }

# data "aws_security_group" "selected" {
#   filter {
#     name   = "vpc-id"
#     # values =  [data.aws_vpc.selected.id]
#     values = [aws_vpc.my_vpc.id]
#   }
# }

resource "aws_security_group" "ecs_tasks_sg" {
  name        = "ecs-tasks-sg"
  description = "Security group for ECS tasks"
  vpc_id      = aws_vpc.my_vpc.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"  # Allow all protocols
    cidr_blocks = ["0.0.0.0/0"]  # Allow all destinations
  }

  tags = {
    Name = "ecs-tasks-sg"
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
  # vpc_id = data.aws_vpc.selected.id
  vpc_id = aws_vpc.my_vpc.id
  # subnet_ids = data.aws_subnets.selected.ids
  subnet_ids = [aws_subnet.subnet_1.id, aws_subnet.subnet_2.id]
  ecr_repository_url = "${module.ecr.repository_url}:latest"
  ecr_repository_arn = module.ecr.repository_arn
  # security_group_id = data.aws_security_group.selected.id
  security_group_id = aws_security_group.ecs_tasks_sg.id
}

module "lambda" {
  source = "./modules/lambda"
  s3_bucket_name  = var.s3_bucket_name
  ecs_cluster_arn = module.ecs.ecs_cluster_arn
  # subnet_ids      = data.aws_subnets.selected.ids
  subnet_ids = [aws_subnet.subnet_1.id, aws_subnet.subnet_2.id]
}

module "lambda_failure_notifications" {
  source               = "./modules/sns"
  sns_topic_name       = "single_site_report_topic"
  email_address        = "winston@rubicon-analytics.com"
  alarm_name           = "single_site_report_alarm"
  lambda_function_name = module.lambda.lambda_function_name
}

resource "aws_sqs_queue" "single_site_report_queue" {
  name                      = "single_site_report_queue"
  delay_seconds             = 5
  max_message_size          = 2048
  message_retention_seconds = 86400
  receive_wait_time_seconds = 5
}