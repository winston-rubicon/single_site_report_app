data "aws_iam_policy_document" "ecs_tasks" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket",
    ]
    resources = [
      "arn:aws:s3:::ncs-washindex-single-site-reports-815867481426",
      "arn:aws:s3:::ncs-washindex-single-site-reports-815867481426/*"
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "sqs:SendMessage",
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
    ]
    resources = [
      "*"
    ]
  }
  statement {
      effect = "Allow"
      actions = [
        "sns:Publish",
      ]
      resources = [
        "*"
      ]
    }
  statement {
    effect = "Allow"
    actions = [
      "ecr:GetAuthorizationToken",
      "ecr:BatchCheckLayerAvailability",
      "ecr:GetDownloadUrlForLayer",
      "ecr:GetRepositoryPolicy",
      "ecr:DescribeRepositories",
      "ecr:ListImages",
      "ecr:DescribeImages",
      "ecr:BatchGetImage"
    ]
    resources = ["*"]


  }
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["${aws_cloudwatch_log_group.ecs_log_group.arn}:*"]
  }
}



resource "aws_iam_role" "ecs_task_execution_role" {
  # replace with your preferred IAM role name
  name = "single_site_report_app_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "ecs_tasks" {
  name   = "single_site_report_app_role"
  role   = aws_iam_role.ecs_task_execution_role.id
  policy = data.aws_iam_policy_document.ecs_tasks.json
}

resource "aws_ecs_cluster" "main" {
  name = "single_site_report_app_cluster"
}


resource "aws_ecs_service" "main" {
  name            = "single_site_report_app_service" 
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.main.arn
   # adjust as needed
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.subnet_ids
    assign_public_ip = true
    security_groups  = [var.security_group_id]
  }
}


# Add this resource block to create the CloudWatch log group
resource "aws_cloudwatch_log_group" "ecs_log_group" {
  name = "/ecs/single_site_report_app_log_group"
   # Adjust the retention period as needed
  retention_in_days = 7
}


resource "aws_ecs_task_definition" "main" {
  family                   = "single_site_report_app_task_definition"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "1024"
  memory                   = "4096"
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_execution_role.arn
  
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture = "X86_64"
  }

  container_definitions = <<DEFINITION
    [
      {
        "name": "single_site_report_app_container",
        "image": "${var.ecr_repository_url}",
        "essential": true,
        "logConfiguration": {
          "logDriver": "awslogs",
          "options": {
            "awslogs-group": "/ecs/single_site_report_app_log_group",
            "awslogs-region": "us-east-1",
            "awslogs-stream-prefix": "ecs"
          }
        }
      }
    ]
  DEFINITION
}
