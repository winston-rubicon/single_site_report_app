resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = var.s3_bucket_name

  lambda_function {
    lambda_function_arn = aws_lambda_function.ecs_trigger.arn
    events              = ["s3:ObjectCreated:*"]
  }
}

resource "aws_lambda_permission" "s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ecs_trigger.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.s3_bucket_name}"
}

resource "aws_lambda_function" "ecs_trigger" {
  filename      = "../lambda_trigger/single_site_report_trigger.py.zip"
  function_name = "triggerEcsTask"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "trigger_ecs_task.lambda_handler"
  runtime       = "python3.11"
  environment {
    variables = {
      SUBNET_IDS  = join(",", data.aws_subnets.selected.ids)
    }
  }
}


resource "aws_iam_role" "lambda_exec" {
  name = "lambda_exec_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_exec" {
  name   = "lambda_exec_policy"
  role   = aws_iam_role.lambda_exec.id
  policy = data.aws_iam_policy_document.lambda_exec.json
}

data "aws_iam_policy_document" "lambda_exec" {
  statement {
    effect = "Allow"
    actions = [
      "ecs:RunTask",
      "ecs:StopTask",
      "ecs:DescribeTasks"
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "ecs:cluster"
      values = [var.ecs_cluster_arn]
    }
  }

  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }

  # You might need more permissions based on the specifics of your use case.
}

