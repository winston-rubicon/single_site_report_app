output "lambda_function_arn" {
  value = aws_lambda_function.ecs_trigger.arn
}

output "lambda_function_name" {
  value = aws_lambda_function.ecs_trigger.function_name
}