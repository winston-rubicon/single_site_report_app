output "repository_url" {
  value = data.aws_ecr_repository.main.repository_url
}

output "repository_arn" {
  value = data.aws_ecr_repository.main.arn
}