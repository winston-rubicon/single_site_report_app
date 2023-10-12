# retrieve information from the existing repo
data "aws_ecr_repository" "main" {
  name = "single_site_report_app"
}


data "aws_iam_policy_document" "ecr_policy" {
  statement {
    sid       = "new statement"
    effect    = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::815867481426:role/single_site_report_app_role"]
    }
    actions   = ["ecr:*"]
  }
}


# apply policy
resource "aws_ecr_repository_policy" "ecr_policy" {
  repository = data.aws_ecr_repository.main.name
  policy     = data.aws_iam_policy_document.ecr_policy.json
}
