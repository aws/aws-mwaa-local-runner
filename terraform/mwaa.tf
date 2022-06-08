# MWAA
# https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/mwaa_environment
# See also:
# https://gist.github.com/takemikami/c2e7800e2a64e3bf75b881f8c7f5d33d

resource "aws_mwaa_environment" "mwaa_pipeline" {
  dag_s3_path           = "dags/"
  execution_role_arn    = aws_iam_role.mwaa_execution_role.arn
  name                  = "mwaa_pipeline_${var.env}"
  source_bucket_arn     = aws_s3_bucket.mwaa_scripts.arn
  webserver_access_mode = "PUBLIC_ONLY"

  network_configuration {
    security_group_ids = [aws_security_group.open_access.id]
    subnet_ids         = [aws_subnet.mwaa_private_1.id, aws_subnet.mwaa_private_2.id]
  }
}
