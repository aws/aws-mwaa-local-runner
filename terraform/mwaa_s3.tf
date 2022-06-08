# S3 bucket for MWAA configuration.
# https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket

resource "aws_s3_bucket" "mwaa_scripts" {
  bucket = "vp-mwaa-${var.env}"
}

resource "aws_s3_bucket_acl" "mwaa_scripts_private" {
  bucket = aws_s3_bucket.mwaa_scripts.id
  acl    = "private"
}

resource "aws_s3_bucket_public_access_block" "mwaa_scripts_block" {
  bucket              = aws_s3_bucket.mwaa_scripts.id
  block_public_acls   = true
  block_public_policy = true
}

resource "aws_s3_bucket_versioning" "mwaa_scripts_version" {
  bucket = aws_s3_bucket.mwaa_scripts.id
  versioning_configuration {
    status = "Enabled"
  }

}
