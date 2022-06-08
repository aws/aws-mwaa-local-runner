# https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-presto.html

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region     = var.region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key

  default_tags {
    tags = {
      Name = "MWAA Test"
      Env  = var.env
    }
  }
}
