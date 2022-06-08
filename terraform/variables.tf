variable "aws_access_key" {}
variable "aws_secret_key" {}

variable "aws_account_id" {
  # NOTE: This is VP's account ID.
  default = 725561212619
}

variable "env" {
  default = "dev"
}

# NOTE: MWAA has limited region support; see the list:
# https://docs.aws.amazon.com/mwaa/latest/userguide/what-is-mwaa.html#regions-mwaa
variable "region" {
  default = "us-west-2"
}

variable "region_az1" {
  default = "us-west-2a"
}

variable "region_az2" {
  default = "us-west-2b"
}

variable "vpc_cidr" {
  default = "10.10.0.0/16"
}

variable "emr_subnet_cidr" { default = "10.10.0.0/24" }

variable "public_subnet1_cidr" { default = "10.10.10.0/24" }

variable "public_subnet2_cidr" { default = "10.10.11.0/24" }

variable "private_subnet1_cidr" { default = "10.10.20.0/24" }

variable "private_subnet2_cidr" { default = "10.10.21.0/24" }
