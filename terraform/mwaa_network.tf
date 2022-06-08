# cribbed from:
# https://gist.github.com/takemikami/c2e7800e2a64e3bf75b881f8c7f5d33d

resource "aws_subnet" "mwaa_private_1" {
  availability_zone = var.region_az1
  cidr_block        = var.private_subnet1_cidr
  tags              = { Name = "MWAA ${var.env} private subnet 1" }
  vpc_id            = aws_vpc.pipeline.id
}

resource "aws_subnet" "mwaa_private_2" {
  availability_zone = var.region_az2
  cidr_block        = var.private_subnet2_cidr
  tags              = { Name = "MWAA ${var.env} private subnet 2" }
  vpc_id            = aws_vpc.pipeline.id
}

resource "aws_subnet" "mwaa_public_1" {
  availability_zone       = var.region_az1
  cidr_block              = var.public_subnet1_cidr
  map_public_ip_on_launch = true
  tags                    = { Name = "MWAA ${var.env} public subnet 1" }
  vpc_id                  = aws_vpc.pipeline.id
}

resource "aws_subnet" "mwaa_public_2" {
  availability_zone       = var.region_az2
  cidr_block              = var.public_subnet2_cidr
  map_public_ip_on_launch = true
  tags                    = { Name = "MWAA ${var.env} public subnet 2" }
  vpc_id                  = aws_vpc.pipeline.id
}
