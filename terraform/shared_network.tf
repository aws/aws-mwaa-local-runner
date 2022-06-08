# Shared VPC for pipeline services

resource "aws_vpc" "pipeline" {
  cidr_block           = "10.10.0.0/16"
  enable_dns_hostnames = true
}

resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.pipeline.id
}

resource "aws_route_table" "r" {
  vpc_id = aws_vpc.pipeline.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.gw.id
  }
}

resource "aws_main_route_table_association" "a" {
  vpc_id         = aws_vpc.pipeline.id
  route_table_id = aws_route_table.r.id
}
