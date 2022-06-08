resource "aws_subnet" "emr_pipeline" {
  cidr_block = var.emr_subnet_cidr
  vpc_id     = aws_vpc.pipeline.id
}
