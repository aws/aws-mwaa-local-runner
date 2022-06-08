# AWS security groups
# Used for enabling network traffic to the VPC.

resource "aws_security_group" "open_access" {
  depends_on  = [aws_subnet.emr_pipeline]
  description = "Allow all inbound traffic"
  name        = "open_access"
  vpc_id      = aws_vpc.pipeline.id

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [aws_vpc.pipeline.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    ignore_changes = [
      ingress,
      egress,
    ]
  }
}
