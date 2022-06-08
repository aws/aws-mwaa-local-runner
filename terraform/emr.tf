# EMR
# https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/emr_cluster

resource "aws_emr_cluster" "emr_pipeline" {
  applications  = ["Trino"]
  name          = "pipeline_${var.env}"
  release_label = "emr-6.6.0"
  service_role  = aws_iam_role.emr_service_role.arn

  ec2_attributes {
    emr_managed_master_security_group = aws_security_group.open_access.id
    emr_managed_slave_security_group  = aws_security_group.open_access.id
    instance_profile                  = aws_iam_instance_profile.emr_profile.arn
    subnet_id                         = aws_subnet.emr_pipeline.id
  }

  master_instance_group {
    instance_type = "m5.xlarge"
  }

  core_instance_group {
    instance_count = 1
    instance_type  = "m5.xlarge"
  }
}
