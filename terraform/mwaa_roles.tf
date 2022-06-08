# cribbed from:
# https://gist.github.com/takemikami/c2e7800e2a64e3bf75b881f8c7f5d33d

resource "aws_iam_role" "mwaa_execution_role" {
  name = "mwaa_execution_role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": [
          "airflow.amazonaws.com",
          "airflow-env.amazonaws.com"
        ]
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "mwaa_execution_policy" {
  name = "mwaa_execution_policy"
  role = aws_iam_role.mwaa_execution_role.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "airflow:PublishMetrics",
      "Resource": "arn:aws:airflow:${var.region}:${var.aws_account_id}:environment/${var.env}"
    },
    { 
      "Effect": "Deny",
      "Action": [ 
        "s3:ListAllMyBuckets"
      ],
      "Resource": [
        "${aws_s3_bucket.mwaa_scripts.arn}",
        "${aws_s3_bucket.mwaa_scripts.arn}/*"
      ]
    },
    { 
      "Effect": "Allow",
      "Action": [ 
        "s3:GetObject*",
        "s3:GetBucket*",
        "s3:List*"
      ],
      "Resource": [
        "${aws_s3_bucket.mwaa_scripts.arn}",
        "${aws_s3_bucket.mwaa_scripts.arn}/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:CreateLogGroup",
        "logs:PutLogEvents",
        "logs:GetLogEvents",
        "logs:GetLogRecord",
        "logs:GetLogGroupFields",
        "logs:GetQueryResults"
      ],
      "Resource": [
        "arn:aws:logs:${var.region}:${var.aws_account_id}:log-group:airflow-${var.env}-*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:DescribeLogGroups"
      ],
      "Resource": [
        "*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "cloudwatch:PutMetricData",
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ChangeMessageVisibility",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
        "sqs:GetQueueUrl",
        "sqs:ReceiveMessage",
        "sqs:SendMessage"
      ],
      "Resource": "arn:aws:sqs:${var.region}:*:airflow-celery-*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kms:Decrypt",
        "kms:DescribeKey",
        "kms:GenerateDataKey*",
        "kms:Encrypt"
      ],
      "NotResource": "arn:aws:kms:*:${var.aws_account_id}:key/*",
      "Condition": {
        "StringLike": {
          "kms:ViaService": [
            "sqs.${var.region}.amazonaws.com"
          ]
        }
      }
    }
  ]
}
EOF
}
