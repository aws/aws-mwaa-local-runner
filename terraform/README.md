# Provisioning for VP's Pipeline

> Make sure you're in the `terraform` directory before running commands. This guide assumes that all work happens in the subdirectory rather than at the root of this repository.

## Getting started

### Prepare Terraform

[Install Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli) and make sure it's ready to roll.

```bash
cd terraform
terraform init
```

Create a file called `secrets.auto.tfvars` with AWS credentials:

```
aws_access_key = "..."
aws_secret_key = "..."
```

This file is covered by `.gitignore` so you won't have to worry about accidentally saving secrets here.

## Applying changes to AWS infra

When you run Terraform, it inspects the current state of AWS and decides what changes to make. Before applying changes **make sure you understand what it will do, or YOU COULD ACCIDENTALLY DELETE IMPORTANT SERVICES**.

```bash
terraform apply
```
