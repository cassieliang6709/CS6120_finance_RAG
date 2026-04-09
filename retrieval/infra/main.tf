terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Optional: use S3 backend for shared state across teammates
  # backend "s3" {
  #   bucket = "your-terraform-state-bucket"
  #   key    = "rag/retrieve/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

provider "aws" {
  region = var.aws_region
}
