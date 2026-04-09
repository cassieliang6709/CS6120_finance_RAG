variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "image_tag" {
  description = "Docker image tag to deploy (e.g. 'latest' or a Git SHA)"
  type        = string
  default     = "latest"
}

variable "db_host" {
  description = "Hostname of the GCP PostgreSQL instance (T1)"
  type        = string
}

variable "db_port" {
  description = "PostgreSQL port"
  type        = string
  default     = "5432"
}

variable "db_name" {
  description = "PostgreSQL database name"
  type        = string
  default     = "financial_rag"
}

variable "db_user" {
  description = "PostgreSQL username"
  type        = string
  default     = "postgres"
}

variable "embedding_model" {
  description = "HuggingFace model ID — must match T1's ingestion model"
  type        = string
  default     = "sentence-transformers/all-MiniLM-L6-v2"
}

variable "default_k" {
  description = "Default number of chunks to return"
  type        = string
  default     = "5"
}

variable "default_alpha" {
  description = "Default hybrid search weight (1.0 = pure vector, 0.0 = pure BM25)"
  type        = string
  default     = "0.7"
}

variable "generate_sg_id" {
  description = "Security group ID of T3's generate service (allowed to call /retrieve)"
  type        = string
}
