resource "aws_secretsmanager_secret" "db_password" {
  name                    = "rag/retrieve/db-password"
  description             = "PostgreSQL password for the RAG retrieve service"
  recovery_window_in_days = 0  # Allow immediate deletion (no 30-day lock for this project)
}

# The actual secret value is set via CLI after terraform apply (see deployment guide)
# aws secretsmanager put-secret-value \
#   --secret-id rag/retrieve/db-password \
#   --secret-string "YOUR_PASSWORD"
