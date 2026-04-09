output "ecr_repository_url" {
  description = "ECR repository URL — use this when tagging and pushing the Docker image"
  value       = aws_ecr_repository.retrieve.repository_url
}

output "retrieve_internal_url" {
  description = "Internal URL for T3 to set as RETRIEVE_URL environment variable"
  value       = "http://${aws_lb.retrieve_internal.dns_name}:8000"
}

output "ecs_cluster_name" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.rag.name
}

output "ecs_service_name" {
  description = "ECS service name"
  value       = aws_ecs_service.retrieve.name
}
