data "aws_caller_identity" "current" {}

resource "aws_cloudwatch_log_group" "retrieve" {
  name              = "/rag/retrieve"
  retention_in_days = 7
}

resource "aws_ecs_cluster" "rag" {
  name = "rag-cluster"
}

resource "aws_ecs_task_definition" "retrieve" {
  family                   = "rag-retrieve"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024  # 1 vCPU
  memory                   = 2048  # 2 GB

  execution_role_arn = aws_iam_role.ecs_task_execution.arn
  task_role_arn      = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "retrieve"
      image     = "${aws_ecr_repository.retrieve.repository_url}:${var.image_tag}"
      essential = true

      portMappings = [
        { containerPort = 8000, protocol = "tcp" }
      ]

      environment = [
        { name = "DB_HOST",          value = var.db_host },
        { name = "DB_PORT",          value = var.db_port },
        { name = "DB_NAME",          value = var.db_name },
        { name = "DB_USER",          value = var.db_user },
        { name = "EMBEDDING_MODEL",  value = var.embedding_model },
        { name = "DEFAULT_K",        value = var.default_k },
        { name = "DEFAULT_ALPHA",    value = var.default_alpha },
      ]

      secrets = [
        {
          name      = "DB_PASSWORD"
          valueFrom = aws_secretsmanager_secret.db_password.arn
        }
      ]

      healthCheck = {
        command     = ["CMD-SHELL", "curl -f http://localhost:8000/health || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 60  # Allow time for sentence-transformers model to load
      }

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.retrieve.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "retrieve"
        }
      }
    }
  ])
}

resource "aws_ecs_service" "retrieve" {
  name            = "rag-retrieve"
  cluster         = aws_ecs_cluster.rag.id
  task_definition = aws_ecs_task_definition.retrieve.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = data.aws_subnets.default.ids
    security_groups  = [aws_security_group.retrieve.id]
    assign_public_ip = true  # Required for ECR pull without a NAT gateway
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.retrieve.arn
    container_name   = "retrieve"
    container_port   = 8000
  }

  depends_on = [aws_lb_listener.retrieve]

  # Prevent Terraform from resetting desired_count during manual scaling
  lifecycle {
    ignore_changes = [desired_count]
  }
}
