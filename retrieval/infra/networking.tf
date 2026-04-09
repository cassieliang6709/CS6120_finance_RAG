# Use the default VPC — no new VPC needed for this project
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "retrieve" {
  name        = "sg-retrieve"
  description = "Security group for the RAG retrieve service"
  vpc_id      = data.aws_vpc.default.id

  # Inbound: only T3's generate service can call /retrieve
  ingress {
    description     = "Allow /retrieve calls from generate service"
    from_port       = 8000
    to_port         = 8000
    protocol        = "tcp"
    security_groups = [var.generate_sg_id]
  }

  # Outbound: GCP PostgreSQL
  egress {
    description = "Allow outbound to GCP PostgreSQL"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Outbound: ECR image pull + HuggingFace model download
  egress {
    description = "Allow HTTPS outbound (ECR pull, HuggingFace model download)"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "sg-retrieve"
  }
}

# Internal ALB so T3 has a stable DNS hostname to call
resource "aws_lb" "retrieve_internal" {
  name               = "alb-retrieve-internal"
  internal           = true
  load_balancer_type = "application"
  security_groups    = [aws_security_group.retrieve.id]
  subnets            = data.aws_subnets.default.ids
}

resource "aws_lb_target_group" "retrieve" {
  name        = "tg-retrieve"
  port        = 8000
  protocol    = "HTTP"
  vpc_id      = data.aws_vpc.default.id
  target_type = "ip"

  health_check {
    path                = "/health"
    interval            = 30
    timeout             = 5
    healthy_threshold   = 2
    unhealthy_threshold = 3
    matcher             = "200"
  }
}

resource "aws_lb_listener" "retrieve" {
  load_balancer_arn = aws_lb.retrieve_internal.arn
  port              = 8000
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.retrieve.arn
  }
}
