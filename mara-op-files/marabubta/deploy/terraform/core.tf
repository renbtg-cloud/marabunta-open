# Core swarm nodes: bootstrap + API servers.

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
}

resource "aws_instance" "core" {
  count = var.core_count

  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.core_instance_type
  key_name               = var.ssh_key_name
  subnet_id              = aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.swarm.id]

  root_block_device {
    volume_size = 50
    volume_type = "gp3"
  }

  tags = {
    Name = "${var.project_name}-core-${count.index}"
    Role = "core"
  }
}

# --- Outputs ---

output "core_public_ips" {
  value = aws_instance.core[*].public_ip
}

output "core_private_ips" {
  value = aws_instance.core[*].private_ip
}

output "api_endpoint" {
  value = "http://${aws_instance.core[0].public_ip}:${var.api_port}"
}

output "dashboard_url" {
  value = "http://${aws_instance.core[0].public_ip}:${var.webapp_port}"
}
