# Scale-pool nodes: auto-scaling group for swarm workers.

resource "aws_launch_template" "scale" {
  name_prefix   = "${var.project_name}-scale-"
  image_id      = data.aws_ami.ubuntu.id
  instance_type = var.scale_instance_type
  key_name      = var.ssh_key_name

  network_interfaces {
    associate_public_ip_address = true
    security_groups             = [aws_security_group.swarm.id]
  }

  block_device_mappings {
    device_name = "/dev/sda1"
    ebs {
      volume_size = 30
      volume_type = "gp3"
    }
  }

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name = "${var.project_name}-scale"
      Role = "scale"
    }
  }
}

resource "aws_autoscaling_group" "scale" {
  name                = "${var.project_name}-scale-asg"
  desired_capacity    = var.scale_count
  min_size            = 0
  max_size            = var.scale_count * 2
  vpc_zone_identifier = [aws_subnet.public.id]

  launch_template {
    id      = aws_launch_template.scale.id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "${var.project_name}-scale"
    propagate_at_launch = true
  }
}

output "scale_asg_name" {
  value = aws_autoscaling_group.scale.name
}
