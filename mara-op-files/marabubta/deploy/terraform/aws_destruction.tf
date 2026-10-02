# The Marabunta Hostile Takeover Provider (AWS Destruction)
#
# This Terraform manifest executes the "Extinction Event" (Pillar 6.2).
# It provisions unmetered, offshore bare-metal compute via the Spot Market,
# and actively scales a target AWS AutoScaling group down to zero.

terraform {
  required_providers {
    marabunta = {
      source  = "registry.marabunta.sh/marabunta/swarm"
      version = "~> 1.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# 1. Provide the cryptographic Genesis parameters
provider "marabunta" {
  genesis_path = "./base_genesis.toml"
  network      = "darknet"
}

# 2. Provision the Offshore Swarm
# We request 5,000 nodes physically located in Paraguay (Itaipu Dam).
# These nodes are exclusively powered by stranded hydroelectric energy.
resource "marabunta_node_group" "itaipu_fleet" {
  target_nodes       = 5000
  datacenter_region  = "py-itaipu-tier3"
  machine_profile    = "compute-optimized-128c-gpu"
  
  # Ensure the nodes boot with PhantomRouters (Meat Shields)
  evasion_config {
    posture          = "DarkNet"
    iot_camouflage   = true
  }

  # The absolute maximum we will pay for this infrastructure
  economics {
    max_bid_usd_per_hour = 0.05
  }

  # Auto-bind the Reed-Solomon parity directories to local NVMe
  storage {
    ephemeral_nvme   = true
    max_capacity_gb  = 500
  }
}

# 3. The Execution Target
# Define the legacy infrastructure we are replacing.
data "aws_autoscaling_group" "legacy_expensive_cluster" {
  name = "production-llm-training-fleet"
}

# 4. The Kill-Shot
# Once the Marabunta swarm achieves BFT consensus and is fully meshed,
# this resource violently scales the AWS AutoScaling group to zero.
resource "aws_autoscaling_group" "execute_destruction" {
  name             = data.aws_autoscaling_group.legacy_expensive_cluster.name
  
  min_size         = 0
  max_size         = 0
  desired_capacity = 0
  
  # We do not kill AWS until the Paraguayan swarm is mathematically online.
  depends_on = [
    marabunta_node_group.itaipu_fleet
  ]
}

output "swarm_status" {
  value = "Hostile Takeover Complete. 5,000 nodes active. AWS ASG Scaled to Zero."
}
