variable "project_name" {
  description = "Name prefix for all resources"
  type        = string
  default     = "marabunta-demo"
}

variable "region" {
  description = "Cloud region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "core_instance_type" {
  description = "Instance type for core swarm nodes (bootstrap + API)"
  type        = string
  default     = "c5.xlarge"
}

variable "scale_instance_type" {
  description = "Instance type for scale-pool nodes"
  type        = string
  default     = "c5.large"
}

variable "core_count" {
  description = "Number of core swarm nodes"
  type        = number
  default     = 3
}

variable "scale_count" {
  description = "Number of scale-pool nodes"
  type        = number
  default     = 50
}

variable "ssh_key_name" {
  description = "SSH key pair name for instance access"
  type        = string
}

variable "ssh_allowed_cidr" {
  description = "CIDR block allowed for SSH access"
  type        = string
  default     = "0.0.0.0/0"
}

variable "swarm_port" {
  description = "TCP port for swarm gossip communication"
  type        = number
  default     = 9000
}

variable "api_port" {
  description = "TCP port for the HTTP API"
  type        = number
  default     = 8080
}

variable "webapp_port" {
  description = "TCP port for the dashboard webapp"
  type        = number
  default     = 3000
}
