variable "edge_host_ip" {
  description = "IP address of the edge lab machine"
  type        = string
}

variable "edge_ssh_user" {
  description = "SSH username for the edge lab machine"
  type        = string
  default     = "alex"
}