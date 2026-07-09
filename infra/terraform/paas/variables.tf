variable "kafka_external_host" {
  description = "Reachable LAN IP advertised to external (edge) Kafka clients"
  type        = string
}

variable "postgres_db" {
  type = string
}

variable "postgres_user" {
  type = string
}

variable "postgres_password" {
  type      = string
  sensitive = true
}

variable "pgadmin_default_email" {
  type = string
}

variable "pgadmin_default_password" {
  type      = string
  sensitive = true
}

variable "grafana_admin_user" {
  type = string
}

variable "grafana_admin_password" {
  type      = string
  sensitive = true
}