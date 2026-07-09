terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {}

resource "docker_network" "paas_network" {
  name = "paas_network"
}

resource "docker_volume" "kafka_data" {
  name = "kafka_data"
}

resource "docker_volume" "postgres_data" {
  name = "postgres_data"
}


# --- Kafka ---
resource "docker_image" "kafka" {
  name = "apache/kafka:3.7.0"
}

resource "docker_container" "kafka" {
  name  = "kafka"
  image = docker_image.kafka.image_id

  networks_advanced {
    name = docker_network.paas_network.name
  }

  ports {
    internal = 9092
    external = 9092
  }
  ports {
    internal = 9094
    external = 9094
  }

  volumes {
    volume_name    = docker_volume.kafka_data.name
    container_path = "/var/lib/kafka/data"
  }

  env = [
    "KAFKA_NODE_ID=1",
    "KAFKA_PROCESS_ROLES=broker,controller",
    "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094",
    "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,EXTERNAL://${var.kafka_external_host}:9094",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT",
    "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
    "KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093",
    "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
    "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
    "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
  ]

  healthcheck {
    test         = ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 || exit 1"]
    interval     = "10s"
    timeout      = "10s"
    retries      = 10
    start_period = "20s"
  }

  restart = "unless-stopped"
}


# Kafka topic init - one-shot job, not a long-running service.
# Its own retry loop (until kafka-broker-api-versions.sh succeeds)
# already replaces the "wait for healthy" role Compose's
resource "docker_container" "kafka_init" {
  name  = "kafka-init"
  image = docker_image.kafka.image_id

  networks_advanced {
    name = docker_network.paas_network.name
  }

  volumes {
    host_path      = abspath("${path.module}/../../../paas/config/kafka/topics-init.sh")
    container_path = "/topics-init.sh"
    read_only      = true
  }

  entrypoint = ["bash", "/topics-init.sh"]

  must_run = false
  attach   = true
  logs     = true
  restart  = "no"

  depends_on = [docker_container.kafka]

  lifecycle {
    postcondition {
      condition     = self.exit_code == 0
      error_message = "kafka-init exited non-zero - topic creation likely failed, check `docker logs kafka-init`."
    }
  }
}


# --- Kafka UI ---
resource "docker_image" "kafka_ui" {
  name = "provectuslabs/kafka-ui:latest"
}

resource "docker_container" "kafka_ui" {
  name  = "kafka-ui"
  image = docker_image.kafka_ui.image_id

  networks_advanced {
    name = docker_network.paas_network.name
  }

  ports {
    internal = 8080
    external = 8080
  }

  env = [
    "KAFKA_CLUSTERS_0_NAME=local",
    "KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka:9092",
  ]

  depends_on = [docker_container.kafka]
  restart    = "unless-stopped"
}

# --- Postgres ---
resource "docker_image" "postgres" {
  name = "postgres:16-alpine"
}

resource "docker_container" "postgres" {
  name  = "postgres"
  image = docker_image.postgres.image_id

  networks_advanced {
    name = docker_network.paas_network.name
  }

  ports {
    internal = 5432
    external = 5432
  }

  volumes {
    volume_name    = docker_volume.postgres_data.name
    container_path = "/var/lib/postgresql/data"
  }
  volumes {
    host_path      = abspath("${path.module}/../../../paas/config/db/postgres/init.sql")
    container_path = "/docker-entrypoint-initdb.d/init.sql"
    read_only      = true
  }

  env = [
    "POSTGRES_DB=${var.postgres_db}",
    "POSTGRES_USER=${var.postgres_user}",
    "POSTGRES_PASSWORD=${var.postgres_password}",
  ]

  healthcheck {
    test     = ["CMD-SHELL", "pg_isready -U ${var.postgres_user}"]
    interval = "5s"
    timeout  = "3s"
    retries  = 5
  }

  restart = "unless-stopped"
}


# --- pgAdmin ---
resource "docker_image" "pgadmin" {
  name = "dpage/pgadmin4:latest"
}

resource "docker_container" "pgadmin" {
  name  = "pgadmin"
  image = docker_image.pgadmin.image_id

  networks_advanced {
    name = docker_network.paas_network.name
  }

  ports {
    internal = 80
    external = 5050
  }

  volumes {
    host_path      = abspath("${path.module}/../../../paas/config/db/pgadmin/servers.json")
    container_path = "/pgadmin4/servers.json"
    read_only      = true
  }

  env = [
    "PGADMIN_DEFAULT_EMAIL=${var.pgadmin_default_email}",
    "PGADMIN_DEFAULT_PASSWORD=${var.pgadmin_default_password}",
  ]

  depends_on = [docker_container.postgres]
  restart    = "unless-stopped"
}

# --- Cleaner ---
resource "docker_image" "cleaner" {
  name = "cleaner:local"
  build {
    context    = "${path.module}/../../../paas/services/cleaner"
    dockerfile = "Dockerfile"
  }
}

resource "docker_container" "cleaner" {
  name  = "cleaner"
  image = docker_image.cleaner.image_id

  networks_advanced {
    name = docker_network.paas_network.name
  }

  env = [
    "KAFKA_BOOTSTRAP=kafka:9092",
    "POSTGRES_DSN=postgresql://${var.postgres_user}:${var.postgres_password}@postgres:5432/${var.postgres_db}",
    "PYTHONUNBUFFERED=1",
  ]

  depends_on = [docker_container.kafka_init, docker_container.postgres]
  restart    = "unless-stopped"
}

# --- Grafana ---
resource "docker_image" "grafana" {
  name = "grafana/grafana:11.4.0"
}

resource "docker_container" "grafana" {
  name  = "grafana"
  image = docker_image.grafana.image_id

  networks_advanced {
    name = docker_network.paas_network.name
  }

  ports {
    internal = 3000
    external = 3000
  }

  volumes {
    host_path      = abspath("${path.module}/../../../paas/config/grafana/provisioning")
    container_path = "/etc/grafana/provisioning"
    read_only      = true
  }
  volumes {
    host_path      = abspath("${path.module}/../../../paas/config/grafana/dashboards")
    container_path = "/var/lib/grafana/dashboards"
    read_only      = true
  }

  env = [
    "GF_SECURITY_ADMIN_USER=${var.grafana_admin_user}",
    "GF_SECURITY_ADMIN_PASSWORD=${var.grafana_admin_password}",
    "POSTGRES_PASSWORD=${var.postgres_password}",
  ]

  depends_on = [docker_container.postgres]
  restart    = "unless-stopped"
}