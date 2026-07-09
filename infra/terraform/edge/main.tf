terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
  }
}

provider "docker" {
  host = "ssh://${var.edge_ssh_user}@${var.edge_host_ip}:22"
}

resource "docker_network" "edge_network" {
  name = "iot_network"
}

resource "docker_volume" "mosquitto_data" {
  name = "mosquitto_data"
}

resource "docker_volume" "mosquitto_log" {
  name = "mosquitto_log"
}

resource "docker_image" "mosquitto" {
  name = "eclipse-mosquitto:2.0.22"
}

resource "docker_container" "broker_mqtt" {
  name  = "mosquitto"
  image = docker_image.mosquitto.image_id

  networks_advanced {
    name    = docker_network.edge_network.name
    aliases = ["broker_mqtt"]
  }

  ports {
    internal = 1883
    external = 1883
  }

  volumes {
    volume_name    = docker_volume.mosquitto_data.name
    container_path = "/mosquitto/data"
  }

  volumes {
    volume_name    = docker_volume.mosquitto_log.name
    container_path = "/mosquitto/log"
  }

  upload {
    content = file("${path.module}/../../../edge/mosquitto/config/mosquitto.conf")
    file    = "/mosquitto/config/mosquitto.conf"
  }

  healthcheck {
    test     = ["CMD-SHELL", "mosquitto_sub -t '$SYS/#' -C 1 -i healthcheck -W 3 || exit 1"]
    interval = "5s"
    timeout  = "3s"
    retries  = 5
  }

  restart = "unless-stopped"
}


# --- iot_devices ---
resource "docker_image" "iot_devices" {
  name = "iot_devices:local"
  build {
    context    = "${path.module}/../../../edge/iot_devices"
    dockerfile = "Dockerfile"
  }
}

resource "docker_container" "iot_devices" {
  name  = "iot_devices"
  image = docker_image.iot_devices.image_id

  networks_advanced {
    name = docker_network.edge_network.name
  }

  env = [
    "BROKER_HOST=broker_mqtt",
    "BROKER_PORT=1883",
    "PYTHONUNBUFFERED=1",
  ]

  depends_on = [docker_container.broker_mqtt]
}


# --- edge_processor ---
resource "docker_image" "edge_processor" {
  name = "edge_processor:local"
  build {
    context    = "${path.module}/../../../edge/edge_processor"
    dockerfile = "Dockerfile"
  }
}

resource "docker_container" "edge_processor" {
  name  = "edge_processor"
  image = docker_image.edge_processor.image_id

  networks_advanced {
    name = docker_network.edge_network.name
  }

  env = [
    "BROKER_HOST=broker_mqtt",
    "BROKER_PORT=1883",
    "KAFKA_BOOTSTRAP=${var.paas_kafka_host}:9094",
    "PYTHONUNBUFFERED=1",
  ]

  depends_on = [docker_container.broker_mqtt]
}