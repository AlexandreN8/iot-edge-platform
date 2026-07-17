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


# --- Fluent Bit ---
resource "docker_image" "fluent_bit" {
  name = "fluent/fluent-bit:4.2"
}

resource "docker_container" "fluent_bit" {
  name  = "fluent-bit"
  image = docker_image.fluent_bit.image_id

  networks_advanced {
    name = docker_network.edge_network.name
  }

  ports {
    internal = 24224
    external = 24224
  }

  upload {
    content = file("${path.module}/../../../edge/config/fluent-bit/fluent-bit.conf")
    file    = "/fluent-bit/etc/fluent-bit.conf"
  }

  upload {
    content = file("${path.module}/../../../edge/config/fluent-bit/parsers.conf")
    file    = "/fluent-bit/etc/parsers.conf"
  }

  lifecycle {
    replace_triggered_by = [
      terraform_data.fluent_bit_config_hash
    ]
  }

  restart = "unless-stopped"
}

resource "terraform_data" "fluent_bit_config_hash" {
  input = filesha256("${path.module}/../../../edge/config/fluent-bit/fluent-bit.conf")
}