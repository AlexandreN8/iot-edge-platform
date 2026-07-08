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

resource "docker_image" "nginx" {
  name = "nginx:1.31.2"
}

resource "docker_container" "edge_test" {
  name  = "edge-lab-test"
  image = docker_image.nginx.image_id
  ports {
    internal = 80
    external = 8080
  }
}