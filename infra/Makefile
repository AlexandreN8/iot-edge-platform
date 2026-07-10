# Orchestrates Terraform (infra topology: network, volumes, third-party
# images) and Ansible (application containers: iot_devices, edge_processor,
# cleaner) in the correct order for each tier.
#
# Order matters and is NOT symmetrical:
#   up   -> Terraform first (topology must exist before containers attach to it)
#   down -> Ansible first (containers must be removed before Terraform can
#           destroy the network they're still attached to - Terraform's
#           state no longer tracks them once Ansible owns their lifecycle)
#
# Usage:
#   make -C infra edge-up      (from repo root)
#   make edge-up               (from inside infra/)

.PHONY: edge-up edge-down paas-up paas-down

# --- Edge tier (remote laptop, over SSH) ---
edge-up:
	cd terraform/edge && terraform apply -auto-approve
	cd ansible && ansible-playbook playbooks/deploy-edge.yml -K

edge-down:
	cd ansible && ansible-playbook playbooks/deploy-edge.yml -K -e container_state=absent
	cd terraform/edge && terraform destroy

# --- PaaS tier (local desktop daemon, no SSH needed) ---
paas-up:
	cd terraform/paas && terraform apply -auto-approve
	cd ansible && ansible-playbook playbooks/deploy-paas.yml -K

paas-down:
	cd ansible && ansible-playbook playbooks/deploy-paas.yml -K -e container_state=absent
	cd terraform/paas && terraform destroy