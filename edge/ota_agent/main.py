import os
import json
import time
import subprocess
from confluent_kafka import Consumer, Producer
from business_rules import is_targeted, format_status

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_BOOTSTRAP_SERVERS_KEY = "bootstrap.servers"

SITE_ID = os.environ.get("SITE_ID", "site-001")
REPO_PATH = os.environ.get("REPO_PATH", f"/home/{os.environ.get('USER', 'alex')}/iot-edge-platform/infra/ansible")
LAST_GOOD_SHA_FILE = os.environ.get("LAST_GOOD_SHA_FILE", "/data/last_known_good_sha")
LOCK_FILE = os.environ.get("LOCK_FILE", "/data/edge-deploy.lock")

INPUT_TOPIC = "ota"
STATUS_TOPIC = "ota_status"
GROUP_ID = f"ota-agent-{SITE_ID}"  # unique per site - broadcast, not competing consumers

HEALTHCHECK_WAIT_SECONDS = 20
HEALTHCHECK_CONTAINERS = ["mosquitto", "iot_devices", "edge_processor"]


def read_last_known_good():
    if os.path.exists(LAST_GOOD_SHA_FILE):
        with open(LAST_GOOD_SHA_FILE) as f:
            return f.read().strip()
    return None


def write_last_known_good(sha):
    with open(LAST_GOOD_SHA_FILE, "w") as f:
        f.write(sha)


def run_deploy(sha):
    """ Invokes the same Ansible playbook used for manual SSH-driven deploys, but locally on the edge host itself. """
    result = subprocess.run(
        [
            "flock", "-n", LOCK_FILE,
            "ansible-playbook", "-i", "inventories/prod/inventory-local.ini",
            "playbooks/deploy-edge-apps.yml", "-e", f"deploy_branch={sha}",
        ],
        cwd=REPO_PATH,
        capture_output=True,
        text=True,
        timeout=600,
    )
    output = result.stdout + result.stderr
    if "no hosts matched" in output.lower() or "skipping: no hosts matched" in output.lower():
        return False, f"Ansible ran but matched no hosts - inventory issue:\n{output}"
    return result.returncode == 0, output


def check_healthy(docker_client):
    """
    A container counts as healthy if it's running - and, where a Docker
    HEALTHCHECK is defined, reports "healthy" specifically. Containers
    without a HEALTHCHECK (none currently besides mosquitto) only need
    to be running; healthcheck-bearing ones must actively report healthy,
    not just "starting".
    """
    for name in HEALTHCHECK_CONTAINERS:
        try:
            container = docker_client.containers.get(name)
        except Exception:
            return False, f"{name} not found"

        if container.status != "running":
            return False, f"{name} status is {container.status}, not running"

        health = container.attrs.get("State", {}).get("Health")
        if health is not None and health.get("Status") != "healthy":
            return False, f"{name} health status is {health.get('Status')}"

    return True, "all containers healthy"


def apply_update(sha, docker_client, status_producer):
    print(f"Applying update to sha={sha}...")
    deploy_ok, deploy_output = run_deploy(sha)

    if not deploy_ok:
        print(f"Deploy failed for sha={sha}:\n{deploy_output}")
        publish_status(status_producer, sha, "failed", "ansible-playbook run failed")
        return

    print(f"Waiting {HEALTHCHECK_WAIT_SECONDS}s before healthcheck...")
    time.sleep(HEALTHCHECK_WAIT_SECONDS)

    healthy, detail = check_healthy(docker_client)
    if healthy:
        write_last_known_good(sha)
        print(f"Update to sha={sha} succeeded and is healthy.")
        publish_status(status_producer, sha, "success", detail)
        return

    print(f"Update to sha={sha} unhealthy ({detail}), attempting rollback...")
    last_good = read_last_known_good()
    if last_good is None:
        print("No known-good sha on record - cannot roll back automatically.")
        publish_status(status_producer, sha, "failed", f"unhealthy and no rollback target: {detail}")
        return

    rollback_ok, rollback_output = run_deploy(last_good)
    if rollback_ok:
        print(f"Rolled back to sha={last_good}.")
        publish_status(status_producer, sha, "rolled_back", f"unhealthy ({detail}), reverted to {last_good}")
    else:
        print(f"Rollback to sha={last_good} also failed:\n{rollback_output}")
        publish_status(status_producer, sha, "failed", f"unhealthy and rollback failed: {detail}")


def publish_status(producer, sha, outcome, detail):
    status = format_status(SITE_ID, sha, outcome, detail)
    producer.produce(STATUS_TOPIC, value=json.dumps(status))
    producer.poll(0)


def run():
    import docker
    docker_client = docker.from_env()

    consumer = Consumer({
        KAFKA_BOOTSTRAP_SERVERS_KEY: KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest",  # only future OTA messages, never replay old ones on restart
        "enable.auto.commit": False,
    })
    consumer.subscribe([INPUT_TOPIC])
    status_producer = Producer({KAFKA_BOOTSTRAP_SERVERS_KEY: KAFKA_BOOTSTRAP})

    print(f"OTA agent ({SITE_ID}) listening on topic '{INPUT_TOPIC}'...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            update = json.loads(msg.value())
            sha = update["sha"]
            wave = update.get("wave", [])

            if not is_targeted(SITE_ID, wave):
                print(f"Update {sha} not targeted at {SITE_ID}, skipping.")
                consumer.commit(msg)
                continue

            apply_update(sha, docker_client, status_producer)
            consumer.commit(msg)
    except KeyboardInterrupt:
        print("Shutdown requested...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()