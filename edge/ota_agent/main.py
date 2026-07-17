import os
import json
import time
import subprocess
from confluent_kafka import Consumer, Producer
from business_rules import is_targeted, format_status
from logging_setup import get_logger

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_BOOTSTRAP_SERVERS_KEY = "bootstrap.servers"

SITE_ID = os.environ.get("SITE_ID", "site-001")
REPO_PATH = os.environ.get("REPO_PATH", f"/home/{os.environ.get('USER', 'alex')}/iot-edge-platform/infra/ansible")
LAST_GOOD_SHA_FILE = os.environ.get("LAST_GOOD_SHA_FILE", "/data/last_known_good_sha")
LOCK_FILE = os.environ.get("LOCK_FILE", "/data/edge-deploy.lock")
ENABLE_MAIN_FALLBACK = os.environ.get("ENABLE_MAIN_FALLBACK", "false").lower() == "true"

INPUT_TOPIC = "ota"
STATUS_TOPIC = "ota_status"
GROUP_ID = f"ota-agent-{SITE_ID}"

HEALTHCHECK_WAIT_SECONDS = 20
HEALTHCHECK_CONTAINERS = ["mosquitto", "iot_devices", "edge_processor"]

UNRESOLVABLE_SHA_MARKERS = ["unable to read tree", "failed to checkout"]

logger = get_logger("ota_agent")


def read_last_known_good():
    if os.path.exists(LAST_GOOD_SHA_FILE):
        with open(LAST_GOOD_SHA_FILE) as f:
            return f.read().strip()
    return None


def write_last_known_good(sha):
    with open(LAST_GOOD_SHA_FILE, "w") as f:
        f.write(sha)


def clear_last_known_good():
    if os.path.exists(LAST_GOOD_SHA_FILE):
        os.remove(LAST_GOOD_SHA_FILE)


def is_unresolvable_sha_error(output):
    lowered = output.lower()
    return any(marker in lowered for marker in UNRESOLVABLE_SHA_MARKERS)


def run_deploy(sha):
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
    logger.info(
        "ansible-playbook run completed",
        extra={"category": "infra", "fields": {"sha": sha, "returncode": result.returncode, "output": output}},
    )

    if "no hosts matched" in output.lower():
        return False, f"Ansible ran but matched no hosts - inventory issue:\n{output}"
    if is_unresolvable_sha_error(output):
        return False, f"sha {sha} is not resolvable (deleted/rewritten in git history):\n{output}"

    return result.returncode == 0, output


def check_healthy(docker_client):
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


def attempt_rollback(sha, target, docker_client, status_producer, label):
    logger.info(
        "Attempting rollback",
        extra={"category": "business", "fields": {"target_label": label, "target_sha": target, "original_sha": sha}},
    )
    rollback_ok, rollback_output = run_deploy(target)

    if not rollback_ok:
        logger.warning(
            "Rollback deploy failed",
            extra={"category": "business", "fields": {"target_label": label, "target_sha": target}},
        )
        if is_unresolvable_sha_error(rollback_output) and label == "last-known-good":
            logger.warning(
                "Recorded last-known-good sha is itself unresolvable - clearing it",
                extra={"category": "business", "fields": {"target_sha": target}},
            )
            clear_last_known_good()
        return False, rollback_output

    logger.info(
        "Waiting before confirming rollback health",
        extra={"category": "infra", "fields": {"wait_seconds": HEALTHCHECK_WAIT_SECONDS}},
    )
    time.sleep(HEALTHCHECK_WAIT_SECONDS)
    healthy, detail = check_healthy(docker_client)

    if not healthy:
        logger.warning(
            "Rollback deployed but unhealthy",
            extra={"category": "business", "fields": {"target_label": label, "target_sha": target, "detail": detail}},
        )
        return False, detail

    write_last_known_good(target)
    logger.info(
        "Rollback succeeded, confirmed healthy",
        extra={"category": "business", "fields": {"target_label": label, "target_sha": target}},
    )
    publish_status(status_producer, sha, "rolled_back", f"reverted to {label} sha={target}")
    return True, detail


def apply_update(sha, docker_client, status_producer):
    logger.info("Applying update", extra={"category": "business", "fields": {"sha": sha}})
    deploy_ok, deploy_output = run_deploy(sha)

    if not deploy_ok:
        logger.warning("Deploy failed", extra={"category": "business", "fields": {"sha": sha}})
        publish_status(status_producer, sha, "failed", "ansible-playbook run failed")
        return

    logger.info(
        "Waiting before healthcheck",
        extra={"category": "infra", "fields": {"wait_seconds": HEALTHCHECK_WAIT_SECONDS}},
    )
    time.sleep(HEALTHCHECK_WAIT_SECONDS)

    healthy, detail = check_healthy(docker_client)
    if healthy:
        write_last_known_good(sha)
        logger.info("Update succeeded and is healthy", extra={"category": "business", "fields": {"sha": sha}})
        publish_status(status_producer, sha, "success", detail)
        return

    logger.warning(
        "Update unhealthy, attempting rollback",
        extra={"category": "business", "fields": {"sha": sha, "detail": detail}},
    )

    last_good = read_last_known_good()
    if last_good is not None:
        resolved, _ = attempt_rollback(sha, last_good, docker_client, status_producer, "last-known-good")
        if resolved:
            return

    if ENABLE_MAIN_FALLBACK:
        logger.info("Falling back to main as last resort", extra={"category": "business", "fields": {"sha": sha}})
        resolved, fallback_detail = attempt_rollback(sha, "main", docker_client, status_producer, "main (fallback)")
        if resolved:
            return
        publish_status(status_producer, sha, "failed", f"unhealthy, last-known-good and main fallback both failed: {fallback_detail}")
        return

    publish_status(status_producer, sha, "failed", f"unhealthy and no rollback target available: {detail}")


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
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([INPUT_TOPIC])
    status_producer = Producer({KAFKA_BOOTSTRAP_SERVERS_KEY: KAFKA_BOOTSTRAP})

    logger.info(
        "OTA agent listening on topic",
        extra={"category": "infra", "fields": {"topic": INPUT_TOPIC, "main_fallback_enabled": ENABLE_MAIN_FALLBACK}},
    )
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error", extra={"category": "infra", "fields": {"error": str(msg.error())}})
                continue

            update = json.loads(msg.value())
            sha = update["sha"]
            wave = update.get("wave", [])

            if not is_targeted(SITE_ID, wave):
                logger.info(
                    "Update not targeted at this site, skipping",
                    extra={"category": "business", "fields": {"sha": sha, "site_id": SITE_ID}},
                )
                consumer.commit(msg)
                continue

            apply_update(sha, docker_client, status_producer)
            consumer.commit(msg)
    except KeyboardInterrupt:
        logger.info("Shutdown requested", extra={"category": "infra"})
    finally:
        consumer.close()


if __name__ == "__main__":
    run()