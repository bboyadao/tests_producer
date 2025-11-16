import json
import os
import time
import random
from datetime import datetime, timezone
from urllib.parse import urljoin
from confluent_kafka import Producer
import logging, sys

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def _create_producer(bootstrap_servers: str) -> "Producer":
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "linger.ms": 10,
        "enable.idempotence": True,
        "acks": "all",
    }
    debug_flags = os.getenv("KAFKA_DEBUG")
    if debug_flags:
        conf["debug"] = debug_flags
    return Producer(conf)


def _send_random_batch(
    producer: "Producer",
    topics_by_domain: dict[str, str],
    count: int,
) -> None:
    errors: list[str] = []
    base_url = os.getenv("CRAWL_BASE_URL", "http://localhost:8000")
    endpoints = {
        "social": "/crawl/social/posts?count=10",
        "bank": "/crawl/bank/transactions?count=10",
        "ecommerce": "/crawl/ecommerce/history?count=10",
    }

    domains_env = os.getenv("CRAWL_DOMAINS", "bank,social,ecommerce")
    configured_domains = [d.strip() for d in domains_env.split(",") if d.strip()]
    allowed = [d for d in configured_domains if d in endpoints]
    logical_domains = allowed or list(endpoints.keys())

    def _delivery(err, _msg) -> None:
        if err is not None:
            errors.append(str(err))
    for _ in range(count):
        logical = random.choice(logical_domains)
        topic_for_domain = (
            topics_by_domain.get(logical)
            or (topics_by_domain.get("ecom") if logical == "ecommerce" else None)
        )

        full_url = urljoin(base_url.rstrip("/") + "/", endpoints[logical].lstrip("/"))

        payload = {
            "type": "crawl_request",
            "domain": logical,
            "path": full_url,
            "requested_at": datetime.now(timezone.utc).isoformat(),
        }

        key_bytes = logical.encode()
        val_bytes = json.dumps(payload, separators=(",", ":")).encode()
        producer.produce(
            topic=topic_for_domain,
            key=key_bytes,
            value=val_bytes,
            headers=[("group_id", (topic_for_domain or "").encode())],
            on_delivery=_delivery,
        )
        producer.poll(0)

    producer.flush()
    if errors:
        raise RuntimeError("Failed to deliver one or more messages: " + "; ".join(errors))


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    group_topic = os.getenv("CRAWL_TOPIC_GROUP", "crawl_group")
    topics_by_domain = {
        "bank": group_topic,
        "social": group_topic,
        "ecom": group_topic,
    }
    try:
        producer = _create_producer(bootstrap)
    except Exception as exc:
        raise RuntimeError(f"Cant create producre {exc.__repr__()}")

    try:

        batch_size = 20
        logger.info(
            f"Producer starting. bootstrap={bootstrap} group_topic={group_topic} batch_size={batch_size}"
        )
        while True:
            _send_random_batch(
                producer,
                topics_by_domain=topics_by_domain,
                count=batch_size,
            )
            logger.info(f"Batch sent: {batch_size} messages to group topic: {group_topic}")
            time.sleep(60)

    except KeyboardInterrupt:
        try:
            producer.flush()
        except Exception:
            pass
        logger.info("Interrupted by user. Exiting.")
    except Exception as exc:
        logger.info(f"Failed to send crawl tasks: {exc.__repr__()}")
        sys.exit(1)

if __name__ == "__main__":
    main()
