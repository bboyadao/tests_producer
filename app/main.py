import json
import os
import sys
import time
import random
from datetime import datetime, timezone
from urllib.parse import urljoin
from confluent_kafka import Producer

def _create_producer(bootstrap_servers: str) -> "Producer":
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "linger.ms": 10,
        "enable.idempotence": True,
        "acks": "all",
    }
    return Producer(conf)


def _send_random_batch(
    producer: "Producer",
    topics_by_domain: dict[str, str],
    count: int,
) -> None:
    errors: list[str] = []
    base_url = os.getenv("CRAWL_BASE_URL")
    endpoints = {
        "social": "/social/posts",
        "bank": "/bank/transactions",
        "ecommerce": "/ecommerce/history",
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
        print(logical, "==========>", json.dumps(payload, separators=(",", ":")))
        producer.produce(
            topic=topic_for_domain,
            key=key_bytes,
            value=val_bytes,
            on_delivery=_delivery,
        )
        producer.poll(0)

    producer.flush()
    if errors:
        raise RuntimeError("Failed to deliver one or more messages: " + "; ".join(errors))


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    topic_bank = os.getenv("CRAWL_TOPIC_BANK", "crawl_bank")
    topic_social = os.getenv("CRAWL_TOPIC_SOCIAL", "crawl_social")
    topic_ecom = os.getenv("CRAWL_TOPIC_ECOM", "crawl_ecom")
    topics_by_domain = {
        "bank": topic_bank,
        "social": topic_social,
        "ecom": topic_ecom,
    }

    try:
        producer = _create_producer(bootstrap)
        while True:
            _send_random_batch(
                producer,
                topics_by_domain=topics_by_domain,
                count=100,
            )
            print("Batch sent: 100 messages across domain topics")
            time.sleep(2)
    except KeyboardInterrupt:
        try:
            producer.flush()
        except Exception:
            pass
        print("Interrupted by user. Exiting.")
    except Exception as exc:
        print(f"Failed to send crawl tasks: {exc}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
