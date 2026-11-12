"""
Real-Time Credit Risk Scoring Consumer
Connects Member 1's Kafka loan application topic to the Track A ML scoring service.
Supports both live Kafka streaming and a standalone offline playback simulation.
"""
import argparse
import json
import sys
import time
import warnings
from pathlib import Path
from typing import Dict, Any, Optional

warnings.filterwarnings("ignore")

# Support both module execution and direct script execution
try:
    from src.config import BASE_DIR, TEST_DATA_PATH
    from src.service.scoring_service import CreditRiskScoringService
except (ImportError, ValueError):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from src.config import BASE_DIR, TEST_DATA_PATH
    from src.service.scoring_service import CreditRiskScoringService


class RealTimeCreditScorer:
    """
    Subscribes to incoming loan application events (topic: hc.los.loan-application.v1),
    computes default probability via CreditRiskScoringService, and issues credit decisions.
    """

    def __init__(self, service: Optional[CreditRiskScoringService] = None):
        self.service = service or CreditRiskScoringService()

    def process_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses an incoming envelope event and returns the scored decision.
        """
        customer_id = event.get("customer_id")
        payload = event.get("payload", event)

        if "SK_ID_CURR" not in payload and customer_id is not None:
            payload["SK_ID_CURR"] = customer_id

        decision = self.service.score_applicant(payload, include_explanations=True)
        return decision

    def run_live_kafka(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "hc.los.loan-application.v1",
        group_id: str = "credit-risk-scoring-engine",
        max_messages: Optional[int] = None,
    ):
        """
        Connects to live Kafka broker and scores incoming loan applications.
        """
        try:
            from confluent_kafka import Consumer, KafkaError
        except ImportError:
            print("confluent_kafka not installed. Run 'pip install confluent-kafka' or use --dry-run.")
            sys.exit(1)

        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )
        consumer.subscribe([topic])
        print(f"Subscribed to Kafka topic: {topic} on {bootstrap_servers}")

        count = 0
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print(f"Consumer error: {msg.error()}")
                    break

                event = json.loads(msg.value().decode("utf-8"))
                decision = self.process_event(event)
                count += 1
                print(
                    f"[{count:04d}] Customer: {decision['SK_ID_CURR']} | "
                    f"PD: {decision['default_probability']:.4f} | "
                    f"Tier: {decision['risk_tier']:6s} | Action: {decision['action']}"
                )

                if max_messages and count >= max_messages:
                    break
        finally:
            consumer.close()

    def run_dry_run_simulation(self, sample_limit: int = 10):
        """
        Simulates real-time streaming using rows from application_test.csv without requiring Kafka.
        """
        import pandas as pd

        print("=" * 70)
        print(" SIMULATED REAL-TIME CREDIT SCORING STREAM (Offline Mode)")
        print("=" * 70)
        print(f"Streaming {sample_limit} applications from {TEST_DATA_PATH.name}...\n")

        df = pd.read_csv(TEST_DATA_PATH, nrows=sample_limit)
        decisions = []

        for idx, row in df.iterrows():
            payload = row.to_dict()
            event = {
                "event_id": f"sim_{idx:08d}",
                "event_type": "loan_application.submitted",
                "customer_id": int(row["SK_ID_CURR"]),
                "payload": payload,
            }

            t0 = time.perf_counter()
            decision = self.process_event(event)
            latency_ms = (time.perf_counter() - t0) * 1000

            top_drivers = ", ".join(
                [d["feature"] for d in decision.get("top_risk_drivers", [])]
            )

            print(
                f"[App #{idx+1:02d}] Customer: {decision['SK_ID_CURR']} | "
                f"PD: {decision['default_probability']:.4f} | "
                f"Tier: {decision['risk_tier']:6s} | "
                f"Action: {decision['action']:14s} | "
                f"Latency: {latency_ms:.1f}ms | Top Drivers: {top_drivers}"
            )
            decisions.append(decision)

        print("\n" + "=" * 70)
        print(f"Simulated streaming complete. Average latency: {latency_ms:.2f}ms/record.")
        print("=" * 70)
        return decisions


def main():
    parser = argparse.ArgumentParser(description="Real-Time Credit Risk Scoring Consumer")
    parser.add_argument("--dry-run", action="store_true", help="Run simulated stream without Kafka broker")
    parser.add_argument("--limit", type=int, default=10, help="Number of records to score in dry-run mode")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="hc.los.loan-application.v1", help="Kafka topic")
    args = parser.parse_args()

    scorer = RealTimeCreditScorer()
    if args.dry_run:
        scorer.run_dry_run_simulation(sample_limit=args.limit)
    else:
        scorer.run_live_kafka(bootstrap_servers=args.bootstrap, topic=args.topic, max_messages=args.limit)


if __name__ == "__main__":
    main()
