#!/bin/bash
# Create the ingestion layer's Kafka topics (safe to re-run thanks to --if-not-exists).
# The list must match SOURCES in ingestion/common.py.
set -euo pipefail
BOOTSTRAP=${BOOTSTRAP:-localhost:9092}
KT=/opt/kafka/bin/kafka-topics.sh
WEEK_MS=604800000
MONTH_MS=2592000000

create() {  # create <topic> <partitions> <retention.ms>
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists --topic "$1" \
    --partitions "$2" --replication-factor 1 \
    --config retention.ms="$3" --config cleanup.policy=delete --config compression.type=producer
}

create hc.los.loan-application.v1          3 "$WEEK_MS"
create hc.bureau.credit-record.v1          3 "$WEEK_MS"
create hc.bureau.credit-balance-monthly.v1 3 "$WEEK_MS"
create hc.history.previous-application.v1  3 "$WEEK_MS"
create hc.pos.cash-balance-monthly.v1      3 "$WEEK_MS"
create hc.card.balance-monthly.v1          3 "$WEEK_MS"
create hc.core.installment-payment.v1      3 "$WEEK_MS"
create hc.dlq.v1                           1 "$MONTH_MS"

echo "Existing topics:"
"$KT" --bootstrap-server "$BOOTSTRAP" --list
