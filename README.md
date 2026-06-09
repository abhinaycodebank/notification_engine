# Event Ingestion & Webhook Notification Fanout Engine

A high-performance, event-driven notification hub built with Go, Apache Kafka, PostgreSQL, and Redis. This system reliably ingests high-throughput event logs, evaluates subscriber routing rules, and delivers robust, asynchronous **At-Least-Once** webhook notifications with automatic retry capabilities.

---

## 🏗️ Architecture Overview

The platform uses a decoupled, event-driven microservices layout to maximize delivery performance and guarantee that network spikes in public traffic cannot block down-stream consumer dispatchers.

```text
  [ Client Application ]
            │
            ▼ (HTTP POST /v1/events)
   ┌─────────────────┐      Publish Byte Payload       ┌──────────────────────┐
   │   Ingest API    │ ──────────────────────────────> │     Apache Kafka     │
   │   (Port 8082)   │                                 │ Topic: 'raw-events'  │
   └─────────────────┘                                 └──────────────────────┘
                                                                  │
                                                                  │ Continuous Consumption
                                                                  ▼
   ┌─────────────────┐       Query / Evict Cache       ┌──────────────────────┐
   │  Subscription   │ <────────────────────────────── │   Matching Engine    │
   │   Admin API     │                                 │   (Worker Thread)    │
   │   (Port 8081)   │ ───┐                            └──────────────────────┘
   └─────────────────┘    │ Write Tables                          │
            │             ▼                                       │ Filter Rule Match?
            │     ┌──────────────┐                                ▼
            └────>│  PostgreSQL  │                     ┌──────────────────────┐
    Evict Cache   │  (Storage &  │                     │     Apache Kafka     │
   (Redis Del)    │  Audit Logs) │                     │Topic: 'delivery-task'│
            │     └──────────────┘                     └──────────────────────┘
            ▼             ▲                                       │
   ┌─────────────────┐    │ Log History Execution                 │ Consume Task
   │   Redis Cache   │ ───┘                                       ▼
   │  (Subscription) │                                 ┌──────────────────────┐
   │  (Subscription) │                                 │   Delivery Worker    │
   │  (Subscription) │                                 │  (Concurrent Pool)   │
   └─────────────────┘                                 └──────────────────────┘
                                                                  │
                                                                  ▼ (HTTP Webhook Dispatch)
                                                       [ Third-Party Subscribers ]
```

---

## 🧰 Tech Stack & Framework Selection

*   **Core Language:** **Go (Golang)** – Exceptional memory footprint efficiency, low execution latency, and native concurrency primitives (goroutines and channels).
*   **Message Broker Queue Layer:** **Apache Kafka** – Handled via Segment's pure-Go `kafka-go` driver. Partitioned deterministically using `event_type` as the partition routing key to protect structural event sequencing.
*   **Primary System Database:** **PostgreSQL** – Powered via the robust connection pooling package `pgx/v5`. Tracks admin subscriptions and immutable time-stamped execution logs.
*   **Caching & Memory Layer:** **Redis** – Managed with `go-redis/v9`. Holds active lookup criteria maps to bypass hitting PostgreSQL for every single event stream element. It features explicit invalidation hooks triggered on configuration changes.

---

## 🔄 End-to-End Request Flows

### 1. Ingestion Path
1. An upstream business client sends a JSON event signature payload to the `/v1/events` endpoint.
2. The `Ingest API` performs basic structural sanity validation.
3. The system captures the payload, resolves its configuration partition metadata tracking, and drops it into Kafka's `raw-events` topic pipeline.
4. The client receives an immediate `202 Accepted` status indicator back within milliseconds.

### 2. Processing & Evaluation Loop
1. The `Matching Engine` background worker continuously polls a replica slice of the active Kafka broker partitions.
2. For each incoming event string, the node checks the local `Redis Memory Store` first. If a cache miss occurs, it loads the subscription rows from `PostgreSQL` and repopulates Redis.
3. The processing context applies a clean key-value exact-intersection matching verification loop. If **all keys** in a subscriber's filter definition align with the properties inside the payload's `data` block, the match is confirmed.
4. Confirmed matches are converted into `DeliveryTask` profiles and written into the `delivery-task` Kafka topic.

### 3. Delivery & Resilience Lifecycle
1. Concurrent execution blocks inside the `Delivery Worker` pull pending notifications from the queue layer.
2. The code schedules non-blocking goroutine HTTP POST operations out across a controlled resource pool.
3. **If a call returns a 2xx Success:** The transaction details write to the `delivery_audits` history ledger in Postgres, and the worker commits the offset.
4. **If a call fails (5xx Server Drop, Timeouts, or Base Network Layer breaks):**
    * If `Current Attempt < Max Attempts (3)`, the engine increases the step counter, schedules an exponential back-off calculation, and routes the message back into the Kafka pipeline queue.
    * If retries are exhausted, the message status is locked inside the relational engine as `failed` for administrative audit trace discovery.

---

## 📋 Table Definitions (PostgreSQL Schema)

```sql
CREATE TABLE subscriptions (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    webhook_url TEXT NOT NULL,
    filters JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE delivery_audits (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id VARCHAR(255) REFERENCES subscriptions(id) ON DELETE CASCADE,
    event_id VARCHAR(255) NOT NULL,
    attempt INT NOT NULL DEFAULT 1,
    http_status INT,
    response_body TEXT,
    delivery_status VARCHAR(50) NOT NULL, -- 'delivered', 'failed', 'pending_retry'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

---

## 🧪 Production Test Runner Scripts

You can use the template below to set up a clean shell script to verify your end-to-end event flow.

Create a file named `run_test.sh` in your workspace directory:

```bash
#!/usr/bin/env bash
set -euo pipefail

# Text color definitions for output logging formatting
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "\({BLUE}[TEST STAGE 1]: Registering a filter target with the Admin Engine...\){NC}"
curl -s -X POST http://localhost:8081/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "id": "sub-testing-999",
    "name": "Live Discord Alerts Webhook Gateway",
    "webhook_url": "https://httpbin.org",
    "filters": {
      "event_type": "transaction.success",
      "region": "APAC"
    }
  }'

echo -e "\n\n\({BLUE}[TEST STAGE 2]: Emitting a NON-MATCHING event profile (Should be ignored)...\){NC}"
curl -s -X POST http://localhost:8082/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "id": "evt-ignore-001",
    "event_type": "transaction.success",
    "source": "checkout-portal",
    "data": { "region": "EMEA" }
  }'

echo -e "\n\n\({GREEN}[TEST STAGE 3]: Emitting a MATCHING event footprint...\){NC}"
curl -s -X POST http://localhost:8082/v1/events \
  -H "Content-Type: application/json" \
  -d '{
    "id": "evt-match-100k",
    "event_type": "transaction.success",
    "source": "checkout-portal",
    "data": { "region": "APAC", "amount": "1250" }
  }'

echo -e "\n\n\({GREEN}[TEST COMPLETE]: Monitor your service terminal execution instances to trace ingestion, matching loops, and audit logs.\){NC}"
```

Make the script executable on your machine:
```bash
chmod +x run_test.sh
```
