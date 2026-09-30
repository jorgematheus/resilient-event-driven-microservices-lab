# Resilient Event-Driven Microservices Lab

Reference laboratory for the article:

**Designing Resilient Event-Driven Microservices on AWS: A Java-Based Architecture Using Amazon SQS, Amazon SNS, and Apache Kafka**

## Architecture

```text
                         +------------------+
                         | Order Service    |
                         | Java / Spring    |
                         +--------+---------+
                                  |
                 +----------------+----------------+
                 |                                 |
                 v                                 v
              Amazon SNS                       Kafka topic
                 |                                 |
          +------+-------+                         |
          |              |                         v
          v              v                  Analytics Service
       SQS queue      SQS queue
          |              |
          v              v
   Payment Service   Inventory Service
```

The lab demonstrates three different asynchronous communication patterns:

1. **SQS**: point-to-point work distribution.
2. **SNS → SQS**: event fan-out with independent consumer queues.
3. **Kafka**: event streaming, consumer groups and replay-oriented processing.

## Prerequisites

- Java 21+
- Maven 3.9+
- Docker / Docker Compose
- AWS CLI (only if you deploy to AWS)
- Terraform (optional, for AWS infrastructure)

The local environment uses Kafka and LocalStack for development. For production experiments, replace LocalStack endpoints with real AWS resources and use appropriate IAM roles rather than static credentials.

LocalStack clients use `test` credentials by default, so no AWS profile or credentials file is needed when running the services locally. Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to override these local credentials. For a non-local `AWS_ENDPOINT`, the AWS SDK default credential provider chain is used.

## Start infrastructure

```bash
docker compose up -d
```

Check:

```bash
docker compose ps
```

Kafka is exposed on `localhost:9092`.
LocalStack is exposed on `localhost:4566`.

## Run the services

Open separate terminals:

```bash
cd services/order-service
mvn spring-boot:run
```

```bash
cd services/payment-service
mvn spring-boot:run
```

```bash
cd services/inventory-service
mvn spring-boot:run
```

```bash
cd services/analytics-service
mvn spring-boot:run
```

## Create an order

```bash
curl -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"customer-001","amount":149.90}'
```

The Order Service creates an `OrderCreated` event.

The reference flow is:

```text
Order Service
      |
      +----> SNS ----> payment-queue ----> Payment Service
      |
      +----> SNS ----> inventory-queue --> Inventory Service
      |
      +----> Kafka --> Analytics Service
```

## Failure experiments

### Experiment 1 — Payment failure

Stop the payment service:

```bash
# stop the payment-service process
```

Create several orders.

Expected observation:

- orders are still accepted;
- payment messages accumulate in SQS;
- the producer does not need the payment service to be online;
- restarting the payment service allows queued work to be processed.

Measure:

- queue depth;
- processing latency;
- number of retries;
- recovery time.

### Experiment 2 — Consumer duplication

Introduce a deliberate processing delay in Payment Service longer than the SQS visibility timeout.

Observe that a message can become visible again and be delivered again. This demonstrates why application-level idempotency is required.

### Experiment 3 — Kafka consumer outage

Stop Analytics Service, publish orders, then restart Analytics Service.

Measure:

- consumer lag;
- recovery time;
- number of events processed after recovery.

### Experiment 4 — Compare SQS and Kafka

Generate a controlled workload and measure:

- throughput;
- p50/p95/p99 latency;
- consumer recovery;
- backlog;
- CPU and memory.

Do not publish synthetic results in the article. Record the actual results produced by your environment.

## Suggested repository evidence

Keep:

```text
docs/
  architecture.md
  experiment-plan.md
  results/
    baseline.csv
    failure-sqs.csv
    failure-kafka.csv
    recovery.csv

diagrams/
  architecture.png

scripts/
  load-test.sh
```

This makes the GitHub repository a reproducible companion to the technical article.
