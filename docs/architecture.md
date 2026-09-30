# Architecture Notes

## SQS

Use SQS for point-to-point asynchronous work where a consumer owns the processing responsibility.

Important properties to investigate:

- visibility timeout;
- retries;
- dead-letter queues;
- long polling;
- idempotent processing.

## SNS + SQS

Use SNS when one event must be delivered to multiple independently managed consumers.

Each consumer receives its own SQS queue, providing independent buffering and failure isolation.

## Kafka

Use Kafka for event-streaming workloads where partitioning, consumer groups, durable offsets and replay-oriented processing are useful.

## Architectural rule

Do not select a broker based only on throughput. Select the communication mechanism based on interaction semantics, delivery requirements, ordering, replay needs, failure isolation and operational constraints.
