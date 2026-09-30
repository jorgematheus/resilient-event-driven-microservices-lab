# Experimental Plan

## Objective

Evaluate how SQS, SNS/SQS fan-out and Kafka behave under controlled failure and recovery scenarios.

## Baseline

Run all services without injected failures.

Record:

- total orders submitted;
- successful processing;
- p50/p95/p99 latency;
- throughput;
- CPU;
- memory;
- queue depth;
- Kafka consumer lag.

## Experiment A — SQS consumer outage

1. Start all services.
2. Generate N orders.
3. Stop Payment Service.
4. Continue generating orders.
5. Record payment queue depth.
6. Restart Payment Service.
7. Record recovery time and backlog drain rate.

## Experiment B — Duplicate processing

1. Reduce the visibility timeout.
2. Add an artificial processing delay.
3. Observe redelivery.
4. Add an idempotency store.
5. Repeat the test.
6. Compare duplicate side effects.

## Experiment C — Kafka consumer outage

1. Generate N events.
2. Stop Analytics Service.
3. Continue generating events.
4. Record consumer lag.
5. Restart Analytics Service.
6. Measure recovery.

## Experiment D — Throughput

Run controlled loads such as:

- 10 events/s
- 100 events/s
- 500 events/s
- 1,000 events/s

Only publish values actually observed in your environment.

## Experimental integrity

Record:

- hardware/VM/container configuration;
- Java version;
- service versions;
- Kafka configuration;
- AWS/LocalStack configuration;
- workload generator;
- timestamps;
- raw result files.

This makes the experiment reproducible.
