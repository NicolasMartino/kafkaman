# Source Note: Debezium Outbox Event Router

Source URL: https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html
Retrieved: 2026-06-20
Mode: Web

## What This Source Is

Debezium documents an outbox event router single message transform for change data capture based outbox publishing.

## Relevant Findings

- Debezium describes the outbox pattern as a way to reliably exchange data between microservices while avoiding inconsistency between service database state and events consumed by other services.
- The Debezium implementation captures changes in an outbox table.
- The event router can map outbox records into Kafka topics and event payloads.
- This approach moves the relay responsibility out of the application process and into CDC infrastructure.

## Implication For kafkaman

kafkaman should decide whether its initial design is:

- Application-level polling/claiming from Postgres, owned by the Rust service or worker.
- CDC-based publishing, where kafkaman mainly provides schemas and helper writes while Debezium publishes.
- A pluggable design that starts with application polling and leaves CDC as a later backend.

Given the current project description mentions a scheduler that attempts to run application code and handle retries, application-level polling is the closer initial match. Debezium remains important as an alternative integration path and future compatibility target.

## Caveats

CDC introduces its own operational dependencies and schema conventions. It may be overkill for a library intended to drop into existing Rust services unless those services already operate Debezium.
