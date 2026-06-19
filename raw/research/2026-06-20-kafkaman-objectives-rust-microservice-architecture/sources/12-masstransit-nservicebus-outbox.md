# Source Note: MassTransit and NServiceBus Outbox Designs

MassTransit URL: https://masstransit.io/documentation/patterns/transactional-outbox
NServiceBus URL: https://docs.particular.net/nservicebus/outbox/
Retrieved: 2026-06-20
Mode: Web

## What These Sources Are

MassTransit and NServiceBus are mature .NET messaging frameworks with outbox support. They are useful comparables because they show how production frameworks package inbox/outbox behavior for application developers.

## Relevant Findings

- MassTransit describes a consumer outbox as a combination of inbox and outbox: the inbox tracks received messages for exactly-once consumer behavior, while the outbox stores outgoing messages until the consumer completes successfully.
- MassTransit describes locking received messages by message ID when the outbox is configured.
- NServiceBus describes its outbox as a consistency mechanism between business data and messages where queues and stores do not support distributed transactions.
- NServiceBus treats messages sent in certain immediate-dispatch/session contexts as outside the outbox, which is a useful reminder that APIs need clear boundaries.

## Implication For kafkaman

kafkaman should expose explicit boundaries:

- Which sends are part of a durable outbox transaction.
- Which receives are protected by inbox dedupe.
- Which ad hoc sends bypass the durability model.
- How a handler transaction publishes follow-up messages atomically with handler state changes.

It should also distinguish "broker exactly once" from "application state exactly once" in docs and API names.

## Caveats

These frameworks are not Rust and carry ecosystem-specific assumptions. They are design comparables, not implementation dependencies.
