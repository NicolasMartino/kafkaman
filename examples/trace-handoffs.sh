#!/usr/bin/env bash
#
# Join linked-mode async Kafka trace handoffs that Kibana APM presents as span
# links.
#
# The consumer-side `kafkaman.ingest` span links back to the producer-side
# `kafkaman.relay.publish` span when `kafka_trace_handoff = "linked"`. The
# example services default to `parented`, where APM already shows one waterfall;
# this helper remains useful when you switch back to linked mode to inspect
# OpenTelemetry's default messaging shape. Kibana can display the link count,
# but it does not reliably provide a forward navigation target from the producer
# waterfall, so this helper prints both direct APM waterfall URLs.
#
#     examples/trace-handoffs.sh
#     TRACE_RANGE=now-24h LIMIT=3 examples/trace-handoffs.sh
#     MESSAGE_TYPE=product_snapshot CONSUMER_SERVICE=kafkaman-example-order \
#       PRODUCER_TRANSACTION='POST /products' examples/trace-handoffs.sh
#     OUTPUT=json examples/trace-handoffs.sh
#
# LIMIT bounds *candidate* handoffs, not printed rows. MESSAGE_TYPE and
# CONSUMER_SERVICE are pushed into the Elasticsearch query, but PRODUCER_SERVICE
# and PRODUCER_TRANSACTION can only be known after the producer trace has been
# fetched, so they filter afterwards. Raise LIMIT when a producer-side filter
# returns fewer rows than you expected.
#
# Each candidate costs two more Elasticsearch searches — one per side of the
# handoff — so LIMIT=100 is a few hundred requests against a local stack. That is
# fine for a debugging helper and is why this is not on the demo path.
#

set -euo pipefail

ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://127.0.0.1:${ELASTICSEARCH_PORT:-9200}}"
KIBANA_URL="${KIBANA_URL:-http://127.0.0.1:${KIBANA_PORT:-5601}}"
TRACE_RANGE="${TRACE_RANGE:-now-4h}"
LIMIT="${LIMIT:-10}"
OUTPUT="${OUTPUT:-text}"
MESSAGE_TYPE="${MESSAGE_TYPE:-}"
PRODUCER_SERVICE="${PRODUCER_SERVICE:-}"
PRODUCER_TRANSACTION="${PRODUCER_TRANSACTION:-}"
CONSUMER_SERVICE="${CONSUMER_SERVICE:-}"

readonly ELASTICSEARCH_URL KIBANA_URL TRACE_RANGE LIMIT OUTPUT
readonly MESSAGE_TYPE PRODUCER_SERVICE PRODUCER_TRANSACTION CONSUMER_SERVICE

command -v curl >/dev/null 2>&1 || { echo "curl is required but not installed" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq is required but not installed" >&2; exit 1; }

case "$LIMIT" in
  ''|*[!0-9]*)
    echo "LIMIT must be a positive integer" >&2
    exit 1
    ;;
esac

if [[ "$LIMIT" -eq 0 ]]; then
    echo "LIMIT must be greater than zero" >&2
    exit 1
fi

api_es() {
    local path="$1"
    local payload="$2"

    curl -fsS --max-time 10 -X POST "$ELASTICSEARCH_URL/$path" \
        -H 'content-type: application/json' \
        -d "$payload"
}

urlencode() {
    jq -rn --arg value "$1" '$value | @uri'
}

apm_url() {
    local service="$1"
    local transaction_name="$2"
    local transaction_type="$3"
    local trace_id="$4"
    local transaction_id="$5"

    printf '%s/app/apm/services/%s/transactions/view?comparisonEnabled=false&environment=ENVIRONMENT_ALL&kuery=&latencyAggregationType=avg&rangeFrom=%s&rangeTo=now&serviceGroup=&showCriticalPath=&transactionName=%s&transactionType=%s&traceId=%s&transactionId=%s' \
        "$KIBANA_URL" \
        "$(urlencode "$service")" \
        "$(urlencode "$TRACE_RANGE")" \
        "$(urlencode "$transaction_name")" \
        "$(urlencode "$transaction_type")" \
        "$(urlencode "$trace_id")" \
        "$(urlencode "$transaction_id")"
}

ingest_payload=$(jq -n \
    --arg range "$TRACE_RANGE" \
    --argjson size "$LIMIT" \
    --arg message_type "$MESSAGE_TYPE" \
    --arg consumer_service "$CONSUMER_SERVICE" \
    '{
      size: $size,
      sort: [{"@timestamp": "desc"}],
      _source: [
        "@timestamp",
        "name",
        "trace_id",
        "span_id",
        "links",
        "attributes.message_type",
        "attributes.messaging.destination.name",
        "attributes.transaction.type",
        "resource.attributes.service.name"
      ],
      query: {
        bool: {
          filter: ([
            {term: {"data_stream.type": "traces"}},
            {term: {name: "kafkaman.ingest"}},
            {exists: {field: "links.trace_id"}},
            {exists: {field: "links.span_id"}},
            {range: {"@timestamp": {gte: $range}}}
          ]
          + (if $message_type == "" then [] else [
              {term: {"attributes.message_type": $message_type}}
            ] end)
          + (if $consumer_service == "" then [] else [
              {term: {"resource.attributes.service.name": $consumer_service}}
            ] end))
        }
      }
    }')

ingests=$(api_es 'traces-*/_search' "$ingest_payload")
rows='[]'

while IFS= read -r handoff; do
    timestamp=$(jq -r '.ingest["@timestamp"] // ""' <<<"$handoff")
    message_type=$(jq -r '.ingest.attributes.message_type // ""' <<<"$handoff")
    destination=$(jq -r '.ingest.attributes["messaging.destination.name"] // ""' <<<"$handoff")
    product_trace_id=$(jq -r '.link.trace_id // ""' <<<"$handoff")
    product_relay_span_id=$(jq -r '.link.span_id // ""' <<<"$handoff")
    order_trace_id=$(jq -r '.ingest.trace_id // ""' <<<"$handoff")
    order_ingest_span_id=$(jq -r '.ingest.span_id // ""' <<<"$handoff")
    order_service=$(jq -r '.ingest.resource.attributes["service.name"] // "unknown-service"' <<<"$handoff")
    order_transaction=$(jq -r '.ingest.name // "kafkaman.ingest"' <<<"$handoff")
    order_transaction_type=$(jq -r '.ingest.attributes["transaction.type"] // "messaging"' <<<"$handoff")

    product_payload=$(jq -n \
        --arg trace "$product_trace_id" \
        '{
          size: 100,
          sort: [{"@timestamp": "asc"}],
          _source: [
            "@timestamp",
            "name",
            "trace_id",
            "span_id",
            "parent_span_id",
            "duration",
            "attributes.transaction.root",
            "attributes.transaction.type",
            "resource.attributes.service.name"
          ],
          query: {term: {trace_id: $trace}}
        }')
    product_docs=$(api_es 'traces-*/_search' "$product_payload")

    order_payload=$(jq -n \
        --arg trace "$order_trace_id" \
        '{
          size: 100,
          sort: [{"@timestamp": "asc"}],
          _source: [
            "@timestamp",
            "name",
            "trace_id",
            "span_id",
            "parent_span_id",
            "duration",
            "resource.attributes.service.name"
          ],
          query: {term: {trace_id: $trace}}
        }')
    order_docs=$(api_es 'traces-*/_search' "$order_payload")

    product_root=$(jq -c '
      [.hits.hits[]._source
       | select(.attributes["transaction.root"] == true)]
      | first // empty
    ' <<<"$product_docs")

    if [[ -z "$product_root" ]]; then
        product_root=$(jq -c '
          [.hits.hits[]._source
           | select(has("parent_span_id") | not)]
          | first // empty
        ' <<<"$product_docs")
    fi

    # Still empty means the producer trace this ingest links to has not been
    # indexed yet — Elasticsearch refreshes on its own schedule, and the consumer
    # side can land first. Report it and move on: jq on empty input produces no
    # output at all, so the `//` defaults below would never fire and every field
    # would silently become the empty string, printing a URL that goes nowhere.
    if [[ -z "$product_root" ]]; then
        echo "skipping ${order_trace_id:-a handoff}: producer trace ${product_trace_id} is not indexed yet" >&2
        continue
    fi

    product_service=$(jq -r '.resource.attributes["service.name"] // "unknown-service"' <<<"$product_root")
    product_transaction=$(jq -r '.name // "unknown transaction"' <<<"$product_root")
    product_transaction_type=$(jq -r '.attributes["transaction.type"] // "request"' <<<"$product_root")
    product_transaction_id=$(jq -r '.span_id // ""' <<<"$product_root")

    if [[ -n "$PRODUCER_SERVICE" && "$product_service" != "$PRODUCER_SERVICE" ]]; then
        continue
    fi

    if [[ -n "$PRODUCER_TRANSACTION" && "$product_transaction" != "$PRODUCER_TRANSACTION" ]]; then
        continue
    fi

    product_relay=$(jq -c \
        --arg span "$product_relay_span_id" \
        '[.hits.hits[]._source | select(.span_id == $span)] | first // {}' \
        <<<"$product_docs")
    product_relay_name=$(jq -r '.name // "kafkaman.relay.publish"' <<<"$product_relay")

    product_flow=$(jq -r \
        '[.hits.hits[]._source.name]
         | unique
         | join(" -> ")' \
        <<<"$product_docs")
    order_flow=$(jq -r \
        '[.hits.hits[]._source.name]
         | unique
         | join(" -> ")' \
        <<<"$order_docs")

    product_url=$(apm_url \
        "$product_service" \
        "$product_transaction" \
        "$product_transaction_type" \
        "$product_trace_id" \
        "$product_transaction_id")
    order_url=$(apm_url \
        "$order_service" \
        "$order_transaction" \
        "$order_transaction_type" \
        "$order_trace_id" \
        "$order_ingest_span_id")

    row=$(jq -nc \
        --arg timestamp "$timestamp" \
        --arg message_type "$message_type" \
        --arg destination "$destination" \
        --arg product_service "$product_service" \
        --arg product_transaction "$product_transaction" \
        --arg product_trace_id "$product_trace_id" \
        --arg product_transaction_id "$product_transaction_id" \
        --arg product_relay "$product_relay_name" \
        --arg product_relay_span_id "$product_relay_span_id" \
        --arg product_url "$product_url" \
        --arg product_flow "$product_flow" \
        --arg order_service "$order_service" \
        --arg order_transaction "$order_transaction" \
        --arg order_trace_id "$order_trace_id" \
        --arg order_transaction_id "$order_ingest_span_id" \
        --arg order_url "$order_url" \
        --arg order_flow "$order_flow" \
        '{
          timestamp: $timestamp,
          message_type: $message_type,
          destination: $destination,
          producer: {
            service: $product_service,
            transaction: $product_transaction,
            trace_id: $product_trace_id,
            transaction_id: $product_transaction_id,
            relay_span: $product_relay,
            relay_span_id: $product_relay_span_id,
            waterfall_url: $product_url,
            flow: $product_flow
          },
          consumer: {
            service: $order_service,
            transaction: $order_transaction,
            trace_id: $order_trace_id,
            transaction_id: $order_transaction_id,
            waterfall_url: $order_url,
            flow: $order_flow
          }
        }')

    rows=$(jq -c --argjson row "$row" '. + [$row]' <<<"$rows")
done < <(jq -c '
    .hits.hits[]._source as $ingest
    | $ingest.links[]?
    | {ingest: $ingest, link: .}
  ' <<<"$ingests")

case "$OUTPUT" in
  json)
    jq . <<<"$rows"
    ;;
  text)
    if [[ "$(jq 'length' <<<"$rows")" -eq 0 ]]; then
        echo "No linked async handoffs found in $TRACE_RANGE."
        echo "Filters: MESSAGE_TYPE=${MESSAGE_TYPE:-*} PRODUCER_SERVICE=${PRODUCER_SERVICE:-*} PRODUCER_TRANSACTION=${PRODUCER_TRANSACTION:-*} CONSUMER_SERVICE=${CONSUMER_SERVICE:-*}"
        echo "Run just examples observe or just examples all with kafka_trace_handoff = \"linked\", then try again."
        exit 0
    fi

    echo "Linked Kafka trace handoffs in $TRACE_RANGE, newest first."
    echo "Filters: MESSAGE_TYPE=${MESSAGE_TYPE:-*} PRODUCER_SERVICE=${PRODUCER_SERVICE:-*} PRODUCER_TRANSACTION=${PRODUCER_TRANSACTION:-*} CONSUMER_SERVICE=${CONSUMER_SERVICE:-*}"
    echo "Product-create path: MESSAGE_TYPE=product_snapshot CONSUMER_SERVICE=kafkaman-example-order PRODUCER_TRANSACTION='POST /products' LIMIT=100 just examples handoffs"
    echo ""

    jq -r '
      to_entries[]
      | (.key + 1) as $n
      | .value as $row
      | "\($n). \($row.timestamp) \($row.message_type)"
        + (if $row.destination == "" then "" else " on \($row.destination)" end)
        + "\n   producer: \($row.producer.service) \($row.producer.transaction) -> \($row.producer.relay_span)"
        + "\n   consumer: \($row.consumer.service) \($row.consumer.transaction)"
        + "\n   producer waterfall: \($row.producer.waterfall_url)"
        + "\n   consumer waterfall: \($row.consumer.waterfall_url)"
        + "\n"
    ' <<<"$rows"
    ;;
  *)
    echo "OUTPUT must be text or json" >&2
    exit 1
    ;;
esac
