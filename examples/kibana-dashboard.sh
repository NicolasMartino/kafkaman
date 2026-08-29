#!/usr/bin/env bash
#
# Create the Kibana dashboard for the example's telemetry.
#
# This is intentionally built from saved Discover panels rather than a large
# Lens export. The first thing a developer needs is a separated view of traces,
# queue metrics and logs; raw Discover over `*-generic.otel-*` mixes all three
# signal streams and makes metric documents look like log volume. APM is the
# primary waterfall view for the parented example trace shape; the handoff panel
# remains a Discover table for inspecting either parented or linked Kafka
# boundaries directly.
#
# Idempotent: run it against a live observed stack as often as you like.
#
#     just examples all
#     examples/kibana-dashboard.sh
#
# Override the endpoint when Kibana is not on the compose default:
#     KIBANA_URL=http://127.0.0.1:5601

set -euo pipefail

KIBANA_URL="${KIBANA_URL:-http://127.0.0.1:${KIBANA_PORT:-5601}}"
KIBANA_READY_SECONDS="${KIBANA_READY_SECONDS:-90}"

DATA_VIEW_TITLE='*-generic.otel-*'
DATA_VIEW_NAME='kafkaman telemetry'
DATA_VIEW_ID=''
INDEX_REF='kibanaSavedObjectMeta.searchSourceJSON.index'
DASHBOARD_ID='kafkaman-telemetry-dashboard'
DASHBOARD_TITLE='kafkaman telemetry'
DASHBOARD_TIME_FROM='now-4h'
DASHBOARD_URL_STATE='_g=(time:(from:now-4h,to:now),filters:!())'

readonly KIBANA_URL KIBANA_READY_SECONDS DATA_VIEW_TITLE DATA_VIEW_NAME INDEX_REF
readonly DASHBOARD_ID DASHBOARD_TITLE DASHBOARD_TIME_FROM DASHBOARD_URL_STATE

command -v curl >/dev/null 2>&1 || { echo "curl is required but not installed" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "jq is required but not installed" >&2; exit 1; }

api_get() {
    curl -fsS --max-time 10 "$KIBANA_URL$1"
}

api_write() {
    local method="$1"
    local path="$2"
    local payload="${3:-}"

    if [[ -n "$payload" ]]; then
        curl -fsS --max-time 10 -X "$method" "$KIBANA_URL$path" \
            -H 'content-type: application/json' \
            -H 'kbn-xsrf: true' \
            -d "$payload" >/dev/null
    else
        curl -fsS --max-time 10 -X "$method" "$KIBANA_URL$path" \
            -H 'kbn-xsrf: true' >/dev/null
    fi
}

wait_for_kibana() {
    local ready=""
    local level=""
    for _ in $(seq "$KIBANA_READY_SECONDS"); do
        level=$(api_get /api/status 2>/dev/null \
                | jq -r '.status.overall.level // empty' 2>/dev/null) || level=""
        if [[ "$level" == "available" ]]; then ready=yes; break; fi
        sleep 1
    done

    if [[ -z "$ready" ]]; then
        echo "Kibana at $KIBANA_URL did not report available within ${KIBANA_READY_SECONDS}s" >&2
        echo "logs: docker compose -f examples/compose.yaml logs kibana" >&2
        exit 1
    fi
}

ensure_data_view() {
    env \
        KIBANA_URL="$KIBANA_URL" \
        KIBANA_READY_SECONDS="$KIBANA_READY_SECONDS" \
        examples/kibana-data-view.sh >/dev/null

    DATA_VIEW_ID=$(api_get /api/data_views \
        | jq -r --arg title "$DATA_VIEW_TITLE" \
            '[.data_view[]? | select(.title == $title) | .id] | first // empty')

    if [[ -z "$DATA_VIEW_ID" ]]; then
        echo "Kibana data view '$DATA_VIEW_NAME' was not found after creation" >&2
        exit 1
    fi
}

upsert_search() {
    local id="$1"
    local title="$2"
    local description="$3"
    local query="$4"
    local columns="$5"
    local search_source=""
    local payload=""

    search_source=$(jq -nc \
        --arg query "$query" \
        --arg ref "$INDEX_REF" \
        '{query: {language: "kuery", query: $query}, filter: [], indexRefName: $ref}')

    payload=$(jq -n \
        --arg title "$title" \
        --arg description "$description" \
        --argjson columns "$columns" \
        --arg searchSourceJSON "$search_source" \
        --arg dataView "$DATA_VIEW_ID" \
        --arg ref "$INDEX_REF" \
        '{
          attributes: {
            title: $title,
            description: $description,
            columns: $columns,
            sort: [["@timestamp", "desc"]],
            kibanaSavedObjectMeta: {searchSourceJSON: $searchSourceJSON}
          },
          references: [{name: $ref, type: "index-pattern", id: $dataView}]
        }')

    api_write POST "/api/saved_objects/search/$id?overwrite=true" "$payload"
}

upsert_dashboard() {
    local version=""
    local panels=""
    local options=""
    local search_source=""
    local payload=""

    version=$(api_get /api/status | jq -r '.version.number // "9.3.5"')
    # Kibana's grid is 48 columns wide and `y` is an absolute row offset, not a
    # flow position, so the seven panels below tile as:
    #
    #   y=0   handoffs       full width, h=10
    #   y=10  errors         full width, h=12
    #   y=22  failed txs     full width, h=10
    #   y=32  failure detail full width, h=14
    #   y=46  traces         left half,  h=16   queue  right half, h=16
    #   y=62  logs           full width, h=16
    #
    # Each `y` is the previous row's `y + h`. Overlapping values do not error —
    # Kibana just stacks the panels on top of each other — so change these
    # together or the dashboard silently comes back wrong.
    #
    # Errors sit above failed transactions because they are the narrower
    # question: an error names what went wrong and groups by it, a failed
    # transaction only says that something did.
    panels=$(jq -nc --arg version "$version" '[
      {
        version: $version,
        type: "search",
        gridData: {x: 0, y: 0, w: 48, h: 10, i: "handoffs"},
        panelIndex: "handoffs",
        embeddableConfig: {},
        panelRefName: "panel_0"
      },
      {
        version: $version,
        type: "search",
        gridData: {x: 0, y: 10, w: 48, h: 12, i: "errors"},
        panelIndex: "errors",
        embeddableConfig: {},
        panelRefName: "panel_6"
      },
      {
        version: $version,
        type: "search",
        gridData: {x: 0, y: 22, w: 48, h: 10, i: "failed-transactions"},
        panelIndex: "failed-transactions",
        embeddableConfig: {},
        panelRefName: "panel_4"
      },
      {
        version: $version,
        type: "search",
        gridData: {x: 0, y: 32, w: 48, h: 14, i: "failures"},
        panelIndex: "failures",
        embeddableConfig: {},
        panelRefName: "panel_5"
      },
      {
        version: $version,
        type: "search",
        gridData: {x: 0, y: 46, w: 24, h: 16, i: "traces"},
        panelIndex: "traces",
        embeddableConfig: {},
        panelRefName: "panel_1"
      },
      {
        version: $version,
        type: "search",
        gridData: {x: 24, y: 46, w: 24, h: 16, i: "queue"},
        panelIndex: "queue",
        embeddableConfig: {},
        panelRefName: "panel_2"
      },
      {
        version: $version,
        type: "search",
        gridData: {x: 0, y: 62, w: 48, h: 16, i: "logs"},
        panelIndex: "logs",
        embeddableConfig: {},
        panelRefName: "panel_3"
      }
    ]')
    options=$(jq -nc '{hidePanelTitles: false, useMargins: true}')
    search_source=$(jq -nc '{query: {query: "", language: "kuery"}, filter: []}')

    payload=$(jq -n \
        --arg title "$DASHBOARD_TITLE" \
        --arg timeFrom "$DASHBOARD_TIME_FROM" \
        --arg panelsJSON "$panels" \
        --arg optionsJSON "$options" \
        --arg searchSourceJSON "$search_source" \
        '{
          attributes: {
            title: $title,
            description: "Curated view of the example stack telemetry: async Kafka handoffs, failures, traces, queue metrics, and service logs.",
            hits: 0,
            panelsJSON: $panelsJSON,
            optionsJSON: $optionsJSON,
            timeRestore: true,
            timeFrom: $timeFrom,
            timeTo: "now",
            refreshInterval: {pause: false, value: 15000},
            kibanaSavedObjectMeta: {searchSourceJSON: $searchSourceJSON}
          },
          references: [
            # The visual grid intentionally places failed receive work before
            # the general trace and metric panels, so the panel reference order
            # differs from creation order below.
            {name: "panel_0", type: "search", id: "kafkaman-async-handoffs"},
            {name: "panel_1", type: "search", id: "kafkaman-recent-traces"},
            {name: "panel_2", type: "search", id: "kafkaman-queue-metrics"},
            {name: "panel_3", type: "search", id: "kafkaman-service-logs"},
            {name: "panel_4", type: "search", id: "kafkaman-failed-transactions"},
            {name: "panel_5", type: "search", id: "kafkaman-failures"},
            {name: "panel_6", type: "search", id: "kafkaman-errors"}
          ]
        }')

    api_write POST "/api/saved_objects/dashboard/$DASHBOARD_ID?overwrite=true" "$payload"
}

wait_for_kibana
ensure_data_view

upsert_search \
    kafkaman-async-handoffs \
    'kafkaman Kafka trace handoffs' \
    'Consumer ingest spans. attributes.service.name repeats the resource service for row-level scanning. In parented mode parent_span_id points to the producer relay span; in linked mode links.trace_id and links.span_id point back to it.' \
    'data_stream.type: traces and name: kafkaman.ingest and (parent_span_id:* or links.trace_id:*)' \
    '["@timestamp","attributes.service.name","resource.attributes.service.name","name","attributes.message_type","attributes.messaging.destination.name","trace_id","span_id","parent_span_id","links.trace_id","links.span_id"]'

upsert_search \
    kafkaman-recent-traces \
    'kafkaman recent waterfall spans' \
    'Recent HTTP, SQL, and kafkaman spans from the example services. attributes.service.name is copied from the span resource so each row shows its producing service.' \
    'data_stream.type: traces and (attributes.http.route:* or attributes.db.query.summary:* or name: kafkaman.*)' \
    '["@timestamp","attributes.service.name","resource.attributes.service.name","name","kind","attributes.http.route","attributes.http.request.method","attributes.http.response.status_code","attributes.message_type","attributes.messaging.destination.name","attributes.db.query.summary","duration","trace_id","span_id","parent_span_id","links.trace_id"]'

# The failure panel. Empty on a stack that has never been driven through
# `examples/faults.sh`, which is the point: before that script existed, this
# query matched nothing at all across 227,394 indexed spans.
#
# `status.code` is where the OTel mapping puts a span's status, and it is present
# *only* on spans that reported one — `kafkaman_core::record_error` sets it, and
# nothing sets it on success. So this is an exhaustive list of what the system
# reported as failed, with no exclusions to maintain.
#
# `attributes.handler.outcome` is what separates a handler that returned an error
# from one that panicked. They share a `ReceivedFailureKind` on purpose, because
# that value is a permanent RFC 9457 URI; the distinction lives here instead.
# The errors panel, and the one that was impossible until kafkaman emitted
# OpenTelemetry `exception` span events. Elastic's `elasticapm` processor turns
# each one into an APM *error* document — verified live on this stack rather than
# assumed: `attributes.processor.event: error`, with Elastic deriving
# `error.grouping_key` and `error.grouping_name` from the exception it was built
# from. That is what the APM Errors UI groups on.
#
# Filtered to `processor.event: error` because the error documents share
# `logs-generic.otel-default` with every ordinary log record the services emit;
# without the filter this panel is the log stream with a few errors in it.
#
# It used to be load-bearing for a second reason. Each exception landed *twice*
# — once as the error document, once as a plain log record the same
# `tracing::error!` produced through the OTLP log bridge — so an unfiltered query
# doubled every count. Measured on this stack: 13 of 13 ERROR log records were
# those duplicates, each with an empty body, because an exception event carries
# no message. `kafkaman_otel::init` now excludes the exception target from log
# export, so they are gone; the filter stays for the reason above.
#
# `exception.type` is the permanent problem URI, which is deliberately finer than
# the four `ReceivedFailureKind` values written into DLQ rows: a handler that
# panicked reports `urn:kafkaman:problem:handler-panicked` and groups separately
# from one that returned an error, while both dead-letter as the same stored
# kind.
upsert_search \
    kafkaman-errors \
    'kafkaman errors' \
    'APM error documents, derived from the exception span events kafkaman emits on every durable-path failure. Group by exception.type for the problem class, or by error.grouping_name for the message. trace_id and span_id pivot to the failed transaction. Drive some with: just examples faults' \
    'data_stream.type: logs and attributes.processor.event: error' \
    '["@timestamp","resource.attributes.service.name","attributes.exception.type","attributes.exception.message","attributes.error.grouping_name","attributes.error.exception.handled","trace_id","span_id"]'

upsert_search \
    kafkaman-failed-transactions \
    'kafkaman failed transactions' \
    'APM transaction documents for failed durable receive attempts. Use transaction.type to separate messaging work from HTTP requests, and kafkaman.failure.stage/kind to group by where the failure was recorded. Drive some with: just examples faults' \
    'data_stream.type: traces and attributes.processor.event: transaction and (status.code: Error or attributes.event.outcome: failure)' \
    '["@timestamp","attributes.service.name","resource.attributes.service.name","name","attributes.transaction.type","status.code","attributes.event.outcome","attributes.kafkaman.failure.stage","attributes.kafkaman.failure.kind","attributes.error.type","attributes.message_type","status.message","duration","trace_id","span_id"]'

upsert_search \
    kafkaman-failures \
    'kafkaman failure details' \
    'All spans that reported a failure status, newest first. status.message carries the error, kafkaman.failure.stage/kind classify it, and handler.outcome marks handler panics rather than returned errors. Drive some with: just examples faults' \
    'data_stream.type: traces and status.code: Error' \
    '["@timestamp","attributes.service.name","name","status.message","attributes.event.outcome","attributes.kafkaman.failure.stage","attributes.kafkaman.failure.kind","attributes.error.type","attributes.kafkaman.failure.type","attributes.handler.outcome","attributes.message_type","attributes.handler.position","attributes.http.route","attributes.http.response.status_code","duration","trace_id","span_id"]'

upsert_search \
    kafkaman-queue-metrics \
    'kafkaman queue metrics' \
    'Queue depth, age, and sampler freshness from kafkaman runtime loops.' \
    'data_stream.type: metrics and (metrics.kafkaman.outbox.depth:* or metrics.kafkaman.received.depth:* or metrics.kafkaman.queue.sample_age:*)' \
    '["@timestamp","resource.attributes.service.name","attributes.message_type","attributes.status","metrics.kafkaman.outbox.depth","metrics.kafkaman.received.depth","metrics.kafkaman.outbox.oldest_age","metrics.kafkaman.received.oldest_age","metrics.kafkaman.queue.sample_age"]'

upsert_search \
    kafkaman-service-logs \
    'kafkaman service logs' \
    'Service and runtime log records exported over OTLP.' \
    'data_stream.type: logs' \
    '["@timestamp","resource.attributes.service.name","severity_text","scope.name","body.text","trace_id","span_id"]'

# Kept out of the dashboard because it reintroduces the original problem: a
# mixed-signal panel dominated by metrics. Delete the old object if a previous
# local run created it.
api_write DELETE /api/saved_objects/search/kafkaman-signal-activity 2>/dev/null || true

upsert_dashboard

echo "Kibana dashboard '$DASHBOARD_TITLE' ready:"
echo "  $KIBANA_URL/app/dashboards#/view/$DASHBOARD_ID?$DASHBOARD_URL_STATE"
