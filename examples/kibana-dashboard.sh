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
    # flow position, so the four panels below tile as:
    #
    #   y=0   handoffs   full width, h=10
    #   y=10  traces     left half,  h=16   queue  right half, h=16
    #   y=26  logs       full width, h=16
    #
    # Each `y` is the previous row's `y + h`. Overlapping values do not error —
    # Kibana just stacks the panels on top of each other — so change these
    # together or the dashboard silently comes back wrong.
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
        gridData: {x: 0, y: 10, w: 24, h: 16, i: "traces"},
        panelIndex: "traces",
        embeddableConfig: {},
        panelRefName: "panel_1"
      },
      {
        version: $version,
        type: "search",
        gridData: {x: 24, y: 10, w: 24, h: 16, i: "queue"},
        panelIndex: "queue",
        embeddableConfig: {},
        panelRefName: "panel_2"
      },
      {
        version: $version,
        type: "search",
        gridData: {x: 0, y: 26, w: 48, h: 16, i: "logs"},
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
            description: "Curated view of the example stack telemetry: async Kafka handoffs, traces, queue metrics, and service logs.",
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
            {name: "panel_0", type: "search", id: "kafkaman-async-handoffs"},
            {name: "panel_1", type: "search", id: "kafkaman-recent-traces"},
            {name: "panel_2", type: "search", id: "kafkaman-queue-metrics"},
            {name: "panel_3", type: "search", id: "kafkaman-service-logs"}
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
