#!/usr/bin/env bash
#
# Create the Kibana data view the example's telemetry lands in.
#
# Without this, Kibana opens on an empty Discover and the developer is told to
# hand-build a data view from a pattern printed in a terminal — which is the
# last manual step between "the stack is up" and "the telemetry is visible", and
# the one most likely to be got wrong or skipped.
#
# The pattern is `*-generic.otel-*` rather than one view per signal. The
# collector writes with `mapping.mode: otel` (examples/otel-collector.yaml),
# which produces the `traces-generic.otel-default`, `logs-generic.otel-default`
# and `metrics-generic.otel-default` data streams; one view over all three is
# what lets Discover pivot from a log record to the span it was emitted inside,
# which is the correlation the observability profile exists to demonstrate.
#
# Idempotent: run it against a live stack as often as you like.
#
#     just examples all        # brings the stack up and runs this
#     examples/kibana-data-view.sh   # against a stack you started yourself
#
# Override the endpoint when Kibana is not on the compose default:
#     KIBANA_URL=http://127.0.0.1:5601

set -euo pipefail

KIBANA_URL="${KIBANA_URL:-http://127.0.0.1:${KIBANA_PORT:-5601}}"
# How long to wait for Kibana to report itself available. Its compose
# healthcheck gates on `/api/status` answering at all, which happens while the
# saved-objects API is still starting; asking for `overall.level` is the
# difference between a 200 and a Kibana that can actually store something.
READY_SECONDS="${KIBANA_READY_SECONDS:-90}"

TITLE='*-generic.otel-*'
NAME='kafkaman telemetry'

readonly KIBANA_URL READY_SECONDS TITLE NAME

command -v jq >/dev/null 2>&1 || { echo "jq is required but not installed" >&2; exit 1; }

ready=""
for _ in $(seq "$READY_SECONDS"); do
    level=$(curl -fsS --max-time 5 "$KIBANA_URL/api/status" 2>/dev/null \
            | jq -r '.status.overall.level // empty' 2>/dev/null) || level=""
    if [[ "$level" == "available" ]]; then ready=yes; break; fi
    sleep 1
done

if [[ -z "$ready" ]]; then
    echo "Kibana at $KIBANA_URL did not report available within ${READY_SECONDS}s" >&2
    echo "logs: docker compose -f examples/compose.yaml logs kibana" >&2
    exit 1
fi

# Ask before creating rather than creating and interpreting the error. The data
# views API does not treat a repeated title as a conflict — it would happily
# make a second view with the same pattern — so a re-run has to be idempotent
# here rather than at the server.
existing=$(curl -fsS --max-time 10 "$KIBANA_URL/api/data_views" \
           | jq -r --arg title "$TITLE" \
                '[.data_view[]? | select(.title == $title) | .id] | first // empty')

if [[ -n "$existing" ]]; then
    echo "Kibana data view '$NAME' already exists ($existing)"
    exit 0
fi

created=$(curl -fsS --max-time 10 -X POST "$KIBANA_URL/api/data_views/data_view" \
               -H 'content-type: application/json' \
               -H 'kbn-xsrf: true' \
               -d "$(jq -n --arg title "$TITLE" --arg name "$NAME" \
                        '{data_view: {title: $title, name: $name, timeFieldName: "@timestamp"}}')" \
          | jq -r '.data_view.id // empty')

if [[ -z "$created" ]]; then
    echo "creating the Kibana data view failed" >&2
    exit 1
fi

echo "Kibana data view '$NAME' created ($created)"
