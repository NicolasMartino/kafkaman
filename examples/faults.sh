#!/usr/bin/env bash
#
# Drives the example stack's failure paths and asserts what each one does.
#
# The happy path had a script (`examples/smoke.sh`) and the failure paths had
# nothing, so the half of kafkaman that exists for when things go wrong —
# backoff, the attempt budget, the dead-letter queue, redrive, ingest quarantine
# — was demonstrated by nothing. Measured against the running stack before this
# existed: 227,394 spans indexed, zero carrying a failure status, zero log
# records above INFO, and every DLQ empty.
#
# Assumes the stack is already up:
#     just examples faults      # runs everything below
#     just examples all         # runs scenarios 1 and 2, so Kibana has errors
#     examples/faults.sh        # against a stack you started yourself
#
# Pick scenarios by number:
#     FAULT_SCENARIOS="1 2" examples/faults.sh
#
# Scenarios 5, 6 and 7 need Docker — for the broker, or to read the service's own
# tables — and are skipped, loudly, when it is not available.

set -euo pipefail

ORDER_URL="${ORDER_URL:-http://127.0.0.1:3001}"
PRODUCT_URL="${PRODUCT_URL:-http://127.0.0.1:3002}"
COMPOSE_FILE="${COMPOSE_FILE:-$(dirname "$0")/compose.yaml}"
# Only scenario 7 reads it, and only for the half of its assertion that has
# nowhere else to live: the fine failure class exists on the span and in APM and
# in no table. Absent when the stack was started without the observability
# profile, which is a skip rather than a failure.
ES_URL="${ES_URL:-http://127.0.0.1:${ELASTICSEARCH_PORT:-9200}}"

# All seven by default. `just examples all` narrows this to the two that are quick
# and leave something behind worth looking at.
FAULT_SCENARIOS="${FAULT_SCENARIOS:-1 2 3 4 5 6 7}"

# The configured budget for `order_snapshot` in examples/product/kafkaman.toml.
# Asserted rather than merely used: a service booted through `RuntimeBuilder`
# ignored its whole `[retry]` section for as long as nothing deliberately failed,
# retrying ten times on library defaults instead of the eight declared here.
EXPECTED_MAX_ATTEMPTS=8

# Eight attempts at 250ms doubling to a 10s cap is 12.9-25.75s of backoff once
# jitter is applied, plus dispatch. Measured at 19s; this is that with room.
DLQ_DEADLINE_SECONDS="${DLQ_DEADLINE_SECONDS:-60}"
CONVERGE_DEADLINE_SECONDS="${CONVERGE_DEADLINE_SECONDS:-90}"
POLL_SECONDS=0.25

readonly ORDER_URL PRODUCT_URL COMPOSE_FILE ES_URL EXPECTED_MAX_ATTEMPTS
readonly DLQ_DEADLINE_SECONDS CONVERGE_DEADLINE_SECONDS POLL_SECONDS

pass() { printf '  \033[32m✓\033[0m %s\n' "$1"; }
step() { printf '\n\033[1m%s\033[0m\n' "$1"; }
skip() { printf '  \033[33m—\033[0m %s\n' "$1"; }
note() { printf '    %s\n' "$1"; }
fail() {
    printf '\n\033[31m✗ %s\033[0m\n' "$1" >&2
    if declare -F disarm >/dev/null 2>&1; then
        disarm >/dev/null 2>&1 || true
    fi
    exit 1
}

for tool in curl jq; do
    command -v "$tool" >/dev/null 2>&1 || fail "$tool is required but not installed"
done

# --- helpers -----------------------------------------------------------------

api() { curl -fsS "$@"; }

# The HTTP status of a GET, with no opinion about whether it is a good one.
#
# Separate from `api` because `curl -f` treats a non-2xx as a failure and prints
# `curl: (22) The requested URL returned error: NNN` to stderr — which is exactly
# what the two probes below are *asking about*. Sharing `api` made a healthy
# convergence poll print an error line for every 409 it waited through, in a
# script whose whole output is a list of ticks.
http_status() { curl -so /dev/null -w '%{http_code}' "$1"; }

arm() {
    api -X POST "$PRODUCT_URL/faults" -H 'content-type: application/json' -d "$1" >/dev/null
}
disarm() { api -X DELETE "$PRODUCT_URL/faults" >/dev/null; }
fired()  { api "$PRODUCT_URL/faults" | jq -er '.fired'; }

dlq_count() { api "$PRODUCT_URL/internal/kafkaman/dlq" | jq -er '[.[].count] | add // 0'; }

# The dead-lettered row for one order, or a non-zero exit while there is none.
#
# Keyed on `entity_key`, which for an `OrderSnapshot` is the order id — see
# `impl KafkaMessage for OrderSnapshot` in examples/contracts. Taking whichever
# row happened to be first instead would assert against a leftover from an
# earlier run whenever the DLQ started non-empty, and the assertions below are
# exact: an attempt count and a failure class that belong to a *different* row
# read as this scenario passing or failing for reasons that have nothing to do
# with it.
dlq_row_for() {
    api "$PRODUCT_URL/internal/kafkaman/dlq" \
        | jq -ec --arg key "$1" 'first(.[].rows[] | select(.entity_key == $key))'
}

# Create a product and block until `order` has it cached, so an order can be
# placed against it. Echoes the product id.
new_product() {
    local name="$1" on_hand="$2" body id
    body=$(api -X POST "$PRODUCT_URL/products" -H 'content-type: application/json' \
                -d "{\"name\":\"$name\",\"price_cents\":100,\"on_hand\":$on_hand}")
    id=$(jq -r '.product_id' <<<"$body")
    [[ -n "$id" && "$id" != "null" ]] || fail "creating product returned: $body"

    local deadline=$((SECONDS + CONVERGE_DEADLINE_SECONDS))
    while (( SECONDS < deadline )); do
        # 409 until the snapshot has propagated, 404 before it exists at all.
        # Both are the answer "not yet", not a failure.
        [[ "$(http_status "$ORDER_URL/products/$id")" == "200" ]] \
            && { printf '%s' "$id"; return 0; }
        sleep "$POLL_SECONDS"
    done
    fail "order never cached product $id"
}

# Place an order. Publishes exactly one OrderSnapshot, which is what `product`'s
# handler — the one the fault switch sits in front of — consumes.
#
# Used where the count of faulted messages has to be exact. A placed order
# changes no availability, so it cannot be used to observe convergence.
place_order() {
    local product_id="$1" quantity="$2" body order_id
    body=$(api -X POST "$ORDER_URL/orders" -H 'content-type: application/json' \
                -d "{\"product_id\":\"$product_id\",\"quantity\":$quantity}")
    order_id=$(jq -r '.order_id' <<<"$body")
    [[ -n "$order_id" && "$order_id" != "null" ]] || fail "creating order returned: $body"
    printf '%s' "$order_id"
}

# Place an order and fulfil it: *two* OrderSnapshots, and therefore up to two
# dispatches. Used where the observable is convergence — availability only moves
# on the fulfil — rather than an exact number of failures.
place_and_fulfil() {
    local order_id
    order_id=$(place_order "$1" "$2")
    api -o /dev/null -X POST "$ORDER_URL/orders/$order_id/fulfil"
    printf '%s' "$order_id"
}

# Block until order's cached view of a product reports `available == $2`.
await_available() {
    local product_id="$1" want="$2"
    local deadline=$((SECONDS + CONVERGE_DEADLINE_SECONDS)) got
    while (( SECONDS < deadline )); do
        got=$(api "$ORDER_URL/products/$product_id" | jq -r '.available // empty')
        [[ "$got" == "$want" ]] && return 0
        sleep "$POLL_SECONDS"
    done
    fail "availability never reached $want (last saw ${got:-<nothing>})"
}

await_dlq_exact() {
    local want="$1"
    local deadline=$((SECONDS + DLQ_DEADLINE_SECONDS)) got
    while (( SECONDS < deadline )); do
        got=$(dlq_count) || fail "could not read DLQ count"
        [[ "$got" == "$want" ]] && return 0
        sleep 0.5
    done
    return 1
}

# Block until this scenario's own order is dead-lettered, echoing its row.
#
# Waiting on a *count* instead — even a count relative to one taken before the
# order was placed — would be satisfied by any other row landing first, and
# would then hand the assertions a row this scenario never created.
await_dlq_row() {
    local key="$1"
    local deadline=$((SECONDS + DLQ_DEADLINE_SECONDS)) row
    while (( SECONDS < deadline )); do
        row=$(dlq_row_for "$key") && { printf '%s' "$row"; return 0; }
        sleep 0.5
    done
    return 1
}

# One field of the newest failure recorded against an order's received row, or a
# non-zero exit while there is none.
#
# Reads the row rather than the DLQ because the failures scenario 7 injects are
# *absorbed*: the question is how each one was classified, and waiting for a
# dead-letter would mean spending an eight-attempt budget to learn it. The
# `errors` array is where both axes live — `type` is what the failure was, and
# `stage` is whose code raised it.
#
# `$key` is a UUID this script just read back from its own API, which is what
# makes interpolating it into the query safe.
received_failure() {
    local value
    value=$(psql_product "select errors -> -1 ->> '$2'
                            from kafkaman.received_order_snapshot
                           where entity_key = '$1'
                             and jsonb_array_length(errors) > 0
                           limit 1")
    [[ -n "$value" ]] || return 1
    printf '%s' "$value"
}

await_received_failure() {
    local deadline=$((SECONDS + DLQ_DEADLINE_SECONDS)) value
    while (( SECONDS < deadline )); do
        value=$(received_failure "$1" "$2") && { printf '%s' "$value"; return 0; }
        sleep "$POLL_SECONDS"
    done
    return 1
}

# Block until an order's received row has been processed, so nothing this
# scenario armed is still in flight.
#
# Load-bearing between modes, not tidiness. The fault switch is one global
# counter, and a row that just failed retries on a 250ms backoff — so arming the
# next mode while the previous row is still due hands that row the new budget.
# Observed exactly that: `contention` fired 13ms after being armed, against the
# message `constraint` had failed, and the order placed for it dispatched cleanly
# with no fault left to catch it.
await_received_processed() {
    local deadline=$((SECONDS + CONVERGE_DEADLINE_SECONDS)) status
    while (( SECONDS < deadline )); do
        status=$(psql_product "select status from kafkaman.received_order_snapshot
                                where entity_key = '$1'")
        [[ "$status" == "Processed" ]] && return 0
        sleep "$POLL_SECONDS"
    done
    return 1
}

es_available() { curl -fsS -o /dev/null --max-time 2 "$ES_URL" 2>/dev/null; }

# Block until Elasticsearch holds an APM error document of one problem type.
#
# Polled rather than read once because everything between the failure and the
# index is asynchronous — batched span export, the collector, ingest, and the
# refresh interval — and none of it is synchronous with the handler that failed.
# Measured on this stack: the document appeared several seconds after the row.
#
# `attributes.processor.event: error` is Elastic's own marker, added by the
# `elasticapm` processor when it turns an `exception` span event into an APM
# error. Filtering on it is what separates the error documents from the ordinary
# log records they share a data stream with.
await_apm_error() {
    local want="$1" deadline=$((SECONDS + 120)) count
    while (( SECONDS < deadline )); do
        count=$(curl -fsS --max-time 5 "$ES_URL/logs-*/_count" \
                     -H 'content-type: application/json' \
                     -d "{\"query\":{\"bool\":{\"filter\":[
                           {\"term\":{\"attributes.processor.event\":\"error\"}},
                           {\"term\":{\"attributes.exception.type\":\"$want\"}}]}}}" \
                 2>/dev/null | jq -r '.count // 0')
        (( ${count:-0} > 0 )) && return 0
        sleep 1
    done
    return 1
}

# `docker compose` against this stack, or a non-zero exit if it is unusable.
compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

docker_available() {
    command -v docker >/dev/null 2>&1 \
        && compose ps --status running --format '{{.Service}}' 2>/dev/null | grep -q redpanda
}

psql_product() {
    compose exec -T postgres psql -U postgres -d product_service -Atc "$1" 2>/dev/null
}

wants() { [[ " $FAULT_SCENARIOS " == *" $1 "* ]]; }

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    disarm >/dev/null 2>&1 || true
    if [[ "${REDPANDA_STOPPED:-0}" == "1" ]]; then
        compose start redpanda >/dev/null 2>&1 || true
    fi
    exit "$status"
}

trap cleanup EXIT INT TERM

# --- preflight ---------------------------------------------------------------

printf '\033[1mkafkaman example fault walkthrough\033[0m\n'
printf '  product %s\n  order   %s\n' "$PRODUCT_URL" "$ORDER_URL"

for scenario in $FAULT_SCENARIOS; do
    case "$scenario" in
        1|2|3|4|5|6|7) ;;
        *) fail "unknown fault scenario '$scenario' (expected numbers 1 through 7)" ;;
    esac
done

api -o /dev/null "$PRODUCT_URL/faults" \
    || fail "no fault endpoint on $PRODUCT_URL — is the stack up? (just examples all)"
api -o /dev/null "$PRODUCT_URL/internal/kafkaman/dlq" \
    || fail "no operator routes on $PRODUCT_URL"

disarm
[[ "$(dlq_count)" == "0" ]] \
    || note "starting with a non-empty DLQ; every assertion below targets a row \
this run created, so leftovers are counted but never inspected"

# --- 1. a transient handler failure is absorbed ------------------------------

if wants 1; then
    step '1. the handler fails twice, and the system converges anyway'
    product=$(new_product "fault-transient" 10)
    before=$(dlq_count)
    arm '{"mode":"error","remaining":2}'
    place_and_fulfil "$product" 3 >/dev/null
    # 10 on hand minus 3 fulfilled. Reaching it at all means the third attempt
    # ran and succeeded: this is the retry budget doing its job, unattended.
    await_available "$product" 7
    pass "availability settled at 7 after two failed attempts"

    [[ "$(fired)" -ge 2 ]] || fail "the fault should have fired twice, saw $(fired)"
    pass "the handler failed $(fired) times on the way there"

    [[ "$(dlq_count)" == "$before" ]] \
        || fail "a transient failure must not dead-letter anything"
    pass "nothing dead-lettered — a retried failure is not an incident"
    disarm
fi

# --- 2. a permanent handler failure spends its budget ------------------------

if wants 2; then
    step "2. the handler fails forever, and the row dead-letters after $EXPECTED_MAX_ATTEMPTS attempts"
    product=$(new_product "fault-permanent" 20)
    arm '{"mode":"error"}'
    # Placed, not fulfilled: one snapshot, so exactly one row spends exactly one
    # budget and the attempt count below is a number rather than a range.
    order=$(place_order "$product" 5)

    row=$(await_dlq_row "$order") || fail \
        "order $order did not dead-letter within ${DLQ_DEADLINE_SECONDS}s (dlq=$(dlq_count))"
    attempts=$(jq -r '.attempts' <<<"$row")
    pass "dead-lettered after $attempts attempts"

    # The number, not just the fact. It is the only end-to-end evidence that the
    # service is retrying on its own configuration rather than on the library's
    # defaults, which is a difference nothing else in the stack can see.
    [[ "$attempts" == "$EXPECTED_MAX_ATTEMPTS" ]] || fail \
        "expected $EXPECTED_MAX_ATTEMPTS attempts, the max_attempts declared for
     order_snapshot in examples/product/kafkaman.toml, but the row shows
     $attempts. A service that retries on library defaults instead of its own
     config looks healthy and is not doing what the operator asked."
    pass "the budget came from kafkaman.toml, not from the library defaults"

    [[ "$(jq -r '.latest_error.type' <<<"$row")" == "urn:kafkaman:problem:handler" ]] \
        || fail "expected a handler failure class, got: $(jq -c '.latest_error' <<<"$row")"
    [[ "$(jq -r '.error_count' <<<"$row")" -ge 2 ]] \
        || fail "the DLQ row should carry its failure history, not just the last one"
    pass "the row carries $(jq -r '.error_count' <<<"$row") recorded failures for triage"
    disarm
fi

# --- 3. redrive ---------------------------------------------------------------

if wants 3; then
    step '3. redrive puts the dead-lettered row back'
    if [[ "$(dlq_count)" == "0" ]]; then
        skip "nothing in the DLQ to redrive (run scenario 2 first)"
    else
        disarm
        redriven=$(api -X POST "$PRODUCT_URL/internal/kafkaman/dlq/order_snapshot/redrive" \
                        -H 'content-type: application/json' -d '{"max_rows":100}' \
                   | jq -r '.redriven')
        [[ "$redriven" -ge 1 ]] || fail "redrive moved nothing: $redriven"
        pass "redrove $redriven row(s) back to Pending"

        await_dlq_exact 0 || fail "the DLQ did not drain after redrive (dlq=$(dlq_count))"
        pass "the DLQ is empty and the rows were processed on the retry"
    fi
fi

# --- 4. a panicking handler does not take the service with it ----------------

if wants 4; then
    step '4. the handler panics three times, and the service stays up'
    product=$(new_product "fault-panic" 30)
    arm '{"mode":"panic","remaining":3}'
    place_and_fulfil "$product" 4 >/dev/null
    await_available "$product" 26
    pass "availability settled at 26 across three panics"

    # `fired` lives in the panicking process's memory and is reset by arming. If
    # a panic had taken the process down — which is what happened before the
    # panic boundary existed — the restarted one would report 0 and a disarmed
    # fault. Reading 3 back is proof of the same process, with no Docker needed.
    [[ "$(fired)" == "3" ]] || fail \
        "expected the fault to report 3 firings; got $(fired). A count of 0 means
     the process restarted, which is a panicking handler killing the service."
    pass "the fault still reports its 3 firings, so the process never restarted"

    [[ "$(http_status "$PRODUCT_URL/health")" =~ ^2 ]] \
        || fail "product stopped serving HTTP"
    pass "HTTP is still served, and dispatch kept running behind it"
    disarm
fi

# --- 5. a poison record is quarantined rather than stalling the partition ----

if wants 5; then
    step '5. an unreadable record is quarantined, and the ones behind it flow'
    if ! docker_available; then
        skip "needs docker compose and a running redpanda"
    else
        before=$(psql_product "select count(*) from kafkaman.received_ingest_failures")
        # A trailing newline is load-bearing: `rpk topic produce` reads
        # newline-delimited records and reports an unterminated one as a read
        # error rather than producing it.
        compose exec -T redpanda rpk topic produce orders --key poison \
            <<<'{"this":"is not an order snapshot"}' >/dev/null
        pass "produced a malformed record onto the orders topic"

        deadline=$((SECONDS + 60))
        while (( SECONDS < deadline )); do
            after=$(psql_product "select count(*) from kafkaman.received_ingest_failures")
            [[ "$after" -gt "$before" ]] && break
            sleep 0.5
        done
        [[ "${after:-0}" -gt "${before:-0}" ]] \
            || fail "the record was not quarantined (ingest failures still $before)"
        pass "quarantined: $(psql_product "select failure_kind from kafkaman.received_ingest_failures order by created_at desc limit 1")"

        # The assertion that matters. A record that can never become a row must
        # not block the partition, or one bad producer stops every later message
        # behind it — permanently, since retrying cannot change the outcome.
        product=$(new_product "post-poison" 12)
        place_and_fulfil "$product" 2 >/dev/null
        await_available "$product" 10
        pass "a good record produced after it converged normally"
    fi
fi

# --- 6. a broker outage ------------------------------------------------------

if wants 6; then
    step '6. the broker goes away, the outbox backs up, and it drains on return'
    if ! docker_available; then
        skip "needs docker compose and a running redpanda"
    else
        compose stop redpanda >/dev/null 2>&1
        REDPANDA_STOPPED=1
        pass "stopped redpanda"

        api -o /dev/null -X POST "$PRODUCT_URL/products" -H 'content-type: application/json' \
            -d '{"name":"during-outage","price_cents":100,"on_hand":5}'
        pass "the write was still accepted — the outbox is what makes that safe"

        # Backlog is `Pending` plus `Publishing`, not "everything that is not
        # `Published`". `Superseded` is terminal — a row a newer snapshot of the
        # same entity overtook before the relay reached it — so it is drained
        # work, not pending work, and counting it makes this scenario fail
        # against any stack that has already served traffic. It also inflates the
        # during-outage count, which would let the first assertion below pass
        # without the outage having backed anything up at all.
        deadline=$((SECONDS + 60))
        pending=0
        while (( SECONDS < deadline )); do
            pending=$(api "$PRODUCT_URL/internal/kafkaman/outbox" \
                      | jq -r '[.[] | select(.status == "Pending" or .status == "Publishing") | .count] | add // 0')
            (( pending > 0 )) && break
            sleep 0.5
        done
        (( pending > 0 )) || fail "the outbox never showed unpublished rows during the outage"
        pass "the outbox is holding $pending unpublished row(s)"

        compose start redpanda >/dev/null 2>&1
        REDPANDA_STOPPED=0
        pass "started redpanda"

        deadline=$((SECONDS + 120))
        while (( SECONDS < deadline )); do
            pending=$(api "$PRODUCT_URL/internal/kafkaman/outbox" \
                      | jq -r '[.[] | select(.status == "Pending" or .status == "Publishing") | .count] | add // 0')
            (( pending == 0 )) && break
            sleep 1
        done
        (( pending == 0 )) || fail "the outbox did not drain after the broker returned ($pending left)"
        pass "the backlog drained with no message lost and nothing to replay by hand"
    fi
fi

# --- 7. database failures are classified by what the database refused --------

if wants 7; then
    step '7. three database failures, three classes, all blamed on the handler'
    if ! docker_available; then
        skip "needs docker compose to read the service's own tables"
    else
        # One order per mode, each armed for a single firing: the row fails once,
        # is retried, and converges. Nothing dead-letters, which is the point — a
        # failure does not have to be terminal to be worth classifying, and these
        # three are the ones an operator most wants told apart. A deadlock is
        # nobody's bug and the retry is the right answer; a check violation will
        # fail identically on all eight attempts; a malformed statement is a bug
        # that shipped.
        product=$(new_product "fault-sql" 40)
        for mode in constraint contention statement; do
            arm "{\"mode\":\"$mode\",\"remaining\":1}"
            # Placed, not fulfilled: one snapshot, so exactly one row fails and
            # the entry read back below is unambiguously this mode's.
            order=$(place_order "$product" 1)

            # The row stores the *coarse* class, and that is by design rather
            # than a shortcoming. `ReceivedFailureKind` has four permanent
            # values because they are written into rows and cannot churn, so
            # fourteen of the eighteen problem URIs collapse onto
            # `infrastructure`. Asserting the fine class here would be asserting
            # something the row has never claimed to hold.
            got=$(await_received_failure "$order" type) \
                || fail "the $mode fault recorded no failure against order $order"
            [[ "$got" == "urn:kafkaman:problem:infrastructure" ]] \
                || fail "the $mode fault stored $got, expected infrastructure"

            # The axis the row *is* uniquely good for. A database error returned
            # by a handler is an infrastructure failure raised in the handler's
            # frame — not a handler failure. Before the two axes were separated
            # the row could only say one of those, and it said `handler`, so an
            # operator filtering for infrastructure problems missed every one a
            # handler had touched.
            stage=$(received_failure "$order" stage) \
                || fail "the $mode failure recorded no stage"
            [[ "$stage" == "handler" ]] \
                || fail "the $mode failure was blamed on '$stage', expected 'handler'"
            pass "$mode → infrastructure at stage=handler; what broke and where, apart"

            disarm
            # Before the next mode is armed. See `await_received_processed`.
            await_received_processed "$order" \
                || fail "the $mode row never converged after its retry"
        done

        # The other half, and the reason the coarsening above is acceptable: the
        # fine class is on the span, and therefore in APM, where the three modes
        # are distinguishable and the row's four values could never tell them
        # apart. There is no table to read this from — that is the whole point.
        if es_available; then
            for class in constraint contention statement; do
                await_apm_error "urn:kafkaman:problem:$class" \
                    || fail "no APM error group for urn:kafkaman:problem:$class"
                pass "APM groups an error under urn:kafkaman:problem:$class"
            done
        else
            skip "no Elasticsearch on $ES_URL — the fine class lives only on the \
span, so this half needs the observability profile (just examples all)"
        fi

        # And it converged regardless. A database error inside the handler aborts
        # the dispatch transaction; recovering from that is what the handler
        # savepoint is for, and a service that could not would be stuck here.
        #
        # 40 on hand minus the 6 just fulfilled. The three orders above were
        # placed and never fulfilled, so they move nothing — which is why they
        # could be spent on failures without disturbing this number.
        place_and_fulfil "$product" 6 >/dev/null
        await_available "$product" 34
        pass "availability settled at 34 — the aborted transactions were unwound cleanly"
    fi
fi

# --- done --------------------------------------------------------------------

disarm
printf '\n\033[32mAll selected scenarios passed.\033[0m\n'
printf '  Kibana now has failed spans, WARN/ERROR logs, and a DLQ history to read.\n'
printf '  Fault switch:  %s/faults      (also in %s/swagger-ui)\n' "$PRODUCT_URL" "$PRODUCT_URL"
printf '  Operator view: %s/internal/kafkaman/dlq\n' "$PRODUCT_URL"
