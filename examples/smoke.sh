#!/usr/bin/env bash
#
# Walks the two-service lifecycle over HTTP and asserts the result at each step.
#
# This exists so the compose path is exercised rather than merely documented.
# The example config in this repo rotted once already because nothing read it;
# a second runnable path that nothing runs would rot the same way.
#
# Assumes the stack is already up:
#     just examples demo   # brings it up and runs this
#     examples/smoke.sh    # against a stack you started yourself
#
# Override the endpoints when running against something other than the compose
# defaults:
#     ORDER_URL=http://127.0.0.1:3001 PRODUCT_URL=http://127.0.0.1:3002

set -euo pipefail

ORDER_URL="${ORDER_URL:-http://127.0.0.1:3001}"
PRODUCT_URL="${PRODUCT_URL:-http://127.0.0.1:3002}"

# Convergence is two hops through a broker, and the first one also pays for
# topic auto-creation and the consumer group's first assignment. The in-repo
# integration test allows the same 90s for exactly this reason.
DEADLINE_SECONDS="${DEADLINE_SECONDS:-90}"
POLL_SECONDS=0.1
# A cached view has to be observed unchanged this many consecutive times before
# it is believed. Polling every 100ms, 8 of them is ~0.8s of stability.
#
# This is the part a naive script gets wrong. A two-hop round trip passes
# through an intermediate republish, so reading once after a write can catch a
# stale-but-plausible value and "pass" for the wrong reason.
SETTLE_POLLS=8

readonly ORDER_URL PRODUCT_URL DEADLINE_SECONDS POLL_SECONDS SETTLE_POLLS

pass() { printf '  \033[32m✓\033[0m %s\n' "$1"; }
step() { printf '\n\033[1m%s\033[0m\n' "$1"; }
fail() { printf '\n\033[31m✗ %s\033[0m\n' "$1" >&2; exit 1; }

for tool in curl jq; do
    command -v "$tool" >/dev/null 2>&1 \
        || fail "$tool is required but not installed"
done

HTTP_STATUS=""
HTTP_BODY=""

# Perform a request, splitting status from body so a non-2xx can be inspected
# rather than swallowed. `curl -f` is deliberately not used: this script needs
# to treat 409 as a meaningful answer, not an error.
http() {
    local method="$1" url="$2" data="${3:-}"
    local raw
    if [[ -n "$data" ]]; then
        raw=$(curl -sS -X "$method" "$url" \
                   -H 'content-type: application/json' \
                   -d "$data" -w $'\n%{http_code}') || fail "request failed: $method $url"
    else
        raw=$(curl -sS -X "$method" "$url" -w $'\n%{http_code}') \
            || fail "request failed: $method $url"
    fi
    HTTP_STATUS="${raw##*$'\n'}"
    HTTP_BODY="${raw%$'\n'*}"
}

# Read the product as `order` sees it: from its local cache, with no call to
# `product` anywhere in the path. 409 means nothing has been applied yet, which
# is a stage of convergence rather than a failure.
read_cached() {
    http GET "$ORDER_URL/products/$1"
    case "$HTTP_STATUS" in
        200) printf '%s' "$HTTP_BODY" ;;
        409) printf '' ;;
        *)   fail "unexpected $HTTP_STATUS reading cached product: $HTTP_BODY" ;;
    esac
}

# Block until order's cache holds a view strictly newer than $2 and that view
# has stopped changing. Echoes the settled view.
await_settled() {
    local product_id="$1" after_offset="$2"
    local deadline=$((SECONDS + DEADLINE_SECONDS))
    local candidate="" stable=0 view offset

    while (( SECONDS < deadline )); do
        view=$(read_cached "$product_id")
        if [[ -n "$view" ]]; then
            offset=$(jq -r '.applied_offset' <<<"$view")
            # A rising applied_offset is the only externally visible sign that
            # the row moved, independent of whether a field we look at changed.
            if (( offset > after_offset )); then
                if [[ "$view" == "$candidate" ]]; then
                    stable=$((stable + 1))
                    if (( stable >= SETTLE_POLLS )); then
                        printf '%s' "$view"
                        return 0
                    fi
                else
                    candidate="$view"
                    stable=1
                fi
            fi
        fi
        sleep "$POLL_SECONDS"
    done

    fail "cache did not settle past offset $after_offset within ${DEADLINE_SECONDS}s
     last observed: ${candidate:-<nothing applied>}"
}

expect_available() {
    local view="$1" want="$2" got
    got=$(jq -r '.available' <<<"$view")
    [[ "$got" == "$want" ]] || fail "expected available=$want, got $got
     view: $view"
}

printf '\033[1mkafkaman example smoke test\033[0m\n'
printf '  product %s\n  order   %s\n' "$PRODUCT_URL" "$ORDER_URL"

step '1. product creates a widget — order has never heard of it'
http POST "$PRODUCT_URL/products" \
     '{"name":"widget","price_cents":1250,"on_hand":10}'
[[ "$HTTP_STATUS" == 2* ]] || fail "creating product returned $HTTP_STATUS: $HTTP_BODY"
PRODUCT_ID=$(jq -r '.product_id' <<<"$HTTP_BODY")
[[ "$PRODUCT_ID" != "null" && -n "$PRODUCT_ID" ]] || fail "no product_id in: $HTTP_BODY"
pass "created $PRODUCT_ID"

step '2. it propagates to order, which serves it from its own cache'
VIEW=$(await_settled "$PRODUCT_ID" -1)
expect_available "$VIEW" 10
OFFSET=$(jq -r '.applied_offset' <<<"$VIEW")
pass "order caches it at offset $OFFSET, available=10"

step '3. order admits an order against that cached state alone'
http POST "$ORDER_URL/orders" \
     "{\"product_id\":\"$PRODUCT_ID\",\"quantity\":3}"
[[ "$HTTP_STATUS" == 2* ]] || fail "creating order returned $HTTP_STATUS: $HTTP_BODY"
ORDER_ID=$(jq -r '.order_id' <<<"$HTTP_BODY")
pass "accepted $ORDER_ID"

step '4. a placed order reserves nothing — availability is still 10'
VIEW=$(await_settled "$PRODUCT_ID" "$OFFSET")
expect_available "$VIEW" 10
OFFSET=$(jq -r '.applied_offset' <<<"$VIEW")
pass "availability held at 10 through the round trip"

step '5. fulfil it — product recomputes availability two hops away'
http POST "$ORDER_URL/orders/$ORDER_ID/fulfil"
[[ "$HTTP_STATUS" == 2* ]] || fail "fulfil returned $HTTP_STATUS: $HTTP_BODY"
VIEW=$(await_settled "$PRODUCT_ID" "$OFFSET")
expect_available "$VIEW" 7
OFFSET=$(jq -r '.applied_offset' <<<"$VIEW")
pass "availability is 7, derived by product and propagated back"

step '6. cancel it — the count restores with no compensating write anywhere'
http POST "$ORDER_URL/orders/$ORDER_ID/cancel"
[[ "$HTTP_STATUS" == 2* ]] || fail "cancel returned $HTTP_STATUS: $HTTP_BODY"
VIEW=$(await_settled "$PRODUCT_ID" "$OFFSET")
expect_available "$VIEW" 10
OFFSET=$(jq -r '.applied_offset' <<<"$VIEW")
pass "availability is back to 10"

step '7. discontinue it — order refuses despite availability being 10'
http POST "$PRODUCT_URL/products/$PRODUCT_ID/discontinue"
[[ "$HTTP_STATUS" == 2* ]] || fail "discontinue returned $HTTP_STATUS: $HTTP_BODY"
VIEW=$(await_settled "$PRODUCT_ID" "$OFFSET")
expect_available "$VIEW" 10
http POST "$ORDER_URL/orders" \
     "{\"product_id\":\"$PRODUCT_ID\",\"quantity\":1}"
[[ "$HTTP_STATUS" == "409" ]] \
    || fail "expected 409 for a discontinued product, got $HTTP_STATUS: $HTTP_BODY"
pass "rejected on status, not on stock"

printf '\n\033[32mAll steps passed.\033[0m\n'
