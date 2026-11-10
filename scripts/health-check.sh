#!/usr/bin/env bash
# =============================================================================
# health-check.sh — Operational health probe for GoWhale
# =============================================================================
# Replaces the legacy validate-phase*.sh scripts with a single endpoint-based
# health check. Consumes /health and /metrics to verify operational status.
#
# Usage: ./scripts/health-check.sh [BASE_URL]
#   BASE_URL defaults to http://localhost:3000
# =============================================================================

set -euo pipefail

BASE_URL="${1:-http://localhost:3000}"
HEALTH_URL="${BASE_URL}/health"
METRICS_URL="${BASE_URL}/metrics"

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

HEALTH_OUT="$TMPDIR/health.json"
METRICS_OUT="$TMPDIR/metrics.txt"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
log_fail()  { echo -e "${RED}[FAIL]${NC} $*"; }

# -----------------------------------------------------------------------------
# Phase 1: /health probe
# -----------------------------------------------------------------------------
log_info "Fetching health from $HEALTH_URL ..."
HTTP_CODE=$(curl -s -o "$HEALTH_OUT" -w "%{http_code}" "$HEALTH_URL" 2>/dev/null || true)

if [[ "$HTTP_CODE" != "200" ]]; then
    log_fail "Health endpoint returned HTTP $HTTP_CODE (expected 200)"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    log_warn "jq not found — skipping JSON parsing, performing basic validation"
    log_info "Health endpoint reachable (HTTP 200)"
else
    if ! jq -e .status "$HEALTH_OUT" &> /dev/null; then
        log_fail "Health response missing 'status' field"
        exit 1
    fi

    STATUS=$(jq -r '.status // "unknown"' "$HEALTH_OUT")
    UPTIME=$(jq -r '.uptime_seconds // "N/A"' "$HEALTH_OUT")
    TF_CHECKSUM=$(jq -r '.timeframe_checksum // "N/A"' "$HEALTH_OUT")

    if [[ "$STATUS" == "healthy" ]]; then
        log_info "Status: $STATUS | Uptime: ${UPTIME}s | TimeframeChecksum: ${TF_CHECKSUM}"
    else
        log_warn "Status: $STATUS (non-healthy but endpoint reachable)"
    fi
fi

# -----------------------------------------------------------------------------
# Phase 2: /metrics probe (optional — failure here is non-fatal)
# -----------------------------------------------------------------------------
log_info "Fetching metrics from $METRICS_URL ..."
METRICS_CODE=$(curl -s -o "$METRICS_OUT" -w "%{http_code}" "$METRICS_URL" 2>/dev/null || true)

if [[ "$METRICS_CODE" == "200" ]]; then
    log_info "Metrics endpoint reachable (HTTP 200)"
else
    log_warn "Metrics endpoint returned HTTP $METRICS_CODE (non-fatal)"
fi

log_info "Health check completed successfully"
