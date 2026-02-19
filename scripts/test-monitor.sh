#!/bin/bash
# Smoke test for scripts/monitor.sh
# Runs monitoring for a few seconds with a synthetic workload,
# then validates that all output files are created and the
# resource summary CSV has the expected structure.
#
# Usage: ./scripts/test-monitor.sh
# Run on the benchmark machine (Ubuntu 22.04 x86_64).

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../common/config/env.sh"
source "${SCRIPT_DIR}/monitor.sh"

TEST_DIR=$(mktemp -d /tmp/monitor-test.XXXXXX)
PREFIX="test"
PASS=0
FAIL=0

cleanup() {
    stop_monitors
    rm -rf "$TEST_DIR"
}
trap cleanup EXIT

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

echo "=== monitor.sh smoke test ==="
echo "Output dir: $TEST_DIR"
echo ""

# --- Test 1: start_monitors creates all 4 output files ---
echo "[1] Starting monitors..."
start_monitors "$TEST_DIR" "$PREFIX"

# Give monitors a moment to initialize and write headers
sleep 1

# Verify PID variable is set and processes are alive
if [ -n "$MONITOR_PIDS" ]; then
    pass "MONITOR_PIDS is set ($MONITOR_PIDS)"
else
    fail "MONITOR_PIDS is empty"
fi

for tool in pidstat iostat mpstat vmstat; do
    if pgrep -x "$tool" >/dev/null 2>&1; then
        pass "$tool process is running"
    else
        fail "$tool process is NOT running"
    fi
done

# --- Test 2: generate some I/O so monitoring has data ---
echo ""
echo "[2] Running synthetic workload (5 seconds)..."
# Light CPU + I/O load: dd to /dev/null + compute
dd if=/dev/urandom of="$TEST_DIR/junk" bs=1M count=50 conv=fsync 2>/dev/null &
DD_PID=$!
# Burn some CPU cycles
awk 'BEGIN { for (i=0; i<5000000; i++) x += sin(i) }' &
AWK_PID=$!
sleep 5
kill $DD_PID $AWK_PID 2>/dev/null || true
wait $DD_PID $AWK_PID 2>/dev/null || true
rm -f "$TEST_DIR/junk"

# --- Test 3: stop_monitors kills all processes ---
echo ""
echo "[3] Stopping monitors..."
stop_monitors

if [ -z "$MONITOR_PIDS" ]; then
    pass "MONITOR_PIDS cleared after stop"
else
    fail "MONITOR_PIDS still set: $MONITOR_PIDS"
fi

# --- Test 4: verify raw output files exist and are non-empty ---
echo ""
echo "[4] Checking raw output files..."
for suffix in pidstat iostat mpstat vmstat; do
    f="${TEST_DIR}/${PREFIX}_${suffix}.txt"
    if [ -s "$f" ]; then
        lines=$(wc -l < "$f")
        pass "${PREFIX}_${suffix}.txt exists (${lines} lines)"
    else
        fail "${PREFIX}_${suffix}.txt missing or empty"
    fi
done

# --- Test 5: generate_resource_summary produces valid CSV ---
echo ""
echo "[5] Generating resource summary..."
generate_resource_summary "$TEST_DIR" "$PREFIX"

SUMMARY="${TEST_DIR}/${PREFIX}_resource_summary.csv"
if [ -s "$SUMMARY" ]; then
    lines=$(wc -l < "$SUMMARY")
    pass "resource_summary.csv exists (${lines} lines)"
else
    fail "resource_summary.csv missing or empty"
fi

# Check CSV header
header=$(head -1 "$SUMMARY")
expected_header="category,metric,avg,min,max,stddev"
if [ "$header" = "$expected_header" ]; then
    pass "CSV header is correct"
else
    fail "CSV header mismatch: got '$header'"
fi

# Check that we have data rows for each category
echo ""
echo "[6] Validating summary content..."
for cat in cpu io mem sys; do
    count=$(grep -c "^${cat}," "$SUMMARY" 2>/dev/null) || count=0
    if [ "$count" -gt 0 ]; then
        pass "category '$cat' has $count metrics"
    else
        fail "category '$cat' has NO metrics"
    fi
done

# Validate specific expected metrics exist
for metric in "cpu,user_pct" "cpu,system_pct" "cpu,iowait_pct" "cpu,idle_pct" \
              "io,read_iops" "io,write_iops" "io,read_kbps" "io,write_kbps" \
              "io,util_pct" "io,queue_depth" "io,read_await_ms" "io,write_await_ms" \
              "mem,free_kb" "mem,cache_kb" "sys,runqueue_avg" "sys,blocked_avg"; do
    if grep -q "^${metric}," "$SUMMARY"; then
        pass "metric '$metric' present"
    else
        fail "metric '$metric' MISSING"
    fi
done

# Check that avg values are numeric and non-negative
echo ""
echo "[7] Validating numeric values..."
bad_rows=$(awk -F, 'NR>1 {
    if ($3 !~ /^-?[0-9]+\.?[0-9]*$/) { print NR": avg="$3; found++ }
}
END { if (!found) print "none" }' "$SUMMARY")
if [ "$bad_rows" = "none" ]; then
    pass "all avg values are numeric"
else
    fail "non-numeric avg values: $bad_rows"
fi

# CPU percentages should sum to ~100
cpu_sum=$(awk -F, '$1=="cpu" { sum += $3 } END { printf "%.0f", sum }' "$SUMMARY")
if [ "$cpu_sum" -ge 90 ] && [ "$cpu_sum" -le 110 ]; then
    pass "CPU percentages sum to ~100 (got $cpu_sum)"
else
    fail "CPU percentages sum to $cpu_sum (expected ~100)"
fi

# --- Print summary ---
echo ""
echo "=== Results ==="
cat "$SUMMARY"
echo ""
echo "=== $PASS passed, $FAIL failed ==="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
