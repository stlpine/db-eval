#!/bin/bash
# flax-preflight.sh -- checks whether a session can possibly show an offload effect,
# in ~10 minutes rather than after a 2.5-hour HTAP run. Answers four questions, all
# of which have silently been "no" in past sessions:
#
#   1. Are the LLTs holding RocksDB snapshots at all? (START TRANSACTION + SLEEP
#      does not acquire one -- MyRocks takes it on the first table access.)
#   2. What is the on-disk version amplification for sbtest1-4?
#   3. Does the analytical query return an answer, and how fast?
#   4. Does the offload engage, and what fraction does it drop?
#
# Usage: ./scripts/flax-preflight.sh [engine]     (default: percona-myrocks-nvmevirt)
#        Requires an already-running, already-prepared instance.

set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Set this here rather than inheriting whatever the calling shell happens to have, so
# the socket/datadir don't depend on a variable left over from an earlier command.
export FLAX_BAREMETAL_ENV="${FLAX_BAREMETAL_ENV:-${SCRIPT_DIR}/../common/config/env-flax-baremetal.sh}"
source "${SCRIPT_DIR}/../common/config/env.sh"

ENGINE="${1:-percona-myrocks-nvmevirt}"
case "$ENGINE" in
    percona-myrocks)          SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS}" ;;
    percona-myrocks-csd)      SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS_CSD}" ;;
    percona-myrocks-nvmevirt) SOCKET="${MYSQL_SOCKET_PERCONA_MYROCKS_NVMEVIRT}" ;;
    *) echo "unsupported engine: $ENGINE" >&2; exit 1 ;;
esac

Q() { mysql --socket="$SOCKET" "$BENCHMARK_DB" -N --batch "$@" 2>&1; }
hr() { printf '%s\n' "------------------------------------------------------------"; }

if ! mysqladmin --socket="$SOCKET" ping &>/dev/null; then
    echo "mysqld is not reachable on $SOCKET" >&2
    echo "start it with:  sudo -E bash ${SCRIPT_DIR}/mysql-control.sh ${ENGINE} start" >&2
    echo "(the device must be mounted first: mountpoint -q /mnt/nvme)" >&2
    exit 1
fi

echo "FLAX offload preflight, engine=${ENGINE}"
hr

# ── 1. Row counts, and whether the join key is where we think it is ───────────
echo "[1] Analytical table row counts + join-key spread"
for t in sbtest1 sbtest2 sbtest3 sbtest4; do
    read -r rows dk minv maxv <<<"$(Q -e "SELECT COUNT(*), COUNT(DISTINCT k), MIN(k), MAX(k) FROM ${t};" | tr '\t' ' ')"
    echo "    ${t}: rows=${rows} distinct_k=${dk} k_range=[${minv},${maxv}]"
done
echo "    top-5 k multiplicities in sbtest1 (flat = uniform = join stays scan-bound):"
Q -e "SELECT k, COUNT(*) c FROM sbtest1 GROUP BY k ORDER BY c DESC LIMIT 5;" \
    | awk '{printf "      k=%s count=%s\n", $1, $2}'
hr

# ── 2. Are any snapshots actually pinned? ────────────────────────────────────
echo "[2] Long-lived transactions / pinned snapshots"
Q -e "SELECT COUNT(*) FROM information_schema.ROCKSDB_TRX;" \
    | awk '{print "    ROCKSDB_TRX rows (active MyRocks transactions): " $1}'
mysql --socket="$SOCKET" --batch --skip-column-names 2>/dev/null \
    -e "SHOW ENGINE ROCKSDB STATUS;" \
    | tr '\\' '\n' | grep -i "oldest snapshot\|snapshot" | head -5 \
    | sed 's/^/    /' || true
echo "    (no snapshot held => compaction drops old versions => nothing to filter)"
hr

# ── 3. On-disk version amplification for the offload CF ──────────────────────
echo "[3] On-disk version amplification (SST entries per live row)"
live=$(Q -e "SELECT COUNT(*) FROM sbtest1;")
live=$(( ${live:-0} * 4 ))
Q -e "SELECT IFNULL(SUM(f.NUM_ROWS),0), COUNT(*)
      FROM information_schema.ROCKSDB_INDEX_FILE_MAP f
      JOIN information_schema.ROCKSDB_DDL d ON d.INDEX_NUMBER = f.INDEX_NUMBER
      WHERE d.TABLE_SCHEMA='${BENCHMARK_DB}'
        AND d.TABLE_NAME IN ('sbtest1','sbtest2','sbtest3','sbtest4');" \
  | awk -v live="$live" '{
        amp = (live>0 ? $1/live : 0);
        printf "    SST entries=%d across %d files, live rows=%d  -> amplification %.3fx\n", $1, $2, live, amp;
        if (amp < 2.0)
            printf "    VERDICT: TOO LOW. No redundant versions on disk; keys_filtered will be ~0\n             no matter how well the device performs. Do not start a session.\n";
        else
            printf "    VERDICT: OK, there is real version pressure for the filter to remove.\n";
    }'
echo "    per-file entry counts (intra-file duplication is what the filter sees):"
Q -e "SELECT f.SST_NAME, f.NUM_ROWS
      FROM information_schema.ROCKSDB_INDEX_FILE_MAP f
      JOIN information_schema.ROCKSDB_DDL d ON d.INDEX_NUMBER = f.INDEX_NUMBER
      WHERE d.TABLE_SCHEMA='${BENCHMARK_DB}'
        AND d.TABLE_NAME IN ('sbtest1','sbtest2','sbtest3','sbtest4')
      ORDER BY f.NUM_ROWS DESC LIMIT 8;" | sed 's/^/      /'
hr

# ── 4. Does the query actually complete, and does the offload engage? ────────
echo "[4] One timed analytical query (stderr NOT suppressed)"
JOIN4_SQL="${SCRIPT_DIR}/../sysbench-htap/queries/join4.sql"
JOIN4_CONTENT=$(cat "$JOIN4_SQL")
OLAP_EXTRA=""
[ "$ENGINE" = "percona-myrocks-nvmevirt" ] && OLAP_EXTRA="SET SESSION rocksdb_nvmevirt_olap_session=1;"

before=$(mysql --socket="$SOCKET" -N --batch 2>/dev/null \
    -e "SHOW GLOBAL STATUS LIKE 'Rocksdb_nvmevirt_keys%';")
t0=$(date +%s.%N)
out=$(mysql --socket="$SOCKET" "$BENCHMARK_DB" --batch <<SQL
SET SESSION transaction_isolation='REPEATABLE-READ';
SET SESSION max_execution_time=${PREFLIGHT_QUERY_TIMEOUT_MS:-600000};
${OLAP_EXTRA}
SET @htap_cutoff = ${HTAP_JOIN_CUTOFF};
${JOIN4_CONTENT}
SQL
)
rc=$?
t1=$(date +%s.%N)
after=$(mysql --socket="$SOCKET" -N --batch 2>/dev/null \
    -e "SHOW GLOBAL STATUS LIKE 'Rocksdb_nvmevirt_keys%';")

printf "    elapsed: %.1fs   client exit: %d\n" "$(echo "$t1 - $t0" | bc)" "$rc"
if [ "$rc" -eq 0 ] && [ -n "$out" ]; then
    echo "    result: $(echo "$out" | tail -1)"
else
    echo "    QUERY DID NOT RETURN A RESULT. This is the thing to fix first."
    echo "$out" | sed 's/^/      /'
fi

if [ -n "$before" ] && [ -n "$after" ]; then
    _v() { echo "$1" | awk -v k="$2" 'tolower($1)==tolower(k){print $2+0; exit}'; }
    sb=$(_v "$before" rocksdb_nvmevirt_keys_seen);     sa=$(_v "$after" rocksdb_nvmevirt_keys_seen)
    fb=$(_v "$before" rocksdb_nvmevirt_keys_filtered); fa=$(_v "$after" rocksdb_nvmevirt_keys_filtered)
    awk -v s=$(( ${sa:-0} - ${sb:-0} )) -v f=$(( ${fa:-0} - ${fb:-0} )) 'BEGIN{
        printf "    offload: keys_seen=%d keys_filtered=%d (ratio %.4f)\n", s, f, (s>0 ? f/s : 0);
        if (s == 0) print "    offload never engaged; check rocksdb_nvmevirt_enabled and the session opt-in.";
        else if (f/s < 0.05) print "    offload engaged but dropped almost nothing; see step [3].";
    }'
fi
hr
echo "Preflight done. Start a full session only if [3] and [4] both look right."
