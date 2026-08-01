-- 4-table equi-join analytical query (AIDE VLDB'23 §6.4)
-- Requires: SET @htap_cutoff = <value>; in the same mysql session before executing.
-- WARNING: Running this file directly without setting @htap_cutoff produces a full table scan.
-- Join columns (k) are non-indexed after sysbench-htap/prepare.sh runs DROP INDEX.
-- This forces full scans with version traversal per row — the MyRocks bottleneck per AIDE paper.
-- STRAIGHT_JOIN pins join order to the FROM-clause order (t1,t2,t3,t4), removing
-- optimizer plan-choice as a confound between runs/sessions: without it, the
-- optimizer's cost-based join-order decision was observed to flip between runs
-- (driven by RocksDB's own approximate, drifting per-CF cardinality estimates,
-- not just ANALYZE TABLE staleness), producing filesort-based plans on some runs
-- and hash-join-based plans on others -- inflating/deflating elapsed time for
-- reasons unrelated to the offload being measured. See
-- flax_migration_htap_naive_offload_findings_20260801.md's "real confound" section.
-- NOTE: this pins join ORDER only, not join ALGORITHM (hash join vs block nested
-- loop) -- MySQL can still choose either per table pair under STRAIGHT_JOIN. If a
-- future run still shows plan-type flips in flamegraphs, also pin
-- optimizer_switch='block_nested_loop=off' to force hash join deterministically.
SELECT STRAIGHT_JOIN SUM(t1.k + t2.k + t3.k + t4.k)
FROM sbtest1 t1
  JOIN sbtest2 t2 ON t1.k = t2.k
  JOIN sbtest3 t3 ON t2.k = t3.k
  JOIN sbtest4 t4 ON t3.k = t4.k
WHERE t1.k <= @htap_cutoff;
