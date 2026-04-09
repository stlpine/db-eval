-- 4-table equi-join analytical query (AIDE VLDB'23 §6.4)
-- Requires: SET @htap_cutoff = <value>; in the same mysql session before executing.
-- WARNING: Running this file directly without setting @htap_cutoff produces a full table scan.
-- Join columns (k) are non-indexed after sysbench-htap/prepare.sh runs DROP INDEX.
-- This forces full scans with version traversal per row — the MyRocks bottleneck per AIDE paper.
SELECT SUM(t1.k + t2.k + t3.k + t4.k)
FROM sbtest1 t1
  JOIN sbtest2 t2 ON t1.k = t2.k
  JOIN sbtest3 t3 ON t2.k = t3.k
  JOIN sbtest4 t4 ON t3.k = t4.k
WHERE t1.k <= @htap_cutoff;
