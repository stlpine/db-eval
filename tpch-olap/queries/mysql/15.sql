-- TPC-H Query 15: Top Supplier
-- Note: MySQL doesn't support CREATE VIEW in the middle of a query,
-- so we use a subquery instead

SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    supplier,
    (
        SELECT
            l_suppkey AS supplier_no,
            SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
        FROM
            lineitem
        WHERE
            l_shipdate >= '1996-01-01'
            AND l_shipdate < DATE_ADD('1996-01-01', INTERVAL 3 MONTH)
        GROUP BY
            l_suppkey
    ) AS revenue0
WHERE
    s_suppkey = supplier_no
    AND total_revenue = (
        SELECT
            MAX(total_revenue)
        FROM
            (
                SELECT
                    l_suppkey AS supplier_no,
                    SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
                FROM
                    lineitem
                WHERE
                    l_shipdate >= '1996-01-01'
                    AND l_shipdate < DATE_ADD('1996-01-01', INTERVAL 3 MONTH)
                GROUP BY
                    l_suppkey
            ) AS revenue1
    )
ORDER BY
    s_suppkey;
