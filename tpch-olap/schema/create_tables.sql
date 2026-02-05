-- TPC-H Schema for MySQL
-- Based on TPC-H Specification v3.0.1
-- ENGINE placeholder will be replaced with InnoDB or ROCKSDB

-- Region table (5 rows)
CREATE TABLE region (
    r_regionkey INTEGER NOT NULL,
    r_name CHAR(25) NOT NULL,
    r_comment VARCHAR(152),
    PRIMARY KEY (r_regionkey)
) ENGINE=_ENGINE_;

-- Nation table (25 rows)
CREATE TABLE nation (
    n_nationkey INTEGER NOT NULL,
    n_name CHAR(25) NOT NULL,
    n_regionkey INTEGER NOT NULL,
    n_comment VARCHAR(152),
    PRIMARY KEY (n_nationkey)
) ENGINE=_ENGINE_;

-- Supplier table (~10K * SF rows)
CREATE TABLE supplier (
    s_suppkey INTEGER NOT NULL,
    s_name CHAR(25) NOT NULL,
    s_address VARCHAR(40) NOT NULL,
    s_nationkey INTEGER NOT NULL,
    s_phone CHAR(15) NOT NULL,
    s_acctbal DECIMAL(15,2) NOT NULL,
    s_comment VARCHAR(101) NOT NULL,
    PRIMARY KEY (s_suppkey)
) ENGINE=_ENGINE_;

-- Part table (~200K * SF rows)
CREATE TABLE part (
    p_partkey INTEGER NOT NULL,
    p_name VARCHAR(55) NOT NULL,
    p_mfgr CHAR(25) NOT NULL,
    p_brand CHAR(10) NOT NULL,
    p_type VARCHAR(25) NOT NULL,
    p_size INTEGER NOT NULL,
    p_container CHAR(10) NOT NULL,
    p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment VARCHAR(23) NOT NULL,
    PRIMARY KEY (p_partkey)
) ENGINE=_ENGINE_;

-- Part-Supplier table (~800K * SF rows)
CREATE TABLE partsupp (
    ps_partkey INTEGER NOT NULL,
    ps_suppkey INTEGER NOT NULL,
    ps_availqty INTEGER NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL,
    ps_comment VARCHAR(199) NOT NULL,
    PRIMARY KEY (ps_partkey, ps_suppkey)
) ENGINE=_ENGINE_;

-- Customer table (~150K * SF rows)
CREATE TABLE customer (
    c_custkey INTEGER NOT NULL,
    c_name VARCHAR(25) NOT NULL,
    c_address VARCHAR(40) NOT NULL,
    c_nationkey INTEGER NOT NULL,
    c_phone CHAR(15) NOT NULL,
    c_acctbal DECIMAL(15,2) NOT NULL,
    c_mktsegment CHAR(10) NOT NULL,
    c_comment VARCHAR(117) NOT NULL,
    PRIMARY KEY (c_custkey)
) ENGINE=_ENGINE_;

-- Orders table (~1.5M * SF rows)
CREATE TABLE orders (
    o_orderkey BIGINT NOT NULL,
    o_custkey INTEGER NOT NULL,
    o_orderstatus CHAR(1) NOT NULL,
    o_totalprice DECIMAL(15,2) NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority CHAR(15) NOT NULL,
    o_clerk CHAR(15) NOT NULL,
    o_shippriority INTEGER NOT NULL,
    o_comment VARCHAR(79) NOT NULL,
    PRIMARY KEY (o_orderkey)
) ENGINE=_ENGINE_;

-- Line Item table (~6M * SF rows) - largest table
CREATE TABLE lineitem (
    l_orderkey BIGINT NOT NULL,
    l_partkey INTEGER NOT NULL,
    l_suppkey INTEGER NOT NULL,
    l_linenumber INTEGER NOT NULL,
    l_quantity DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount DECIMAL(15,2) NOT NULL,
    l_tax DECIMAL(15,2) NOT NULL,
    l_returnflag CHAR(1) NOT NULL,
    l_linestatus CHAR(1) NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct CHAR(25) NOT NULL,
    l_shipmode CHAR(10) NOT NULL,
    l_comment VARCHAR(44) NOT NULL,
    PRIMARY KEY (l_orderkey, l_linenumber)
) ENGINE=_ENGINE_;

-- Create indexes for better query performance
CREATE INDEX idx_supplier_nation ON supplier (s_nationkey);
CREATE INDEX idx_partsupp_suppkey ON partsupp (ps_suppkey);
CREATE INDEX idx_customer_nation ON customer (c_nationkey);
CREATE INDEX idx_orders_custkey ON orders (o_custkey);
CREATE INDEX idx_orders_orderdate ON orders (o_orderdate);
CREATE INDEX idx_lineitem_partkey ON lineitem (l_partkey);
CREATE INDEX idx_lineitem_suppkey ON lineitem (l_suppkey);
CREATE INDEX idx_lineitem_shipdate ON lineitem (l_shipdate);
CREATE INDEX idx_lineitem_commitdate ON lineitem (l_commitdate);
CREATE INDEX idx_lineitem_receiptdate ON lineitem (l_receiptdate);
CREATE INDEX idx_lineitem_orderkey ON lineitem (l_orderkey);
CREATE INDEX idx_lineitem_part_supp ON lineitem (l_partkey, l_suppkey);
