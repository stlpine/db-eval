# MySQL Storage Engine Benchmarks

Benchmarking framework for comparing InnoDB vs MyRocks on OLTP workloads.

## Tested Configurations

- Vanilla MySQL 8.4.7 with InnoDB
- Percona Server 8.4 with InnoDB
- Percona Server 8.4 with MyRocks

## Setup

```bash
# Check your SSD
./scripts/setup-ssd.sh check

# Mount if needed
sudo ./scripts/setup-ssd.sh mount
```

Edit `common/config/env.sh` for your setup (SSD mount point, MySQL password, benchmark params).

Note: Vanilla MySQL and Percona can't be installed at the same time - see MYSQL_INSTALLATION.md.

## Running Benchmarks

### With Memory Limit (Recommended)

Uses cgroup to limit memory, simulating constrained environments:

```bash
# Setup cgroup first (one-time)
sudo ./scripts/setup-cgroup.sh

# Run benchmark with cgroup memory limit
# - Data preparation runs WITHOUT limit (fast)
# - Benchmark runs WITH limit (realistic measurement)
./scripts/run-cgroup.sh -e percona-innodb -b tpcc
./scripts/run-cgroup.sh -e percona-myrocks -b tpcc
```

### Without Memory Limit

```bash
./scripts/run-full.sh -e percona-innodb -b tpcc
```

### Individual Steps

For more control, run each phase separately:

```bash
# Prepare data (resets SSD, loads data)
./scripts/prepare-data.sh -e percona-innodb -b tpcc

# Run benchmark (can run multiple times without re-preparing)
./scripts/run-benchmark.sh -e percona-innodb -b tpcc

# Cleanup when done
./scripts/cleanup-data.sh -e percona-innodb -b tpcc
```

### Benchmark Types

- `sysbench` - OLTP read/write workloads
- `tpcc` - TPC-C via tpcc-mysql
- `sysbench-tpcc` - TPC-C via sysbench (Percona's lua scripts)
- `all` - Run all benchmarks

```bash
./scripts/run-cgroup.sh -e percona-innodb -b sysbench,tpcc
./scripts/run-cgroup.sh -e percona-innodb -b all
```

The scripts reset the SSD before data preparation (format + remount) to ensure clean state. MySQL restarts between benchmark types for fair comparison (cold buffer pool).

## Results

Results go to `results/<benchmark>/<engine>/<timestamp>/`. Compare with:

```bash
./scripts/compare-results.sh sysbench results/sysbench/vanilla-innodb/... results/sysbench/percona-myrocks/...
```

## Directory Layout

```
common/config/     # env.sh, MySQL configs (my-*.cnf)
scripts/
  ├── run-cgroup.sh      # Wrapper: prepare + run with cgroup
  ├── run-full.sh        # Wrapper: prepare + run (no cgroup)
  ├── prepare-data.sh    # Prepare benchmark data
  ├── run-benchmark.sh   # Run benchmark (core)
  ├── cleanup-data.sh    # Cleanup benchmark data
  ├── mysql-control.sh   # MySQL init/start/stop
  ├── setup-ssd.sh       # SSD mount/format/reset
  ├── setup-cgroup.sh    # Setup memory cgroup
  └── compare-results.sh # Compare benchmark results
sysbench/          # Sysbench OLTP benchmark
sysbench-tpcc/     # TPC-C via sysbench (Percona's lua scripts)
tpcc/              # TPC-C via tpcc-mysql
results/           # Benchmark output
```

## Troubleshooting

MySQL won't start? Check permissions (`chown -R mysql:mysql /mnt/nvme/mysql-*`) and look at error logs in the data directory.

MyRocks plugin missing? You need Percona Server with the rocksdb package.

Temperature issues? The scripts wait for SSD to cool down between runs. Disable with `SSD_COOLDOWN_ENABLED=false` in env.sh if you don't care.
