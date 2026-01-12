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

```bash
# Run everything for one engine
sudo ./scripts/run-full-benchmark.sh -e vanilla-innodb

# Or just sysbench
sudo ./scripts/run-full-benchmark.sh -e vanilla-innodb -b sysbench

# Manual control
./scripts/mysql-control.sh vanilla-innodb init
./scripts/mysql-control.sh vanilla-innodb start
./sysbench/prepare.sh vanilla-innodb
./sysbench/run.sh vanilla-innodb
```

The full benchmark script resets the SSD before each run (format + remount) to ensure clean state.

## Results

Results go to `results/<benchmark>/<engine>/<timestamp>/`. Compare with:

```bash
./scripts/compare-results.sh sysbench results/sysbench/vanilla-innodb/... results/sysbench/percona-myrocks/...
```

## Directory Layout

```
common/config/     # env.sh, MySQL configs
scripts/           # mysql-control.sh, setup-ssd.sh, etc.
sysbench/          # prepare.sh, run.sh, cleanup.sh
sysbench-tpcc/     # TPC-C via sysbench (Percona's lua scripts)
tpcc/              # Original tpcc-mysql
results/           # Output goes here
```

## Troubleshooting

MySQL won't start? Check permissions (`chown -R mysql:mysql /mnt/nvme/mysql-*`) and look at error logs in the data directory.

MyRocks plugin missing? You need Percona Server with the rocksdb package.

Temperature issues? The scripts wait for SSD to cool down between runs. Disable with `SSD_COOLDOWN_ENABLED=false` in env.sh if you don't care.
