# Quick Start

```bash
# 1. Check SSD
./scripts/setup-ssd.sh check

# 2. Mount if needed
sudo ./scripts/setup-ssd.sh mount

# 3. Make scripts executable
chmod +x scripts/*.sh sysbench/*.sh tpcc/*.sh sysbench-tpcc/*.sh

# 4. Edit config if needed
vim common/config/env.sh

# 5. Run sysbench benchmark
sudo ./scripts/run-full-benchmark.sh -e vanilla-innodb -b sysbench
```

Results end up in `results/sysbench/vanilla-innodb/<timestamp>/`.

For manual step-by-step:

```bash
./scripts/mysql-control.sh vanilla-innodb init
./scripts/mysql-control.sh vanilla-innodb start
./sysbench/prepare.sh vanilla-innodb
./sysbench/run.sh vanilla-innodb
./sysbench/cleanup.sh vanilla-innodb
./scripts/mysql-control.sh vanilla-innodb stop
```
