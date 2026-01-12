# MySQL Installation

Vanilla MySQL and Percona Server can't be installed together (file conflicts). Run benchmarks sequentially.

## Vanilla MySQL 8.4.7

```bash
wget https://dev.mysql.com/get/mysql-apt-config_0.8.29-1_all.deb
sudo dpkg -i mysql-apt-config_0.8.29-1_all.deb
# Select MySQL 8.4 LTS
sudo apt update
sudo apt install -y mysql-server
```

## Percona Server 8.4 (with MyRocks)

```bash
wget https://repo.percona.com/apt/percona-release_latest.generic_all.deb
sudo dpkg -i percona-release_latest.generic_all.deb
sudo apt update
sudo percona-release setup ps84
sudo apt install -y percona-server-server percona-server-rocksdb
```

MyRocks plugin at `/usr/lib/mysql/plugin/ha_rocksdb.so`.

## Switching Between Distributions

```bash
# Remove Vanilla MySQL
sudo systemctl stop mysql
sudo apt remove --purge mysql-community-server mysql-community-client \
    mysql-community-client-plugins mysql-community-common mysql-server
sudo apt autoremove

# Then install Percona
sudo apt install percona-server-server percona-server-rocksdb
```

Or vice versa for Percona -> Vanilla.

## Full Workflow

```bash
# Phase 1: Vanilla MySQL
sudo apt install mysql-server
sudo ./scripts/run-full-benchmark.sh -e vanilla-innodb

# Phase 2: Switch to Percona
sudo systemctl stop mysql
sudo apt remove --purge mysql-community-server mysql-community-client \
    mysql-community-client-plugins mysql-community-common mysql-server
sudo apt autoremove
sudo apt install percona-server-server percona-server-rocksdb

# Phase 3: Percona benchmarks
sudo ./scripts/run-full-benchmark.sh -e percona-innodb
sudo ./scripts/run-full-benchmark.sh -e percona-myrocks
```

Results saved separately per engine in `results/`.
