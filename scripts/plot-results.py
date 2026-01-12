#!/usr/bin/env python3
"""
Plot benchmark results for InnoDB vs MyRocks comparison
Requires: matplotlib, pandas
Install: pip3 install matplotlib pandas
"""

import argparse
import pandas as pd
import matplotlib.pyplot as plt
import sys
from pathlib import Path

def plot_sysbench(csv_file, output_dir):
    """Plot sysbench results"""
    df = pd.read_csv(csv_file)

    workloads = df['workload'].unique()

    for workload in workloads:
        workload_df = df[df['workload'] == workload]

        # Plot TPS
        fig, ax = plt.subplots(figsize=(12, 6))

        for engine in ['innodb', 'myrocks']:
            engine_df = workload_df[workload_df['engine'] == engine]
            ax.plot(engine_df['threads'], engine_df['tps'],
                   marker='o', label=engine.upper(), linewidth=2)

        ax.set_xlabel('Threads', fontsize=12)
        ax.set_ylabel('Transactions Per Second (TPS)', fontsize=12)
        ax.set_title(f'Sysbench {workload} - Throughput Comparison', fontsize=14)
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3)
        ax.set_xscale('log', base=2)

        plt.tight_layout()
        plt.savefig(f"{output_dir}/{workload}_tps.png", dpi=300)
        plt.close()

        # Plot Latency
        fig, ax = plt.subplots(figsize=(12, 6))

        for engine in ['innodb', 'myrocks']:
            engine_df = workload_df[workload_df['engine'] == engine]
            ax.plot(engine_df['threads'], engine_df['latency_avg'],
                   marker='o', label=engine.upper(), linewidth=2)

        ax.set_xlabel('Threads', fontsize=12)
        ax.set_ylabel('Average Latency (ms)', fontsize=12)
        ax.set_title(f'Sysbench {workload} - Latency Comparison', fontsize=14)
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3)
        ax.set_xscale('log', base=2)
        ax.set_yscale('log')

        plt.tight_layout()
        plt.savefig(f"{output_dir}/{workload}_latency.png", dpi=300)
        plt.close()

        print(f"Created plots for {workload}")

    # Create summary comparison plot
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    for idx, workload in enumerate(workloads[:3]):  # Plot first 3 workloads
        workload_df = df[df['workload'] == workload]

        for engine in ['innodb', 'myrocks']:
            engine_df = workload_df[workload_df['engine'] == engine]
            axes[idx].plot(engine_df['threads'], engine_df['tps'],
                          marker='o', label=engine.upper(), linewidth=2)

        axes[idx].set_xlabel('Threads', fontsize=10)
        axes[idx].set_ylabel('TPS', fontsize=10)
        axes[idx].set_title(f'{workload}', fontsize=11)
        axes[idx].legend(fontsize=9)
        axes[idx].grid(True, alpha=0.3)
        axes[idx].set_xscale('log', base=2)

    plt.tight_layout()
    plt.savefig(f"{output_dir}/summary_comparison.png", dpi=300)
    plt.close()

    print(f"Created summary comparison plot")

def plot_tpcc(csv_file, output_dir):
    """Plot TPC-C results"""
    df = pd.read_csv(csv_file)

    # Plot TpmC
    fig, ax = plt.subplots(figsize=(12, 6))

    for engine in ['innodb', 'myrocks']:
        engine_df = df[df['engine'] == engine]
        ax.plot(engine_df['threads'], engine_df['tpmC'],
               marker='o', label=engine.upper(), linewidth=2, markersize=8)

    ax.set_xlabel('Threads', fontsize=12)
    ax.set_ylabel('TpmC (Transactions per Minute)', fontsize=12)
    ax.set_title('TPC-C Performance Comparison', fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.set_xscale('log', base=2)

    plt.tight_layout()
    plt.savefig(f"{output_dir}/tpcc_tpmC.png", dpi=300)
    plt.close()

    # Plot speedup
    innodb_df = df[df['engine'] == 'innodb'].set_index('threads')
    myrocks_df = df[df['engine'] == 'myrocks'].set_index('threads')

    speedup = myrocks_df['tpmC'] / innodb_df['tpmC']

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(speedup.index, speedup.values, marker='o', linewidth=2, markersize=8, color='green')
    ax.axhline(y=1.0, color='r', linestyle='--', label='Equal Performance')

    ax.set_xlabel('Threads', fontsize=12)
    ax.set_ylabel('Speedup (MyRocks / InnoDB)', fontsize=12)
    ax.set_title('TPC-C Speedup: MyRocks vs InnoDB', fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.set_xscale('log', base=2)

    plt.tight_layout()
    plt.savefig(f"{output_dir}/tpcc_speedup.png", dpi=300)
    plt.close()

    print(f"Created TPC-C plots")

def plot_sysbench_tpcc(csv_file, output_dir):
    """Plot sysbench-tpcc results (hybrid metrics)"""
    df = pd.read_csv(csv_file)

    # Plot TpmC comparison
    fig, ax = plt.subplots(figsize=(12, 6))

    # Handle both engine naming conventions
    engine_map = {
        'vanilla-innodb': 'Vanilla InnoDB',
        'percona-innodb': 'Percona InnoDB',
        'percona-myrocks': 'Percona MyRocks',
        'innodb': 'InnoDB',
        'myrocks': 'MyRocks'
    }

    for engine in df['engine'].unique():
        engine_df = df[df['engine'] == engine]
        label = engine_map.get(engine, engine.upper())
        ax.plot(engine_df['threads'], engine_df['tpmC'],
               marker='o', label=label, linewidth=2, markersize=8)

    ax.set_xlabel('Threads', fontsize=12)
    ax.set_ylabel('TpmC (Transactions per Minute)', fontsize=12)
    ax.set_title('Sysbench-TPCC TpmC Comparison', fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.set_xscale('log', base=2)

    plt.tight_layout()
    plt.savefig(f"{output_dir}/tpmC_comparison.png", dpi=300)
    plt.close()

    # Plot TPS comparison
    fig, ax = plt.subplots(figsize=(12, 6))

    for engine in df['engine'].unique():
        engine_df = df[df['engine'] == engine]
        label = engine_map.get(engine, engine.upper())
        ax.plot(engine_df['threads'], engine_df['tps'],
               marker='o', label=label, linewidth=2, markersize=8)

    ax.set_xlabel('Threads', fontsize=12)
    ax.set_ylabel('TPS (Transactions per Second)', fontsize=12)
    ax.set_title('Sysbench-TPCC TPS Comparison', fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.set_xscale('log', base=2)

    plt.tight_layout()
    plt.savefig(f"{output_dir}/tps_comparison.png", dpi=300)
    plt.close()

    # Plot Latency comparison
    fig, ax = plt.subplots(figsize=(12, 6))

    for engine in df['engine'].unique():
        engine_df = df[df['engine'] == engine]
        label = engine_map.get(engine, engine.upper())
        ax.plot(engine_df['threads'], engine_df['latency_avg'],
               marker='o', label=label, linewidth=2, markersize=8)

    ax.set_xlabel('Threads', fontsize=12)
    ax.set_ylabel('Average Latency (ms)', fontsize=12)
    ax.set_title('Sysbench-TPCC Latency Comparison', fontsize=14)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    ax.set_xscale('log', base=2)
    ax.set_yscale('log')

    plt.tight_layout()
    plt.savefig(f"{output_dir}/latency_comparison.png", dpi=300)
    plt.close()

    # Plot speedup (if exactly 2 engines for pairwise comparison)
    unique_engines = df['engine'].unique()
    if len(unique_engines) == 2:
        # Identify which is innodb and which is myrocks
        innodb_engine = None
        myrocks_engine = None

        for engine in unique_engines:
            if 'innodb' in engine:
                innodb_engine = engine
            elif 'myrocks' in engine:
                myrocks_engine = engine

        if innodb_engine and myrocks_engine:
            innodb_df = df[df['engine'] == innodb_engine].set_index('threads')
            myrocks_df = df[df['engine'] == myrocks_engine].set_index('threads')

            speedup_tpmC = myrocks_df['tpmC'] / innodb_df['tpmC']
            speedup_tps = myrocks_df['tps'] / innodb_df['tps']

            fig, ax = plt.subplots(figsize=(12, 6))
            ax.plot(speedup_tpmC.index, speedup_tpmC.values,
                   marker='o', linewidth=2, markersize=8, label='TpmC Speedup')
            ax.plot(speedup_tps.index, speedup_tps.values,
                   marker='s', linewidth=2, markersize=8, label='TPS Speedup')
            ax.axhline(y=1.0, color='r', linestyle='--', label='Equal Performance')

            ax.set_xlabel('Threads', fontsize=12)
            ax.set_ylabel('Speedup (MyRocks / InnoDB)', fontsize=12)
            ax.set_title('Sysbench-TPCC Speedup: MyRocks vs InnoDB', fontsize=14)
            ax.legend(fontsize=11)
            ax.grid(True, alpha=0.3)
            ax.set_xscale('log', base=2)

            plt.tight_layout()
            plt.savefig(f"{output_dir}/speedup_comparison.png", dpi=300)
            plt.close()

    print(f"Created sysbench-tpcc plots")

def main():
    parser = argparse.ArgumentParser(description='Plot benchmark results')
    parser.add_argument('benchmark', choices=['sysbench', 'tpcc', 'sysbench-tpcc'],
                       help='Benchmark type')
    parser.add_argument('csv_file', help='Path to merged_results.csv')
    parser.add_argument('-o', '--output', default='.',
                       help='Output directory for plots')

    args = parser.parse_args()

    if not Path(args.csv_file).exists():
        print(f"Error: CSV file not found: {args.csv_file}", file=sys.stderr)
        sys.exit(1)

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Plotting {args.benchmark} results from {args.csv_file}")
    print(f"Output directory: {output_dir}")

    if args.benchmark == 'sysbench':
        plot_sysbench(args.csv_file, output_dir)
    elif args.benchmark == 'tpcc':
        plot_tpcc(args.csv_file, output_dir)
    elif args.benchmark == 'sysbench-tpcc':
        plot_sysbench_tpcc(args.csv_file, output_dir)

    print(f"Plots saved to {output_dir}")

if __name__ == '__main__':
    main()
