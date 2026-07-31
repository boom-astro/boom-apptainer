"""Benchmark only the filter-worker latency.

Measures the wall time between the scheduler logging "starting filter worker"
and the filter workers finishing to produce every (alert x filter) message to
the ZTF_alerts_results Kafka topic.

This script writes a focused config (n_alert=0, n_enrichment=0 so only filter
workers run) and then delegates to tests/throughput/_run_filter_only.sh for
the actual run.

Prerequisites:
  1. `python3 tests/throughput/run.py --apptainer --phase setup` to bring up
     services.
  2. `python3 tests/throughput/run_alert_only.py --apptainer ...` to fill
     enrichment_queue + MongoDB.
  3. `python3 tests/throughput/run_enrichment_only.py --apptainer ...` to
     drain enrichment_queue and fill filter_queue.

    requires:
        Python 3.13+,
        pyyaml,
        pandas>2
"""
import argparse
import os
import subprocess

import pandas as pd
import yaml

parser = argparse.ArgumentParser(description="Benchmark only the filter-worker latency.")
parser.add_argument("--n-filter-workers", type=int, default=15)
parser.add_argument("--boom-repo-dir", default=".")
parser.add_argument("--timeout", type=int, default=300)
parser.add_argument("--apptainer", action="store_true",
                    help="Run the benchmark in Apptainer (currently the only supported mode).")
args = parser.parse_args()

if not args.apptainer:
    raise SystemExit("run_filter_only.py currently only supports --apptainer mode.")

hosts = {"mongo": "localhost", "redis": "localhost", "kafka": "localhost"}
ports = {"mongo": 27018, "redis": 6380, "kafka": 29192}

# The filter-only config forces n_alert=0 and n_enrichment=0 so that the
# filter queue stays in its pre-filled state until the filter workers drain
# it. Only the filter workers run.
with open(os.path.join(args.boom_repo_dir, "config.yaml"), "r") as f:
    config = yaml.safe_load(f)

config["database"]["host"] = hosts["mongo"]
config["database"]["port"] = ports["mongo"]
config["database"]["name"] = "boom-benchmarking"
config["database"]["password"] = "mongoadminsecret"
config["redis"]["host"] = hosts["redis"]
config["redis"]["port"] = ports["redis"]
config["kafka"]["consumer"]["ztf"]["server"] = f"{hosts['kafka']}:{ports['kafka']}"
config["kafka"]["producer"]["server"] = f"{hosts['kafka']}:{ports['kafka']}"
config["api"]["port"] = 4000
config["api"]["auth"]["secret_key"] = "1234"
config["api"]["auth"]["admin_password"] = "adminsecret"
config["cutouts_storage"]["type"] = "mongo"
config["cutouts_storage"]["host"] = hosts["mongo"]
config["cutouts_storage"]["port"] = ports["mongo"]
config["cutouts_storage"]["name"] = "boom-benchmarking"
config["cutouts_storage"]["username"] = "mongoadmin"
config["cutouts_storage"]["password"] = "mongoadminsecret"
config["babamul"]["enabled"] = True
config["workers"]["ztf"]["alert"]["n_workers"] = 0
config["workers"]["ztf"]["enrichment"]["n_workers"] = 0
config["workers"]["ztf"]["filter"]["n_workers"] = args.n_filter_workers
with open(
    os.path.join(args.boom_repo_dir, "tests", "throughput", "config.yaml"), "w"
) as f:
    yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

# Filter workers run no GPU inference, so GPU enablement is irrelevant to
# this measurement and we omit it from the logs_dir name.
logs_dir = os.path.join(
    f"{args.boom_repo_dir}/logs",
    f"boom-filter-only-nf={args.n_filter_workers}",
)

os.environ["BOOM_REPO_ROOT"] = os.path.abspath(args.boom_repo_dir)
os.environ["BENCHMARK_MONGO_PORT"] = str(ports["mongo"])
os.environ["BENCHMARK_REDIS_PORT"] = str(ports["redis"])
os.environ["BENCHMARK_KAFKA_PORT"] = str(ports["kafka"])
os.environ["TIMEOUT_SECS"] = str(args.timeout)
cmd = [
    "bash",
    os.path.join(args.boom_repo_dir, "tests", "throughput", "_run_filter_only.sh"),
    logs_dir,
]
subprocess.run(cmd, check=True)


def extract_date_from_log(line):
    return pd.to_datetime(
        line.split()[0].replace("\x1b[2m", "").replace("\x1b[0m", "")
    )


t_start = None
with open(f"{logs_dir}/scheduler.log") as f:
    for line in f:
        if "starting filter worker" in line:
            t_start = extract_date_from_log(line)
            break
if t_start is None:
    raise ValueError("Could not find 'starting filter worker' line in scheduler.log")

with open(f"{logs_dir}/filter_worker_end_time.txt") as f:
    # _run_filter_only.sh writes this timestamp with `TZ=utc date`, so the
    # value is UTC but the string carries no tz suffix. Localize it so the
    # subtraction against the tz-aware scheduler.log timestamp works.
    t_end = pd.to_datetime(f.read().strip()).tz_localize("UTC")

wall_time_s = (t_end - t_start).total_seconds()
print(f"Filter worker wall time: {wall_time_s:.1f} seconds")

with open(os.path.join(logs_dir, "filter_worker_wall_time.txt"), "w") as f:
    f.write(f"{wall_time_s:.1f}\n")
