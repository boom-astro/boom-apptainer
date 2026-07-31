"""Benchmark only the enrichment-worker latency.

Measures the wall time between the last "enrichment worker ready" log line
(emitted by each worker right before entering its consume loop, after model
load + redis/mongo init) and the enrichment workers finishing to push every
alert onto the Valkey filter queue (LLEN ZTF_alerts_filter_queue ==
EXPECTED_ALERTS).

This script writes a focused config (n_alert=0, n_filter=0 so only enrichment
workers consume the enrichment queue) and then delegates to
tests/throughput/_run_enrichment_only.sh for the actual run.

Prerequisites:
  1. `python3 tests/throughput/run.py --apptainer --phase setup` to bring up
     services.
  2. `python3 tests/throughput/run_alert_only.py --apptainer ...` to pre-fill
     ZTF_alerts_enrichment_queue + MongoDB.

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

parser = argparse.ArgumentParser(description="Benchmark only the enrichment-worker latency.")
parser.add_argument("--n-enrichment-workers", type=int, default=37)
parser.add_argument("--boom-repo-dir", default=".")
parser.add_argument("--timeout", type=int, default=300)
parser.add_argument("--apptainer", action="store_true",
                    help="Run the benchmark in Apptainer (currently the only supported mode).")
args = parser.parse_args()

if not args.apptainer:
    raise SystemExit("run_enrichment_only.py currently only supports --apptainer mode.")

hosts = {"mongo": "localhost", "redis": "localhost", "kafka": "localhost"}
ports = {"mongo": 27018, "redis": 6380, "kafka": 29192}

# The enrichment-only config forces n_alert=0 and n_filter=0 so that
#   - the alert worker does not drain the input queue (we want it pre-filled)
#   - the filter worker does not drain the output queue (we count its LLEN)
# Only the enrichment workers run.
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
config["workers"]["ztf"]["enrichment"]["n_workers"] = args.n_enrichment_workers
config["workers"]["ztf"]["filter"]["n_workers"] = 0
with open(
    os.path.join(args.boom_repo_dir, "tests", "throughput", "config.yaml"), "w"
) as f:
    yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

if os.environ.get("BOOM_GPU__ENABLED", "false").lower() == "true":
    gpus = len(
        [d for d in os.environ.get("BOOM_GPU__DEVICE_IDS", "0").split(",") if d.strip()]
    )
else:
    gpus = 0

logs_dir = os.path.join(
    f"{args.boom_repo_dir}/logs",
    f"boom-enrichment-only-ne={args.n_enrichment_workers}-gpu={gpus}",
)

os.environ["BOOM_REPO_ROOT"] = os.path.abspath(args.boom_repo_dir)
os.environ["BENCHMARK_MONGO_PORT"] = str(ports["mongo"])
os.environ["BENCHMARK_REDIS_PORT"] = str(ports["redis"])
os.environ["BENCHMARK_KAFKA_PORT"] = str(ports["kafka"])
os.environ["TIMEOUT_SECS"] = str(args.timeout)
os.environ["BENCHMARK_N_ENRICHMENT"] = str(args.n_enrichment_workers)
cmd = [
    "bash",
    os.path.join(args.boom_repo_dir, "tests", "throughput", "_run_enrichment_only.sh"),
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
        if "enrichment worker ready" in line:
            t_start = extract_date_from_log(line)
if t_start is None:
    raise ValueError("Could not find 'enrichment worker ready' line in scheduler.log")

with open(f"{logs_dir}/enrichment_worker_end_time.txt") as f:
    # _run_enrichment_only.sh writes this timestamp with `TZ=utc date`, so the
    # value is UTC but the string carries no tz suffix. Localize it so the
    # subtraction against the tz-aware scheduler.log timestamp works.
    t_end = pd.to_datetime(f.read().strip()).tz_localize("UTC")

wall_time_s = (t_end - t_start).total_seconds()
print(f"Enrichment worker wall time: {wall_time_s:.1f} seconds")

with open(os.path.join(logs_dir, "enrichment_worker_wall_time.txt"), "w") as f:
    f.write(f"{wall_time_s:.1f}\n")
