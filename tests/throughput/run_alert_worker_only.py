"""Benchmark only the alert-worker wall time (no kafka_consumer running during
the measurement window).

Measures the wall time between the last "alert worker ready" log line
(emitted by each worker right before entering its consume loop, after
mongo/redis client init) and the alert workers finishing to push every alert
onto the Valkey enrichment queue (LLEN ZTF_alerts_enrichment_queue ==
EXPECTED_ALERTS).

The _run_alert_worker_only.sh script handles the per-iteration prefill of the
packets queue (running kafka_consumer with --exit-on-eof, then exiting it),
so this wrapper only needs to write the config and read the wall time at the
end.

This script writes a focused config (n_enrichment=0, n_filter=0 + a fresh
group_id for the prefill consumer) and then delegates to
tests/throughput/_run_alert_worker_only.sh for the actual run.

Prerequisite: bring the BOOM services up once with
    python3 tests/throughput/run.py --apptainer --phase setup
and tear them down at the end with
    python3 tests/throughput/run.py --apptainer --phase teardown

    requires:
        Python 3.13+,
        pyyaml,
        pandas>2
"""
import argparse
import os
import subprocess
import time

import pandas as pd
import yaml

parser = argparse.ArgumentParser(
    description="Benchmark only the alert-worker wall time.",
)
parser.add_argument(
    "--n-alert-workers",
    type=int,
    default=20,
    help="Number of alert workers to use for the benchmark.",
)
parser.add_argument(
    "--max-in-queue",
    type=int,
    default=50000,
    help="--max-in-queue passed to the prefill kafka_consumer. Must be > "
         "EXPECTED_ALERTS (29142) so the prefill never blocks.",
)
parser.add_argument(
    "--boom-repo-dir",
    default=".",
    help="Path to the BOOM repo directory.",
)
parser.add_argument(
    "--timeout",
    type=int,
    default=300,
    help="Per-step timeout in seconds (prefill, worker-ready, valkey LLEN polling).",
)
parser.add_argument(
    "--apptainer",
    action="store_true",
    help="Run the benchmark in Apptainer (currently the only supported mode).",
)
args = parser.parse_args()

if not args.apptainer:
    raise SystemExit("run_alert_worker_only.py currently only supports --apptainer mode.")

hosts = {"mongo": "localhost", "redis": "localhost", "kafka": "localhost"}
ports = {"mongo": 27018, "redis": 6380, "kafka": 29192}

# The alert-worker-only config forces n_enrichment=0 and n_filter=0 so that
#   - the enrichment worker does not drain the enrichment queue (we count its LLEN)
#   - the filter worker does not drain its downstream queue (irrelevant here)
# A fresh group_id is essential: the per-iteration prefill consumer must start
# from the earliest offset on every iteration, regardless of any committed
# offsets from prior runs.
with open(os.path.join(args.boom_repo_dir, "config.yaml"), "r") as f:
    config = yaml.safe_load(f)

config["database"]["host"] = hosts["mongo"]
config["database"]["port"] = ports["mongo"]
config["database"]["name"] = "boom-benchmarking"
config["database"]["password"] = "mongoadminsecret"
config["redis"]["host"] = hosts["redis"]
config["redis"]["port"] = ports["redis"]
config["kafka"]["consumer"]["ztf"]["server"] = f"{hosts['kafka']}:{ports['kafka']}"
config["kafka"]["consumer"]["ztf"]["group_id"] = (
    f"alert-worker-only-benchmarking-{time.time_ns()}"
)
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
config["workers"]["ztf"]["alert"]["n_workers"] = args.n_alert_workers
config["workers"]["ztf"]["enrichment"]["n_workers"] = 0
config["workers"]["ztf"]["filter"]["n_workers"] = 0
with open(
    os.path.join(args.boom_repo_dir, "tests", "throughput", "config.yaml"), "w"
) as f:
    yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

# Alert workers run no ONNX inference, so GPU enablement is irrelevant to
# this measurement; omit it from logs_dir (mirrors run_alert_only.py).
logs_dir = os.path.join(
    f"{args.boom_repo_dir}/logs",
    f"boom-alert-worker-only-na={args.n_alert_workers}",
)

os.environ["BOOM_REPO_ROOT"] = os.path.abspath(args.boom_repo_dir)
os.environ["BENCHMARK_MONGO_PORT"] = str(ports["mongo"])
os.environ["BENCHMARK_REDIS_PORT"] = str(ports["redis"])
os.environ["BENCHMARK_KAFKA_PORT"] = str(ports["kafka"])
os.environ["TIMEOUT_SECS"] = str(args.timeout)
os.environ["BENCHMARK_MAX_IN_QUEUE"] = str(args.max_in_queue)
os.environ["BENCHMARK_N_ALERT"] = str(args.n_alert_workers)
cmd = [
    "bash",
    os.path.join(
        args.boom_repo_dir, "tests", "throughput", "_run_alert_worker_only.sh"
    ),
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
        if "alert worker ready" in line:
            t_start = extract_date_from_log(line)
if t_start is None:
    raise ValueError("Could not find 'alert worker ready' line in scheduler.log")

with open(f"{logs_dir}/alert_worker_end_time.txt") as f:
    t_end = pd.to_datetime(f.read().strip()).tz_localize("UTC")

wall_time_s = (t_end - t_start).total_seconds()
print(f"Alert worker wall time: {wall_time_s:.1f} seconds")

with open(os.path.join(logs_dir, "alert_worker_wall_time.txt"), "w") as f:
    f.write(f"{wall_time_s:.1f}\n")
