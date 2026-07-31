"""Benchmark only the alert-worker latency.

Measures the wall time between the Kafka consumer receiving its first message
and the alert worker finishing to push every alert onto the Valkey enrichment
queue (LLEN ZTF_alerts_enrichment_queue == EXPECTED_ALERTS).

This script writes a focused config (n_enrichment=0, n_filter=0 so the alert
worker is the only stage that drains the packets queue) and then delegates to
tests/throughput/_run_alert_only.sh for the actual run.

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

parser = argparse.ArgumentParser(description="Benchmark only the alert-worker latency.")
parser.add_argument(
    "--n-alert-workers",
    type=int,
    default=20,
    help="Number of alert workers to use for the benchmark.",
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
    help="Per-step timeout in seconds (GPU warmup, consumer first message, "
         "valkey LLEN polling).",
)
parser.add_argument(
    "--apptainer",
    action="store_true",
    help="Run the benchmark in Apptainer (currently the only supported mode).",
)
args = parser.parse_args()

if not args.apptainer:
    raise SystemExit("run_alert_only.py currently only supports --apptainer mode.")

hosts = {"mongo": "localhost", "redis": "localhost", "kafka": "localhost"}
ports = {"mongo": 27018, "redis": 6380, "kafka": 29192}

# Write the config the running BOOM apptainer instance will reload. Forcing
# n_enrichment=0 and n_filter=0 makes the alert worker the only consumer of
# the packets queue and prevents the enrichment queue from being drained, so
# LLEN can be used as the "alert worker is done" signal.
with open(os.path.join(args.boom_repo_dir, "config.yaml"), "r") as f:
    config = yaml.safe_load(f)

config["database"]["host"] = hosts["mongo"]
config["database"]["port"] = ports["mongo"]
config["database"]["name"] = "boom-benchmarking"
config["database"]["password"] = "mongoadminsecret"
config["redis"]["host"] = hosts["redis"]
config["redis"]["port"] = ports["redis"]
config["kafka"]["consumer"]["ztf"]["server"] = f"{hosts['kafka']}:{ports['kafka']}"
# Unique group_id so this iteration does not inherit committed offsets from a
# previous iteration's consumer group.
config["kafka"]["consumer"]["ztf"]["group_id"] = (
    f"alert-only-benchmarking-{time.time_ns()}"
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

# The alert worker runs no ONNX inference (that's the enrichment worker's
# job), so GPU enablement is irrelevant to this measurement and we omit it
# from the logs_dir name.
logs_dir = os.path.join(
    f"{args.boom_repo_dir}/logs",
    f"boom-alert-only-na={args.n_alert_workers}",
)

os.environ["BOOM_REPO_ROOT"] = os.path.abspath(args.boom_repo_dir)
os.environ["BENCHMARK_MONGO_PORT"] = str(ports["mongo"])
os.environ["BENCHMARK_REDIS_PORT"] = str(ports["redis"])
os.environ["BENCHMARK_KAFKA_PORT"] = str(ports["kafka"])
os.environ["TIMEOUT_SECS"] = str(args.timeout)
cmd = [
    "bash",
    os.path.join(args.boom_repo_dir, "tests", "throughput", "_run_alert_only.sh"),
    logs_dir,
]
subprocess.run(cmd, check=True)


def extract_date_from_log(line):
    # Apptainer logs put the UTC timestamp as the first whitespace-separated
    # token, matching _run.sh's `current_datetime` output ("YYYY-MM-DD HH:MM:SS").
    return pd.to_datetime(
        line.split()[0].replace("\x1b[2m", "").replace("\x1b[0m", "")
    )


t_start = None
with open(f"{logs_dir}/consumer.log") as f:
    for line in f:
        if "Consumer received first message, continuing..." in line:
            t_start = extract_date_from_log(line)
            break
if t_start is None:
    raise ValueError("Could not find consumer first-message line in consumer.log")

with open(f"{logs_dir}/alert_worker_end_time.txt") as f:
    # _run_alert_only.sh writes this timestamp with `TZ=utc date`, so the value
    # is UTC but the string carries no tz suffix. Localize it so the subtraction
    # against the tz-aware consumer.log timestamp works.
    t_end = pd.to_datetime(f.read().strip()).tz_localize("UTC")

wall_time_s = (t_end - t_start).total_seconds()
print(f"Alert worker wall time: {wall_time_s:.1f} seconds")

with open(os.path.join(logs_dir, "alert_worker_wall_time.txt"), "w") as f:
    f.write(f"{wall_time_s:.1f}\n")
