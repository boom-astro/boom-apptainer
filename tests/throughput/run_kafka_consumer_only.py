"""Benchmark only the kafka_consumer wall time.

Measures the wall time between the kafka_consumer logging its first message
and LLEN ZTF_alerts_packets_queue == EXPECTED_ALERTS. No scheduler / alert
workers run during this benchmark, so the packets queue strictly accumulates
and isolates the kafka -> valkey transfer cost.

This script writes a focused config (n_alert=0, n_enrichment=0, n_filter=0
since no scheduler runs anyway, plus a fresh consumer group_id so the
consumer always starts from the earliest offset) and then delegates to
tests/throughput/_run_kafka_consumer_only.sh for the actual run.

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

parser = argparse.ArgumentParser(description="Benchmark only the kafka_consumer wall time.")
parser.add_argument(
    "--max-in-queue",
    type=int,
    default=50000,
    help="--max-in-queue passed to kafka_consumer. Must be > EXPECTED_ALERTS "
         "(29142) so the consumer never blocks on a full packets queue "
         "(no alert worker drains it here).",
)
parser.add_argument(
    "--n-processes",
    type=int,
    default=1,
    help="Number of parallel processes kafka_consumer should run (forwarded as "
         "`kafka_consumer --processes N`). Each process gets its own subset of "
         "topic partitions and pushes to ZTF_alerts_packets_queue in parallel.",
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
    help="Per-step timeout in seconds (consumer first message, valkey LLEN polling).",
)
parser.add_argument(
    "--apptainer",
    action="store_true",
    help="Run the benchmark in Apptainer (currently the only supported mode).",
)
parser.add_argument(
    "--batch-size",
    type=int,
    default=None,
    help="Override KAFKA_BATCH_SIZE for this run (forwarded to the consumer "
         "via BOOM_KAFKA_BATCH_SIZE env var). Defaults to the const baked into "
         "the binary.",
)
args = parser.parse_args()

if not args.apptainer:
    raise SystemExit("run_kafka_consumer_only.py currently only supports --apptainer mode.")

hosts = {"mongo": "localhost", "redis": "localhost", "kafka": "localhost"}
ports = {"mongo": 27018, "redis": 6380, "kafka": 29192}

# Write the config the running BOOM apptainer instance will reload. All worker
# counts are forced to 0 because no scheduler runs in this mode; only the
# kafka_consumer process is invoked, and it reads `kafka.consumer.ztf.*`.
with open(os.path.join(args.boom_repo_dir, "config.yaml"), "r") as f:
    config = yaml.safe_load(f)

config["database"]["host"] = hosts["mongo"]
config["database"]["port"] = ports["mongo"]
config["database"]["name"] = "boom-benchmarking"
config["database"]["password"] = "mongoadminsecret"
config["redis"]["host"] = hosts["redis"]
config["redis"]["port"] = ports["redis"]
config["kafka"]["consumer"]["ztf"]["server"] = f"{hosts['kafka']}:{ports['kafka']}"
# Fresh group_id every iteration so the consumer always starts from the
# earliest offset and is never affected by committed offsets from a prior run.
config["kafka"]["consumer"]["ztf"]["group_id"] = (
    f"kafka-consumer-only-benchmarking-{time.time_ns()}"
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
config["workers"]["ztf"]["alert"]["n_workers"] = 0
config["workers"]["ztf"]["enrichment"]["n_workers"] = 0
config["workers"]["ztf"]["filter"]["n_workers"] = 0
with open(
    os.path.join(args.boom_repo_dir, "tests", "throughput", "config.yaml"), "w"
) as f:
    yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

# np=N (kafka_consumer --processes N) is the natural sweep dimension here.
# Tag the logs_dir with the batch size when overridden so a batch-size sweep
# does not overwrite results between iterations.
logs_dir_name = f"boom-kafka-consumer-only-np={args.n_processes}"
if args.batch_size is not None:
    logs_dir_name += f"-bs={args.batch_size}"
logs_dir = os.path.join(f"{args.boom_repo_dir}/logs", logs_dir_name)

os.environ["BOOM_REPO_ROOT"] = os.path.abspath(args.boom_repo_dir)
os.environ["BENCHMARK_MONGO_PORT"] = str(ports["mongo"])
os.environ["BENCHMARK_REDIS_PORT"] = str(ports["redis"])
os.environ["BENCHMARK_KAFKA_PORT"] = str(ports["kafka"])
os.environ["TIMEOUT_SECS"] = str(args.timeout)
os.environ["BENCHMARK_MAX_IN_QUEUE"] = str(args.max_in_queue)
os.environ["BENCHMARK_N_PROCESSES"] = str(args.n_processes)
if args.batch_size is not None:
    os.environ["BENCHMARK_KAFKA_BATCH_SIZE"] = str(args.batch_size)
cmd = [
    "bash",
    os.path.join(
        args.boom_repo_dir, "tests", "throughput", "_run_kafka_consumer_only.sh"
    ),
    logs_dir,
]
subprocess.run(cmd, check=True)


def extract_date_from_log(line):
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

with open(f"{logs_dir}/kafka_consumer_end_time.txt") as f:
    t_end = pd.to_datetime(f.read().strip()).tz_localize("UTC")

wall_time_s = (t_end - t_start).total_seconds()
print(f"Kafka consumer wall time: {wall_time_s:.1f} seconds")

with open(os.path.join(logs_dir, "kafka_consumer_wall_time.txt"), "w") as f:
    f.write(f"{wall_time_s:.1f}\n")
