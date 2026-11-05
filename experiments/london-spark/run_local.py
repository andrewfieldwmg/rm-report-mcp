"""Start a disposable loopback broker and run the bounded synthetic Spark replay."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parent
JARS = (
    "delta-spark_2.12-3.3.2.jar", "delta-storage-3.3.2.jar",
    "spark-sql-kafka-0-10_2.12-3.5.8.jar", "spark-token-provider-kafka-0-10_2.12-3.5.8.jar",
    "kafka-clients-3.4.1.jar", "commons-pool2-2.11.1.jar",
)


def ports():
    with socket.socket() as listener, socket.socket() as controller:
        for sock in (listener, controller):
            sock.bind(("127.0.0.1", 0))
        return listener.getsockname()[1], controller.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("java", "kafka", "spark", "jars"):
        parser.add_argument(f"--{key}", required=True, type=Path)
    parser.add_argument("--evidence", type=Path, help="Optional new receipt file; never overwritten")
    args = parser.parse_args()
    java, kafka, spark, jars = (getattr(args, key).resolve() for key in ("java", "kafka", "spark", "jars"))
    jar_paths = [jars / name for name in JARS]
    for path in [java / "bin/java", kafka / "bin/kafka-server-start.sh", spark / "bin/spark-submit", *jar_paths]:
        if not path.is_file():
            parser.error(f"Required local dependency missing: {path}")
    if args.evidence and args.evidence.exists():
        parser.error("Evidence file already exists; choose a new path")

    state = Path(tempfile.mkdtemp(prefix="london-spark-l1-", dir="/var/tmp"))
    print(f"Local run files: {state}", flush=True)
    for directory in ("home", "eventlog"):
        (state / directory).mkdir()
    port, controller = ports()
    endpoint = f"127.0.0.1:{port}"
    config = state / "broker.properties"
    config.write_text(f"""process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:{controller}
listeners=PLAINTEXT://{endpoint},CONTROLLER://127.0.0.1:{controller}
advertised.listeners=PLAINTEXT://{endpoint}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs={state / 'broker-data'}
num.partitions=1
auto.create.topics.enable=false
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
""", encoding="utf-8")
    env = {"HOME": str(state / "home"), "PATH": f"{java / 'bin'}:/usr/bin:/bin", "JAVA_HOME": str(java),
           "LANG": "C.UTF-8", "SPARK_LOCAL_IP": "127.0.0.1", "PYSPARK_PYTHON": sys.executable,
           "KAFKA_HEAP_OPTS": "-Xms128m -Xmx384m", "LOG_DIR": str(state)}

    def broker_cmd(name, *options, **kwargs):
        return subprocess.run([str(kafka / "bin" / name), *options], cwd=state, env=env,
                              text=True, timeout=30, check=True, **kwargs)

    cluster = broker_cmd("kafka-storage.sh", "random-uuid", capture_output=True).stdout.strip()
    broker_cmd("kafka-storage.sh", "format", "--standalone", "-t", cluster, "-c", str(config))
    with (state / "kafka.log").open("w", encoding="utf-8") as broker_log:
        broker = subprocess.Popen([str(kafka / "bin/kafka-server-start.sh"), str(config)], cwd=state, env=env,
                                  stdout=broker_log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 35
            while True:
                if broker.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("Broker did not start; see kafka.log")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                        break
                except OSError:
                    time.sleep(0.25)
            broker_cmd("kafka-topics.sh", "--bootstrap-server", endpoint, "--create", "--topic", "synthetic-sales",
                       "--partitions", "1", "--replication-factor", "1")
            with (state / "spark.log").open("w", encoding="utf-8") as log:
                subprocess.run([
                    str(spark / "bin/spark-submit"), "--master", "local[2]", "--driver-memory", "1g",
                    "--conf", "spark.ui.enabled=false", "--conf", "spark.driver.host=127.0.0.1",
                    "--conf", "spark.driver.bindAddress=127.0.0.1", "--conf", "spark.eventLog.enabled=true",
                    "--conf", f"spark.eventLog.dir={state / 'eventlog'}", "--jars", ",".join(map(str, jar_paths)),
                    str(ROOT / "pipeline.py"), str(state), str(kafka), endpoint,
                ], cwd=state, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=240, check=True)
        finally:
            if broker.poll() is None:
                broker.terminate()
                try:
                    broker.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    broker.kill()
                    broker.wait(timeout=5)

    result = json.loads((state / "receipt.json").read_text(encoding="utf-8"))
    result["broker_stopped"] = broker.poll() is not None
    result["java_release"] = subprocess.check_output([str(java / "bin/java"), "-version"], stderr=subprocess.STDOUT,
                                                    text=True, env=env, timeout=15).splitlines()[0]
    result["kafka_release"] = broker_cmd("kafka-topics.sh", "--version", capture_output=True).stdout.strip()
    result["jar_sha256"] = {p.name: hashlib.file_digest(p.open("rb"), "sha256").hexdigest() for p in jar_paths}
    encoded = json.dumps(result, indent=2) + "\n"
    if args.evidence:
        args.evidence.parent.mkdir(parents=True, exist_ok=True)
        with args.evidence.open("x", encoding="utf-8") as evidence:
            evidence.write(encoded)
    print(encoded, end="")


if __name__ == "__main__":
    main()
