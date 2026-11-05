"""Local L1 adaptation of the original JDBC/Kafka -> Delta -> silver/gold path."""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys

from pyspark.sql import SparkSession, Window, functions as F

from contract import verify_replay, verify_schemas

SCHEMA = "sale_id STRING, district STRING, price LONG, revision INT"
COLS = ("sale_id", "district", "price", "revision")
TOPIC = "synthetic-sales"


def backfill(spark, state, *, seed=False):
    jdbc = f"jdbc:derby:{state / 'fixture-db'};create=true"
    options = {"url": jdbc, "dbtable": "sales_fixture", "driver": "org.apache.derby.jdbc.EmbeddedDriver"}
    if seed:
        historical = spark.createDataFrame([("H1", "E10", 300000, 1), ("H2", "E5", 220000, 1)], SCHEMA)
        historical.write.format("jdbc").options(**options).mode("errorifexists").save()
    snapshot = spark.read.format("jdbc").options(**options).load()
    snapshot.select(*[F.col(name.upper()).alias(name) for name in COLS]).write.format("delta").mode("overwrite").save(str(state / "batch-bronze"))
    return spark.read.format("delta").load(str(state / "batch-bronze")).count()


def send(kafka, endpoint, records):
    lines = "\n".join(json.dumps(dict(zip(COLS, item))) if isinstance(item, tuple) else item for item in records)
    subprocess.run(
        [str(kafka / "bin/kafka-console-producer.sh"), "--bootstrap-server", endpoint, "--topic", TOPIC,
         "--producer-property", "acks=all", "--producer-property", "delivery.timeout.ms=15000"],
        input=lines + "\n", text=True, timeout=30, check=True,
    )


def ingest(spark, state, endpoint, accept):
    parsed = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", endpoint)
              .option("subscribe", TOPIC).option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", 16).load()
              .select("partition", "offset", F.from_json(F.col("value").cast("string"), SCHEMA).alias("sale")))
    valid = F.coalesce(
        F.col("sale.sale_id").rlike(r"^[A-Z0-9]{1,12}$")
        & F.col("sale.district").rlike(r"^[A-Z]{1,2}[0-9]{1,2}$")
        & (F.col("sale.price") > 0) & (F.col("sale.revision") > 0), F.lit(False),
    )
    key = "stream-valid" if accept else "stream-rejected"
    if accept:
        output = parsed.where(valid).select("sale.*", "partition", "offset")
    else:
        output = parsed.where(~valid).select("partition", "offset", F.lit("invalid_sale").alias("reason"))
    query = (output.writeStream.format("delta").outputMode("append")
             .option("checkpointLocation", str(state / "checkpoints" / key))
             .trigger(availableNow=True).start(str(state / key)))
    try:
        if not query.awaitTermination(90):
            raise TimeoutError(f"{key} did not drain within 90 seconds")
        return str(query.id), str(query.runId)
    finally:
        if query.isActive:
            query.stop()


def offsets(spark, state, key):
    return sorted(row.offset for row in spark.read.format("delta").load(str(state / key)).select("offset").collect())


def curate(spark, state):
    batch = spark.read.format("delta").load(str(state / "batch-bronze")).withColumn("offset", F.lit(-1))
    stream = spark.read.format("delta").load(str(state / "stream-valid")).drop("partition")
    latest = Window.partitionBy("sale_id").orderBy(F.desc("revision"), F.desc("offset"))
    silver = batch.unionByName(stream).withColumn("rank", F.row_number().over(latest)).where("rank = 1").select(*COLS)
    silver.write.format("delta").mode("overwrite").save(str(state / "silver"))
    silver = spark.read.format("delta").load(str(state / "silver"))
    gold = silver.groupBy("district").agg(F.count("*").alias("sale_count"), F.sum("price").alias("total_price"))
    gold.write.format("delta").mode("overwrite").save(str(state / "gold"))
    gold = spark.read.format("delta").load(str(state / "gold"))
    return [list(row) for row in silver.orderBy("sale_id").collect()], [list(row) for row in gold.orderBy("district").collect()]


def main(state, kafka, endpoint):
    spark = (SparkSession.builder.appName("LondonProperty-L1-SyntheticReplay")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.shuffle.partitions", "2").config("spark.databricks.delta.snapshotPartitions", "2")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        observed = {"batch_before": backfill(spark, state, seed=True), "batch_after": backfill(spark, state)}
        query_runs = []
        for stage, records in (
            ("first", [("H1", "E10", 310000, 2), ("S1", "E10", 450000, 1), "{malformed"]),
            ("final", [("S1", "E10", 450000, 1), ("S2", "E5", 280000, 1), '{"sale_id":"BAD","price":-1}']),
            ("idle", []),
        ):
            if records:
                send(kafka, endpoint, records)
            query_runs.append([ingest(spark, state, endpoint, accept) for accept in (True, False)])
            observed[f"{stage}_valid"] = offsets(spark, state, "stream-valid")
            observed[f"{stage}_rejected"] = offsets(spark, state, "stream-rejected")
        observed["same_checkpoint_identity"] = (
            query_runs[0][0][0] != query_runs[0][1][0]
            and all(len({pair[sink][0] for pair in query_runs}) == 1
                    and len({pair[sink][1] for pair in query_runs}) == len(query_runs) for sink in (0, 1))
        )
        observed["silver"], observed["gold"] = curate(spark, state)
        verify_replay(observed)
        schemas = {key: [list(field) for field in spark.read.format('delta').load(str(state / key)).dtypes]
                   for key in ('stream-valid', 'stream-rejected', 'silver', 'gold')}
        verify_schemas(schemas)
        hashes = {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                  for name in ("pipeline.py", "contract.py", "run_local.py")}
        evidence = {"status": "passed", "dataset": "synthetic", "spark_version": spark.version,
                    "utc": datetime.now(timezone.utc).isoformat(), "source_sha256": hashes,
                    "query_runs": query_runs, "observed": observed, "schemas": schemas}
        (state / "receipt.json").write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    finally:
        spark.stop()


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3])
