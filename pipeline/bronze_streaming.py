from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import os
import boto3
from dotenv import load_dotenv

load_dotenv('/var/www/vhosts/rm-report-mcp/.env')

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_BROKER   = os.environ.get('KAFKA_BROKER',                'localhost:9092')
KAFKA_TOPIC    = os.environ.get('KAFKA_TOPIC_PROPERTY_EVENTS', 'property-scrape-events')
BUCKET         = os.environ.get('AWS_BUCKET_NAME',             'andrew-field-data-engineering-training')
BRONZE         = os.environ.get('DELTA_BRONZE',                f's3a://{BUCKET}/property/bronze/price_comparisons')
BRONZE_DLQ     = os.environ.get('DELTA_BRONZE_DLQ',            f's3a://{BUCKET}/property/bronze/dead_letter')
CHECKPOINT     = os.environ.get('DELTA_CHECKPOINT',            f's3a://{BUCKET}/property/checkpoints/bronze')
CHECKPOINT_DLQ = os.environ.get('DELTA_CHECKPOINT_DLQ',        f's3a://{BUCKET}/property/checkpoints/bronze_dlq')

# ── DEBUG ONLY: wipe Bronze and checkpoints on every run ─────────────────────
# Remove this block once the pipeline is stable
#s3 = boto3.resource('s3',
    #aws_access_key_id     = os.environ.get('AWS_ACCESS_KEY_ID'),
    #aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY'),
    #region_name           = os.environ.get('AWS_DEFAULT_REGION', 'eu-west-2')
#)
#bucket = s3.Bucket('andrew-field-data-engineering-training')
#bucket.objects.filter(Prefix='property/checkpoints/').delete()
#bucket.objects.filter(Prefix='property/bronze/').delete()
#print('DEBUG: Checkpoint and Bronze cleared')

# ───────────────────────────────────────────────
# ── SparkSession ──────────────────────────────────────────────────────────────

builder = (
    SparkSession.builder
    .appName('rm-bronze-streaming')
    .master('local[*]')
    .config('spark.sql.shuffle.partitions', '8')
    .config('spark.sql.extensions',
            'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog',
            'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config('spark.hadoop.fs.s3a.access.key',    os.environ.get('AWS_ACCESS_KEY_ID', ''))
    .config('spark.hadoop.fs.s3a.secret.key',    os.environ.get('AWS_SECRET_ACCESS_KEY', ''))
    .config('spark.hadoop.fs.s3a.endpoint',      f's3.{os.environ.get("AWS_DEFAULT_REGION", "eu-west-2")}.amazonaws.com')
    .config('spark.hadoop.fs.s3a.path.style.access', 'false')
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=[
        'io.delta:delta-spark_4.1_2.13:4.1.0',
        'org.apache.hadoop:hadoop-aws:3.4.1',
        'com.amazonaws:aws-java-sdk-bundle:1.12.262',
        'org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1',
    ]
).getOrCreate()

spark.sparkContext.setLogLevel('WARN')

key = os.environ.get('AWS_ACCESS_KEY_ID', 'NOT SET')
print(f'Spark {spark.version} ready')
print(f'AWS key: {key[:8]}...' if key != 'NOT SET' else 'WARNING: AWS key not set')
print(f'Broker:  {KAFKA_BROKER}')
print(f'Topic:   {KAFKA_TOPIC}')
print(f'Bronze:  {BRONZE}')
print(f'UI:      http://localhost:4040')

# ── Schema ────────────────────────────────────────────────────────────────────

event_schema = StructType([
    StructField('searchPostcode',                    StringType(), True),
    StructField('propertyType',                      StringType(), True),
    StructField('propertyUrl',                       StringType(), True),
    StructField('propertyId',                        StringType(), True),
    StructField('propertyAddress',                   StringType(), True),
    StructField('propertyAddressPostcode',           StringType(), True),
    StructField('propertyAddressPostcode2',          StringType(), True),
    StructField('listingPrice',                      StringType(), True),
    StructField('listingDate',                       StringType(), True),
    StructField('soldPrice',                         StringType(), True),
    StructField('priceDifference',                   StringType(), True),
    StructField('priceDifferencePercent',            StringType(), True),
    StructField('positiveOrNegativePriceDifference', StringType(), True),
    StructField('soldDate',                          StringType(), True),
    StructField('soldDateTimestamp',                 StringType(), True),
    StructField('propertyNumBeds',                   StringType(), True),
    StructField('propertyLatitude',                  StringType(), True),
    StructField('propertyLongitude',                 StringType(), True),
    StructField('propertyAddressWard',               StringType(), True),
    StructField('propertyNumber',                    StringType(), True),
    StructField('propertyStreet',                    StringType(), True),
    StructField('listingAgent',                      StringType(), True),
    StructField('listingDescription',                StringType(), True),
    StructField('listingBullets',                    StringType(), True),
    StructField('scrapedAt',                         StringType(), False),
])

# ── Read from Kafka ───────────────────────────────────────────────────────────

kafka_stream = (
    spark.readStream
    .format('kafka')
    .option('kafka.bootstrap.servers', KAFKA_BROKER)
    .option('subscribe',               KAFKA_TOPIC)
    .option('startingOffsets',         'earliest')
    .load()
)

# ── Parse and split ───────────────────────────────────────────────────────────

df_parsed = (
    kafka_stream
    .select(
        F.col('value').cast('string').alias('json_val'),
        F.col('timestamp').alias('kafka_ts'),
        F.col('partition').alias('kafka_partition'),
        F.col('offset').alias('kafka_offset'),
    )
    .withColumn('event', F.from_json('json_val', event_schema))
    .select('event.*', 'json_val', 'kafka_ts', 'kafka_partition', 'kafka_offset')
    .withColumn('_ingested_at', F.current_timestamp())
    .withColumn('_source',      F.lit('kafka.property-scrape-events'))
)

df_valid = df_parsed.filter(
    F.col('propertyAddress').isNotNull() | F.col('searchPostcode').isNotNull()
)

df_dlq = df_parsed.filter(
    F.col('propertyAddress').isNull() & F.col('searchPostcode').isNull()
).withColumn('_dlq_reason', F.lit('json_parse_failed'))

# ── Write to Delta Bronze ─────────────────────────────────────────────────────

query_bronze = (
    df_valid.drop('json_val').writeStream
    .format('delta')
    .outputMode('append')
    .option('checkpointLocation', CHECKPOINT)
    .option('mergeSchema',        'true')
    .trigger(processingTime='30 seconds')
    .start(BRONZE)
)

query_dlq = (
    df_dlq.writeStream
    .format('delta')
    .outputMode('append')
    .option('checkpointLocation', CHECKPOINT_DLQ)
    .trigger(processingTime='30 seconds')
    .start(BRONZE_DLQ)
)

print('Streaming jobs started')
print(f'Bronze:     {BRONZE}')
print(f'DLQ:        {BRONZE_DLQ}')
print('Waiting for data... (Ctrl+C to stop)')

spark.streams.awaitAnyTermination()