from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os
from dotenv import load_dotenv

load_dotenv('/var/www/vhosts/rm-report-mcp/.env')

# ── Config ────────────────────────────────────────────────────────────────────

BUCKET  = os.environ.get('AWS_BUCKET_NAME', 'andrew-field-data-engineering-training')
BRONZE  = os.environ.get('DELTA_BRONZE',    f's3a://{BUCKET}/property/bronze/price_comparisons')
SILVER  = os.environ.get('DELTA_SILVER',    f's3a://{BUCKET}/property/silver/price_comparisons')

# ── DEBUG ONLY: wipe Silver on every run ─────────────────────────────────────
import boto3
s3 = boto3.resource('s3',
    aws_access_key_id     = os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name           = os.environ.get('AWS_DEFAULT_REGION', 'eu-west-2')
)
bucket = s3.Bucket('andrew-field-data-engineering-training')
bucket.objects.filter(Prefix='property/silver/').delete()
print('DEBUG: Silver cleared')

# ─────────────────────────────────────────────────────────────────────────────
# ── SparkSession ──────────────────────────────────────────────────────────────

builder = (
    SparkSession.builder
    .appName('rm-silver')
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
    ]
).getOrCreate()

spark.sparkContext.setLogLevel('WARN')
print(f'Spark {spark.version} ready')
print(f'Bronze: {BRONZE}')
print(f'Silver: {SILVER}')

# ── Read Bronze ───────────────────────────────────────────────────────────────

print('Reading Bronze...')
df_bronze = spark.read.format('delta').load(BRONZE)
print(f'Bronze rows: {df_bronze.count():,}')

# ── Step 1: Cast varchar columns to proper types ──────────────────────────────
# Bronze stores everything as strings — Silver casts to correct types
# Invalid values (e.g. empty strings) become null rather than crashing

print('Casting types...')

df_typed = (
    df_bronze

    # Prices: strip £ and commas, cast to double
    .withColumn('soldPrice_num',
        F.regexp_replace(F.col('soldPrice'), r'[£,\s]', '').cast('double'))
    .withColumn('listingPrice_num',
        F.when(F.col('listingPrice').isNotNull() & (F.col('listingPrice') != ''),
            F.regexp_replace(F.col('listingPrice'), r'[£,\s]', '').cast('double'))
        .otherwise(F.lit(None).cast('double')))
    .withColumn('priceDifference_num',
        F.when(F.col('priceDifference').isNotNull() & (F.col('priceDifference') != ''),
            F.regexp_replace(F.col('priceDifference'), r'[£,\s]', '').cast('double'))
        .otherwise(F.lit(None).cast('double')))
    .withColumn('priceDifferencePercent_num',
        F.when(F.col('priceDifferencePercent').isNotNull() & (F.col('priceDifferencePercent') != ''),
            F.col('priceDifferencePercent').cast('double'))
        .otherwise(F.lit(None).cast('double')))

    # Dates
    .withColumn('soldDate_ts',
        F.try_to_timestamp(F.col('soldDateTimestamp')))
    .withColumn('listingDate_ts',
        F.try_to_timestamp(F.col('listingDate')))

    # Coordinates
    .withColumn('latitude',
        F.when(F.col('propertyLatitude').isNotNull() & (F.col('propertyLatitude') != ''),
            F.col('propertyLatitude').cast('double'))
        .otherwise(F.lit(None).cast('double')))
    .withColumn('longitude',
        F.when(F.col('propertyLongitude').isNotNull() & (F.col('propertyLongitude') != ''),
            F.col('propertyLongitude').cast('double'))
        .otherwise(F.lit(None).cast('double')))

    # Beds
    .withColumn('num_beds',
        F.when(F.col('propertyNumBeds').isNotNull() & (F.col('propertyNumBeds') != ''),
            F.col('propertyNumBeds').cast('int'))
        .otherwise(F.lit(None).cast('int')))

    # Normalise searchPostcode — trim and uppercase before deriving district
    .withColumn('searchPostcode',
        F.upper(F.trim(F.col('searchPostcode'))))
    .withColumn('propertyAddressPostcode',
        F.upper(F.trim(F.col('propertyAddressPostcode'))))
    .withColumn('propertyAddressPostcode2',
        F.upper(F.trim(F.col('propertyAddressPostcode2'))))

    # Postcode district derived from normalised searchPostcode
    .withColumn('postcode_district',
        F.substring(F.col('searchPostcode'), 1, 4))
)

# ── Step 2: Data quality filters ──────────────────────────────────────────────
# Exact port of WHERE clause from getQuarterlyCube in PriceComparisonController.js

print('Applying data quality filters...')
df_filtered = (
    df_typed
    .filter(F.col('soldPrice_num') >= 100000)
    .filter(
        F.col('listingAgent').isNull() 
        | (F.col('listingAgent') != 'Homewise, Worthing'))
    .filter(
        F.datediff(F.col('soldDate_ts'), F.col('listingDate_ts')).isNull()
        | (F.datediff(F.col('soldDate_ts'), F.col('listingDate_ts')) <= 730))
)

print(f'Rows after quality filters: {df_filtered.count():,}')

# ── Step 3: Deduplication ─────────────────────────────────────────────────────
# Key: propertyNumber + propertyStreet + propertyAddressPostcode + soldPrice
# Mirrors the scraper's findOne check exactly
# MySQL records win over Kafka records — more complete data
# Within MySQL records, most recent _ingested_at wins

print('Deduplicating...')

# Source priority: mysql.historical_backfill > kafka.property-scrape-events
# Lower number = higher priority
df_with_priority = df_filtered.withColumn('_source_priority',
    F.when(F.col('_source') == 'mysql.historical_backfill', F.lit(1))
     .otherwise(F.lit(2))
)

# Window: partition by dedup key, order by source priority then ingestion time
dedup_window = Window.partitionBy(
    'propertyNumber',
    'propertyStreet',
    'propertyAddressPostcode',
    'soldPrice'
).orderBy(
    F.col('_source_priority').asc(),
    F.col('_ingested_at').desc()
)

df_deduped = (
    df_with_priority
    .withColumn('_row_num', F.row_number().over(dedup_window))
    .filter(F.col('_row_num') == 1)
    .drop('_row_num', '_source_priority')
)

print(f'Rows after deduplication: {df_deduped.count():,}')

# ── Step 4: Building classification ──────────────────────────────────────────
# Direct port of CASE expression from PriceComparisonController.js getQuarterlyCube()

print('Classifying buildings...')

addr  = F.col('propertyAddress')
ptype = F.col('propertyType')

flat_num = F.regexp_extract(addr, r'^Flat (\d+)', 1).cast('int')
leading_num = F.when(
    addr.rlike(r'^\d+,') & ~addr.rlike(r'^\d+[A-Za-z],'),
    F.regexp_extract(addr, r'^(\d+),', 1).cast('int')
).otherwise(F.lit(None).cast('int'))

df_classified = df_deduped.withColumn('building_class',
    F.when(ptype != 'Flat', F.lit(None))
    .when(ptype == 'Flat', F.when(
        addr.rlike(r'Flat [A-Z][,\s]')
        | addr.rlike(r'^\d+[A-Za-z],')
        | addr.rlike(r'^(Ground|First|Second|Third|Top|Lower|Upper|Basement)')
        | addr.rlike(r'Floor Flat|Garden Flat|Basement Flat|Garden Level|Lower Ground'),
        'conversion')
    .when(addr.rlike(r'Mansions'), 'mansion')
    .when(
        addr.rlike(r'Tower|Point|Heights|Plaza|Apartments|Wharf|Quay|Works|Quarter|Yard')
        | (addr.rlike(r'^Flat \d') & (flat_num >= 18))
        | (leading_num >= 18)
        | addr.rlike(r'[1-9]\d(st|nd|rd|th) [Ff]loor'),
        'highrise')
    .when(
        (addr.rlike(r'^Flat \d') & flat_num.between(7, 17))
        | (leading_num.between(7, 17))
        | addr.rlike(r'Court|Lodge|Gate| House,| House '),
        'midrise')
    .when(
        (addr.rlike(r'^Flat \d') & flat_num.between(1, 6))
        | (leading_num.between(1, 6)),
        'lowrise')
    .otherwise('unclassified'))
)

# Normalise legacy 'Listing Removed' values from pre-enrichment scraper versions
# These are records from before updatePropertyLog was implemented (~pre 2023)
# They have no listing data so treat the same as empty string
df_classified = df_classified.withColumn(
    'positiveOrNegativePriceDifference',
    F.when(
        F.col('positiveOrNegativePriceDifference') == 'Listing Removed',
        F.lit('')
    ).otherwise(F.col('positiveOrNegativePriceDifference'))
)


# ── Step 5: Add processed timestamp and write Silver ─────────────────────────

df_silver = df_classified.withColumn('_processed_at', F.current_timestamp())

print(f'Writing Silver: {df_silver.count():,} rows')

(
    df_silver.write
    .format('delta')
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .save(SILVER)
)

print('Silver write complete.')

# ── Verification ──────────────────────────────────────────────────────────────

df_check = spark.read.format('delta').load(SILVER)
print(f'Silver row count: {df_check.count():,}')

print('\nBuilding class distribution (flats only):')
df_check.filter(F.col('propertyType') == 'Flat') \
    .groupBy('building_class').count() \
    .orderBy(F.col('count').desc()).show()

print('\nSource distribution:')
df_check.groupBy('_source').count().orderBy('_source').show()

print('\nPrice type distribution:')
df_check.groupBy('positiveOrNegativePriceDifference').count() \
    .orderBy(F.col('count').desc()).show()

print('\nDate range:')
df_check.agg(
    F.min('soldDate_ts').alias('earliest_sale'),
    F.max('soldDate_ts').alias('latest_sale')
).show()