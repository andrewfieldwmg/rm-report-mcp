from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import percentile_approx
import os
from dotenv import load_dotenv

load_dotenv('/var/www/vhosts/rm-report-mcp/.env')

# ── DEBUG ONLY: wipe Gold on every run ───────────────────────────────────────
import boto3
s3 = boto3.resource('s3',
    aws_access_key_id     = os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name           = os.environ.get('AWS_DEFAULT_REGION', 'eu-west-2')
)
bucket = s3.Bucket('andrew-field-data-engineering-training')
bucket.objects.filter(Prefix='property/gold/').delete()
print('DEBUG: Gold cleared')
# ─────────────────────────────────────────────────────────────────────────────

# ── Config ────────────────────────────────────────────────────────────────────

BUCKET         = os.environ.get('AWS_BUCKET_NAME',        'andrew-field-data-engineering-training')
SILVER         = os.environ.get('DELTA_SILVER',           f's3a://{BUCKET}/property/silver/price_comparisons')
GOLD_QUARTERLY = os.environ.get('DELTA_GOLD_QUARTERLY',   f's3a://{BUCKET}/property/gold/quarterly_cube')
GOLD_POSTCODE  = os.environ.get('DELTA_GOLD_POSTCODE',    f's3a://{BUCKET}/property/gold/postcode_analytics')

# ── SparkSession ──────────────────────────────────────────────────────────────

builder = (
    SparkSession.builder
    .appName('rm-gold')
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
print(f'Silver: {SILVER}')
print(f'Gold quarterly: {GOLD_QUARTERLY}')
print(f'Gold postcode:  {GOLD_POSTCODE}')

# ── Read Silver ───────────────────────────────────────────────────────────────

print('Reading Silver...')
df_silver = spark.read.format('delta').load(SILVER)
print(f'Silver rows: {df_silver.count():,}')

# ── Gold table 1: quarterly cube ──────────────────────────────────────────────
# Direct port of getQuarterlyCube() from PriceComparisonController.js
# Groups by area (LEFT 4 of searchPostcode), propertyType, year, quarter
# Splits avg price into newer (lowrise/midrise/highrise) vs older (conversion/mansion)

print('Building quarterly cube...')

df_quarterly = (
    df_silver
    .filter(F.col('soldDate_ts').isNotNull())
    .groupBy(
        F.substring(F.col('searchPostcode'), 1, 4).alias('area'),
        F.col('propertyType').alias('type'),
        F.year('soldDate_ts').alias('year'),
        F.quarter('soldDate_ts').alias('quarter'),
    )
    .agg(
        F.count('*').alias('sales'),
        F.round(F.avg('soldPrice_num'), 0).alias('avg_sold'),

        # newer = purpose-built blocks (lowrise / midrise / highrise)
        F.round(F.avg(F.when(
            F.col('building_class').isin('lowrise', 'midrise', 'highrise'),
            F.col('soldPrice_num')
        )), 0).alias('avg_sold_newer'),

        # older = period buildings (conversion / mansion)
        F.round(F.avg(F.when(
            F.col('building_class').isin('conversion', 'mansion'),
            F.col('soldPrice_num')
        )), 0).alias('avg_sold_older'),

        F.sum(F.when(
            F.col('building_class').isin('lowrise', 'midrise', 'highrise'), 1
        ).otherwise(0)).alias('newer_vol'),

        F.sum(F.when(
            F.col('building_class').isin('conversion', 'mansion'), 1
        ).otherwise(0)).alias('older_vol'),
    )
    .orderBy('area', 'type', 'year', 'quarter')
)

df_quarterly.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .save(GOLD_QUARTERLY)

print(f'Quarterly cube rows: {df_quarterly.count():,}')

# ── Gold table 2: postcode analytics ─────────────────────────────────────────
# Postcode district level summary for MCP queries
# Flats only — building class is only meaningful for flats

print('Building postcode analytics...')

df_postcode = (
    df_silver
    .filter(F.col('soldDate_ts').isNotNull())
    .filter(F.col('propertyType') == 'Flat')
    .groupBy('postcode_district', 'building_class')
    .agg(
        F.count('*').alias('transaction_count'),
        F.round(F.avg('soldPrice_num'), 0).alias('avg_price'),
        F.round(percentile_approx('soldPrice_num', 0.5), 0).alias('median_price'),
        F.round(F.min('soldPrice_num'), 0).alias('min_price'),
        F.round(F.max('soldPrice_num'), 0).alias('max_price'),
        F.round(F.avg('priceDifference_num'), 0).alias('avg_vs_asking'),
        F.max('soldDate_ts').alias('most_recent_sale'),
        # Profit/loss breakdown
        F.sum(F.when(
            F.col('positiveOrNegativePriceDifference') == 'Profit', 1
        ).otherwise(0)).alias('profit_count'),
        F.sum(F.when(
            F.col('positiveOrNegativePriceDifference') == 'Loss', 1
        ).otherwise(0)).alias('loss_count'),
        F.sum(F.when(
            F.col('positiveOrNegativePriceDifference') == 'Breakeven', 1
        ).otherwise(0)).alias('breakeven_count'),
    )
    .filter(F.col('transaction_count') >= 5)
    .withColumn('_computed_at', F.current_timestamp())
)

df_postcode.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .save(GOLD_POSTCODE)

print(f'Postcode analytics rows: {df_postcode.count():,}')


# ── Gold table 3: street-level analytics ─────────────────────────────────────

print('Building street analytics...')

GOLD_STREET = os.environ.get('DELTA_GOLD_STREET',
    f's3a://{BUCKET}/property/gold/street_analytics')

df_street = (
    df_silver
    .filter(F.col('soldDate_ts').isNotNull())
    .filter(F.col('propertyStreet').isNotNull())
    .filter(F.col('propertyAddressPostcode').isNotNull())
    .groupBy('propertyStreet', 'propertyAddressPostcode', 'postcode_district', 'propertyType')
    .agg(
        F.count('*').alias('transaction_count'),
        F.round(F.avg('soldPrice_num'), 0).alias('avg_price'),
        F.round(percentile_approx('soldPrice_num', 0.5), 0).alias('median_price'),
        F.round(F.min('soldPrice_num'), 0).alias('min_price'),
        F.round(F.max('soldPrice_num'), 0).alias('max_price'),
        F.max('soldDate_ts').alias('most_recent_sale'),
        F.min('soldDate_ts').alias('earliest_sale'),
    )
    .filter(F.col('transaction_count') >= 3)
    .withColumn('_computed_at', F.current_timestamp())
)

df_street.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true').save(GOLD_STREET)
print(f'Street analytics rows: {df_street.count():,}')

# ── Gold table 4: beds analytics ──────────────────────────────────────────────

print('Building beds analytics...')

GOLD_BEDS = os.environ.get('DELTA_GOLD_BEDS',
    f's3a://{BUCKET}/property/gold/beds_analytics')

df_beds = (
    df_silver
    .filter(F.col('soldDate_ts').isNotNull())
    .filter(F.col('num_beds').isNotNull())
    .filter(F.col('num_beds') > 0)
    .groupBy('postcode_district', 'propertyType', 'num_beds', 'building_class')
    .agg(
        F.count('*').alias('transaction_count'),
        F.round(F.avg('soldPrice_num'), 0).alias('avg_price'),
        F.round(percentile_approx('soldPrice_num', 0.5), 0).alias('median_price'),
        F.round(F.min('soldPrice_num'), 0).alias('min_price'),
        F.round(F.max('soldPrice_num'), 0).alias('max_price'),
        F.max('soldDate_ts').alias('most_recent_sale'),
    )
    .filter(F.col('transaction_count') >= 3)
    .withColumn('_computed_at', F.current_timestamp())
)

df_beds.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true').save(GOLD_BEDS)
print(f'Beds analytics rows: {df_beds.count():,}')

# ── Gold table 5: listing efficiency ─────────────────────────────────────────
# Only records with valid listing data

print('Building listing efficiency...')

GOLD_LISTING = os.environ.get('DELTA_GOLD_LISTING',
    f's3a://{BUCKET}/property/gold/listing_efficiency')

df_listing = (
    df_silver
    .filter(F.col('soldDate_ts').isNotNull())
    .filter(F.col('listingDate_ts').isNotNull())
    .filter(F.col('listingPrice_num').isNotNull())
    .filter(F.col('listingPrice_num') > 0)
    .filter(F.col('positiveOrNegativePriceDifference') != '')
    .withColumn('days_on_market',
        F.datediff(F.col('soldDate_ts'), F.col('listingDate_ts')))
    .filter(F.col('days_on_market') >= 0)
    .groupBy('postcode_district', 'propertyType', 'building_class')
    .agg(
        F.count('*').alias('transaction_count'),
        F.round(F.avg('days_on_market'), 0).alias('avg_days_on_market'),
        F.round(percentile_approx('days_on_market', 0.5), 0).alias('median_days_on_market'),
        F.round(F.avg('priceDifference_num'), 0).alias('avg_price_vs_asking'),
        F.round(F.avg('priceDifferencePercent_num'), 2).alias('avg_pct_vs_asking'),
        F.sum(F.when(F.col('positiveOrNegativePriceDifference') == 'Profit', 1)
            .otherwise(0)).alias('sold_above_asking'),
        F.sum(F.when(F.col('positiveOrNegativePriceDifference') == 'Loss', 1)
            .otherwise(0)).alias('sold_below_asking'),
        F.sum(F.when(F.col('positiveOrNegativePriceDifference') == 'Breakeven', 1)
            .otherwise(0)).alias('sold_at_asking'),
    )
    .filter(F.col('transaction_count') >= 5)
    .withColumn('_computed_at', F.current_timestamp())
)

df_listing.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true').save(GOLD_LISTING)
print(f'Listing efficiency rows: {df_listing.count():,}')

# ── Gold table 6: building class price gap over time ─────────────────────────

print('Building class price gap over time...')

GOLD_CLASS_GAP = os.environ.get('DELTA_GOLD_CLASS_GAP',
    f's3a://{BUCKET}/property/gold/building_class_gap')

df_class_gap = (
    df_silver
    .filter(F.col('soldDate_ts').isNotNull())
    .filter(F.col('propertyType') == 'Flat')
    .filter(F.col('building_class').isin('lowrise', 'midrise', 'highrise',
                                          'conversion', 'mansion'))
    .groupBy(
        'postcode_district',
        'building_class',
        F.year('soldDate_ts').alias('year'),
        F.quarter('soldDate_ts').alias('quarter'),
    )
    .agg(
        F.count('*').alias('transaction_count'),
        F.round(F.avg('soldPrice_num'), 0).alias('avg_price'),
        F.round(percentile_approx('soldPrice_num', 0.5), 0).alias('median_price'),
    )
    .filter(F.col('transaction_count') >= 3)
    .withColumn('_computed_at', F.current_timestamp())
)

df_class_gap.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true').save(GOLD_CLASS_GAP)
print(f'Building class gap rows: {df_class_gap.count():,}')

# ── Gold table 7: seasonal patterns ──────────────────────────────────────────

print('Building seasonal patterns...')

GOLD_SEASONAL = os.environ.get('DELTA_GOLD_SEASONAL',
    f's3a://{BUCKET}/property/gold/seasonal_patterns')

df_seasonal = (
    df_silver
    .filter(F.col('soldDate_ts').isNotNull())
    .groupBy(
        'postcode_district',
        'propertyType',
        F.quarter('soldDate_ts').alias('quarter'),
    )
    .agg(
        F.count('*').alias('transaction_count'),
        F.round(F.avg('soldPrice_num'), 0).alias('avg_price'),
        F.round(percentile_approx('soldPrice_num', 0.5), 0).alias('median_price'),
    )
    .filter(F.col('transaction_count') >= 5)
    .withColumn('_computed_at', F.current_timestamp())
)

df_seasonal.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true').save(GOLD_SEASONAL)
print(f'Seasonal patterns rows: {df_seasonal.count():,}')

# ── Gold table 8: cladding freeze detector ───────────────────────────────────
# Identifies blocks with zero sales since April 2022 (Building Safety Act)
# Compares pre/post regulatory inflection points:
# EWS1 introduced:  April 2019
# EWS1 extended:    January 2020
# Building Safety Act: April 2022

print('Building cladding freeze detector...')

GOLD_CLADDING = os.environ.get('DELTA_GOLD_CLADDING',
    f's3a://{BUCKET}/property/gold/cladding_freeze')

# Aggregate sales by block (street + postcode) by regulatory period
df_cladding = (
    df_silver
    .filter(F.col('soldDate_ts').isNotNull())
    .filter(F.col('propertyType') == 'Flat')
    .filter(F.col('building_class').isin('highrise', 'midrise'))
    .withColumn('regulatory_period',
        F.when(F.col('soldDate_ts') < '2019-04-01', 'pre_ewsi')
         .when(F.col('soldDate_ts') < '2020-01-01', 'ewsi_introduced')
         .when(F.col('soldDate_ts') < '2022-04-01', 'ewsi_extended')
         .otherwise('post_bsa'))
    .groupBy(
        'propertyStreet',
        'propertyAddressPostcode',
        'postcode_district',
        'building_class',
        'regulatory_period',
    )
    .agg(
        F.count('*').alias('transaction_count'),
        F.round(F.avg('soldPrice_num'), 0).alias('avg_price'),
        F.max('soldDate_ts').alias('most_recent_sale'),
        F.min('soldDate_ts').alias('earliest_sale'),
    )
    .withColumn('_computed_at', F.current_timestamp())
)

df_cladding.write.format('delta').mode('overwrite') \
    .option('overwriteSchema', 'true').save(GOLD_CLADDING)
print(f'Cladding freeze rows: {df_cladding.count():,}')


# ── Verification ──────────────────────────────────────────────────────────────

print('\nQuarterly cube sample (N16 flats, most recent quarters):')
spark.read.format('delta').load(GOLD_QUARTERLY) \
    .filter((F.col('area') == 'N16 ') & (F.col('type') == 'Flat')) \
    .orderBy(F.col('year').desc(), F.col('quarter').desc()) \
    .show(8)

print('\nPostcode analytics sample:')
spark.read.format('delta').load(GOLD_POSTCODE) \
    .orderBy(F.col('median_price').desc()) \
    .show(10)