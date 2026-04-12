import duckdb
import os
import json
from typing import Optional
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv

load_dotenv('/var/www/vhosts/rm-report-mcp/.env')

# ── AWS — disable EC2 metadata lookup, use explicit credentials ───────────────
os.environ['AWS_EC2_METADATA_DISABLED'] = 'true'

BUCKET = os.environ.get('AWS_BUCKET_NAME', 'andrew-field-data-engineering-training')

# ── DuckDB — single connection, initialised once on startup ───────────────────
_con = None

def get_con() -> duckdb.DuckDBPyConnection:
    global _con
    if _con is None:
        _con = duckdb.connect()
        _con.execute("INSTALL httpfs; LOAD httpfs;")
        _con.execute(f"SET s3_region='{os.environ.get('AWS_DEFAULT_REGION', 'eu-west-2')}';")
        _con.execute(f"SET s3_access_key_id='{os.environ.get('AWS_ACCESS_KEY_ID')}';")
        _con.execute(f"SET s3_secret_access_key='{os.environ.get('AWS_SECRET_ACCESS_KEY')}';")
        _con.execute("SET s3_endpoint='s3.eu-west-2.amazonaws.com';")
        _con.execute("SET s3_use_ssl=true;")
        _con.execute("SET s3_url_style='path';")
    return _con

# ── Path helpers ──────────────────────────────────────────────────────────────

def parquet(table: str) -> str:
    """S3 glob path for a Gold Delta table."""
    env_key = f'DELTA_GOLD_{table.upper()}'
    path = os.environ.get(env_key, f's3://{BUCKET}/property/gold/{table}')
    return path.replace('s3a://', 's3://') + '/**/*.parquet'

def silver() -> str:
    """S3 glob path for the Silver Delta table."""
    path = os.environ.get('DELTA_SILVER',
        f's3://{BUCKET}/property/silver/price_comparisons')
    return path.replace('s3a://', 's3://') + '/**/*.parquet'

# ── Query helper ──────────────────────────────────────────────────────────────

def safe(val: str) -> str:
    """Sanitise string inputs to prevent SQL injection."""
    return val.replace("'", "''")

def query(sql: str) -> str:
    """Execute SQL and return results as JSON string."""
    result = get_con().execute(sql)
    cols = [d[0] for d in result.description]
    rows = result.fetchall()
    return json.dumps(
        [dict(zip(cols, row)) for row in rows],
        default=str
    )

# ── MCP server ────────────────────────────────────────────────────────────────

mcp = FastMCP("property-analytics")

# ── Tool 1: quarterly cube ────────────────────────────────────────────────────

@mcp.tool()
def get_quarterly_cube(
    area: Optional[str] = None,
    property_type: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
) -> str:
    """
    Quarterly aggregated sold price data by area and property type.
    Splits flat prices into newer builds (lowrise/midrise/highrise) vs
    older builds (conversion/mansion) — use this to detect cladding
    regulatory impact on flat prices.

    Key regulatory dates:
    - Grenfell fire:          Q2 2017
    - EWS1 introduced:        Q2 2019
    - EWS1 extended <18m:     Q1 2020
    - Building Safety Act:    Q2 2022

    Columns: area, type, year, quarter, sales, avg_sold,
             avg_sold_newer, avg_sold_older, newer_vol, older_vol

    Building classes:
    - newer (cladding risk):  lowrise + midrise + highrise
    - older (low risk):       conversion + mansion

    Args:
        area: Postcode prefix e.g. 'N16', 'E5', 'SW1A'
        property_type: 'Flat', 'Terraced', 'Semi-Detached', 'Detached'
        year_from: Start year e.g. 2017
        year_to: End year e.g. 2025
    """
    try:
        conditions = []
        if area:
            conditions.append(f"area ILIKE '%{safe(area)}%'")
        if property_type:
            conditions.append(f"type = '{safe(property_type)}'")
        if year_from:
            conditions.append(f"year >= {int(year_from)}")
        if year_to:
            conditions.append(f"year <= {int(year_to)}")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        return query(f"""
            SELECT area, type, year, quarter, sales, avg_sold,
                   avg_sold_newer, avg_sold_older, newer_vol, older_vol
            FROM read_parquet('{parquet('quarterly_cube')}')
            {where}
            ORDER BY area, type, year, quarter
        """)
    except Exception as e:
        return json.dumps({"error": str(e)})

# ── Tool 2: postcode analytics ────────────────────────────────────────────────

@mcp.tool()
def get_postcode_analytics(
    postcode_district: Optional[str] = None,
    building_class: Optional[str] = None,
    min_transactions: int = 5,
) -> str:
    """
    Median and average sold prices by postcode district and building class.
    Flats only. Use for comparing prices across areas or building types.

    Building classes: conversion, mansion, lowrise, midrise, highrise, unclassified
    - conversion:  Victorian/Edwardian house splits, lowest cladding risk
    - mansion:     Pre-war purpose-built mansion blocks
    - lowrise:     2-3 storey purpose-built, flat numbers 1-6
    - midrise:     4-6 storey, flat numbers 7-17, named courts
    - highrise:    Tower blocks, 6+ storeys, flat numbers 18+

    Columns: postcode_district, building_class, transaction_count,
             avg_price, median_price, min_price, max_price,
             avg_vs_asking, most_recent_sale,
             profit_count, loss_count, breakeven_count

    Args:
        postcode_district: e.g. 'N16', 'E5', 'SW1A'
        building_class: Filter to one class e.g. 'highrise'
        min_transactions: Minimum sample size (default 5)
    """
    try:
        conditions = [f"transaction_count >= {int(min_transactions)}"]
        if postcode_district:
            conditions.append(f"postcode_district ILIKE '%{safe(postcode_district)}%'")
        if building_class:
            conditions.append(f"building_class = '{safe(building_class)}'")

        return query(f"""
            SELECT postcode_district, building_class, transaction_count,
                   avg_price, median_price, min_price, max_price,
                   avg_vs_asking, most_recent_sale,
                   profit_count, loss_count, breakeven_count
            FROM read_parquet('{parquet('postcode_analytics')}')
            WHERE {' AND '.join(conditions)}
            ORDER BY median_price DESC
        """)
    except Exception as e:
        return json.dumps({"error": str(e)})

# ── Tool 3: street analytics ──────────────────────────────────────────────────

@mcp.tool()
def get_street_analytics(
    street: Optional[str] = None,
    postcode: Optional[str] = None,
    property_type: Optional[str] = None,
) -> str:
    """
    Average and median sold prices aggregated by street.
    Use to compare specific streets or find the most/least expensive
    streets in an area.

    Columns: propertyStreet, propertyAddressPostcode, postcode_district,
             propertyType, transaction_count, avg_price, median_price,
             min_price, max_price, most_recent_sale, earliest_sale

    Args:
        street: Street name e.g. 'Kyverdale Road'
        postcode: Postcode or prefix e.g. 'N16', 'N16 7AH'
        property_type: 'Flat', 'Terraced', 'Semi-Detached', 'Detached'
    """
    try:
        conditions = []
        if street:
            conditions.append(f"propertyStreet ILIKE '%{safe(street)}%'")
        if postcode:
            conditions.append(
                f"(propertyAddressPostcode ILIKE '%{safe(postcode)}%' "
                f"OR postcode_district ILIKE '%{safe(postcode)}%')")
        if property_type:
            conditions.append(f"propertyType = '{safe(property_type)}'")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        return query(f"""
            SELECT propertyStreet, propertyAddressPostcode, postcode_district,
                   propertyType, transaction_count, avg_price, median_price,
                   min_price, max_price, most_recent_sale, earliest_sale
            FROM read_parquet('{parquet('street_analytics')}')
            {where}
            ORDER BY median_price DESC
            LIMIT 100
        """)
    except Exception as e:
        return json.dumps({"error": str(e)})

# ── Tool 4: beds analytics ────────────────────────────────────────────────────

@mcp.tool()
def get_beds_analytics(
    postcode_district: Optional[str] = None,
    num_beds: Optional[int] = None,
    property_type: Optional[str] = None,
    building_class: Optional[str] = None,
) -> str:
    """
    Median and average prices broken down by number of bedrooms.
    Use to answer questions like 'what does a 3-bed flat cost in N16?'
    or 'how much more does an extra bedroom add in value?'

    Columns: postcode_district, propertyType, num_beds, building_class,
             transaction_count, avg_price, median_price,
             min_price, max_price, most_recent_sale

    Args:
        postcode_district: e.g. 'N16', 'E5'
        num_beds: Number of bedrooms e.g. 2, 3, 4
        property_type: 'Flat', 'Terraced', 'Semi-Detached', 'Detached'
        building_class: e.g. 'highrise', 'conversion'
    """
    try:
        conditions = []
        if postcode_district:
            conditions.append(f"postcode_district ILIKE '%{safe(postcode_district)}%'")
        if num_beds is not None:
            conditions.append(f"num_beds = {int(num_beds)}")
        if property_type:
            conditions.append(f"propertyType = '{safe(property_type)}'")
        if building_class:
            conditions.append(f"building_class = '{safe(building_class)}'")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        return query(f"""
            SELECT postcode_district, propertyType, num_beds, building_class,
                   transaction_count, avg_price, median_price,
                   min_price, max_price, most_recent_sale
            FROM read_parquet('{parquet('beds_analytics')}')
            {where}
            ORDER BY postcode_district, num_beds, median_price DESC
            LIMIT 200
        """)
    except Exception as e:
        return json.dumps({"error": str(e)})

# ── Tool 5: listing efficiency ────────────────────────────────────────────────

@mcp.tool()
def get_listing_efficiency(
    postcode_district: Optional[str] = None,
    property_type: Optional[str] = None,
    building_class: Optional[str] = None,
) -> str:
    """
    How quickly properties sell and how close to asking price.
    Use to answer questions like 'which areas sell fastest?' or
    'do highrise flats sell above or below asking price?'

    Columns: postcode_district, propertyType, building_class,
             transaction_count, avg_days_on_market, median_days_on_market,
             avg_price_vs_asking, avg_pct_vs_asking,
             sold_above_asking, sold_below_asking, sold_at_asking

    Args:
        postcode_district: e.g. 'N16', 'E5'
        property_type: 'Flat', 'Terraced', 'Semi-Detached', 'Detached'
        building_class: e.g. 'highrise', 'conversion'
    """
    try:
        conditions = []
        if postcode_district:
            conditions.append(f"postcode_district ILIKE '%{safe(postcode_district)}%'")
        if property_type:
            conditions.append(f"propertyType = '{safe(property_type)}'")
        if building_class:
            conditions.append(f"building_class = '{safe(building_class)}'")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        return query(f"""
            SELECT postcode_district, propertyType, building_class,
                   transaction_count, avg_days_on_market, median_days_on_market,
                   avg_price_vs_asking, avg_pct_vs_asking,
                   sold_above_asking, sold_below_asking, sold_at_asking
            FROM read_parquet('{parquet('listing_efficiency')}')
            {where}
            ORDER BY avg_days_on_market ASC
            LIMIT 200
        """)
    except Exception as e:
        return json.dumps({"error": str(e)})

# ── Tool 6: building class price gap ─────────────────────────────────────────

@mcp.tool()
def get_building_class_gap(
    postcode_district: Optional[str] = None,
    building_class: Optional[str] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
) -> str:
    """
    Quarterly price trends by building class — flats only.
    Use to quantify the price gap between newer builds (cladding risk)
    and older conversions, and how that gap has changed over time.
    Particularly useful for analysing the impact of EWS1 and the
    Building Safety Act on purpose-built flat prices.

    Columns: postcode_district, building_class, year, quarter,
             transaction_count, avg_price, median_price

    Key regulatory dates:
    - EWS1 introduced:     Q2 2019
    - EWS1 extended <18m:  Q1 2020
    - Building Safety Act: Q2 2022

    Args:
        postcode_district: e.g. 'N16', 'E5'
        building_class: e.g. 'highrise', 'conversion'
        year_from: Start year e.g. 2017
        year_to: End year e.g. 2025
    """
    try:
        conditions = []
        if postcode_district:
            conditions.append(f"postcode_district ILIKE '%{safe(postcode_district)}%'")
        if building_class:
            conditions.append(f"building_class = '{safe(building_class)}'")
        if year_from:
            conditions.append(f"year >= {int(year_from)}")
        if year_to:
            conditions.append(f"year <= {int(year_to)}")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        return query(f"""
            SELECT postcode_district, building_class, year, quarter,
                   transaction_count, avg_price, median_price
            FROM read_parquet('{parquet('building_class_gap')}')
            {where}
            ORDER BY postcode_district, building_class, year, quarter
        """)
    except Exception as e:
        return json.dumps({"error": str(e)})

# ── Tool 7: seasonal patterns ─────────────────────────────────────────────────

@mcp.tool()
def get_seasonal_patterns(
    postcode_district: Optional[str] = None,
    property_type: Optional[str] = None,
) -> str:
    """
    Average prices and transaction volumes by quarter across all years.
    Use to identify whether Q2 consistently outperforms Q4, or whether
    certain property types show stronger seasonal variation than others.

    Quarter: 1=Jan-Mar, 2=Apr-Jun, 3=Jul-Sep, 4=Oct-Dec

    Columns: postcode_district, propertyType, quarter,
             transaction_count, avg_price, median_price

    Args:
        postcode_district: e.g. 'N16', 'E5'
        property_type: 'Flat', 'Terraced', 'Semi-Detached', 'Detached'
    """
    try:
        conditions = []
        if postcode_district:
            conditions.append(f"postcode_district ILIKE '%{safe(postcode_district)}%'")
        if property_type:
            conditions.append(f"propertyType = '{safe(property_type)}'")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        return query(f"""
            SELECT postcode_district, propertyType, quarter,
                   transaction_count, avg_price, median_price
            FROM read_parquet('{parquet('seasonal_patterns')}')
            {where}
            ORDER BY postcode_district, propertyType, quarter
        """)
    except Exception as e:
        return json.dumps({"error": str(e)})

# ── Tool 8: cladding freeze detector ─────────────────────────────────────────

@mcp.tool()
def get_cladding_freeze(
    postcode_district: Optional[str] = None,
    building_class: Optional[str] = None,
    street: Optional[str] = None,
) -> str:
    """
    Identifies blocks where sales activity dropped or stopped after
    key building safety regulatory events. Use to quantify cladding-related
    market illiquidity and identify specific frozen blocks.

    Regulatory periods:
    - pre_ewsi:        before April 2019
    - ewsi_introduced: April 2019 - December 2019
    - ewsi_extended:   January 2020 - March 2022
    - post_bsa:        April 2022 onwards (Building Safety Act)

    A block with zero post_bsa transactions but active pre_ewsi is
    likely cladding-frozen.

    Columns: propertyStreet, propertyAddressPostcode, postcode_district,
             building_class, regulatory_period, transaction_count,
             avg_price, most_recent_sale, earliest_sale

    Args:
        postcode_district: e.g. 'N16', 'E5'
        building_class: 'highrise' or 'midrise'
        street: Street or block name e.g. 'Elder Court'
    """
    try:
        conditions = []
        if postcode_district:
            conditions.append(f"postcode_district ILIKE '%{safe(postcode_district)}%'")
        if building_class:
            conditions.append(f"building_class = '{safe(building_class)}'")
        if street:
            conditions.append(f"propertyStreet ILIKE '%{safe(street)}%'")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        return query(f"""
            SELECT propertyStreet, propertyAddressPostcode, postcode_district,
                   building_class, regulatory_period, transaction_count,
                   avg_price, most_recent_sale, earliest_sale
            FROM read_parquet('{parquet('cladding_freeze')}')
            {where}
            ORDER BY postcode_district, propertyStreet, regulatory_period
        """)
    except Exception as e:
        return json.dumps({"error": str(e)})

# ── Tool 9: individual sold properties ───────────────────────────────────────

@mcp.tool()
def get_sold_properties(
    postcode: Optional[str] = None,
    street: Optional[str] = None,
    property_type: Optional[str] = None,
    min_price: Optional[int] = None,
    max_price: Optional[int] = None,
    year_from: Optional[int] = None,
    year_to: Optional[int] = None,
    limit: int = 50,
) -> str:
    """
    Individual sold property records from the Silver layer.
    Use ONLY when the question requires specific property-level detail
    e.g. 'what did 30 Kyverdale Road sell for?' or 'show me all sales
    on this street'. For any aggregation or comparison use the
    analytical tools instead.

    Columns: propertyAddress, propertyAddressPostcode, propertyType,
             soldPrice_num, listingPrice_num, priceDifference_num,
             priceDifferencePercent_num, positiveOrNegativePriceDifference,
             soldDate_ts, listingDate_ts, num_beds, building_class,
             soldListingUrl

    Args:
        postcode: Full or partial postcode e.g. 'N16 7AH' or 'N16'
        street: Street name e.g. 'Kyverdale Road'
        property_type: 'Flat', 'Terraced', 'Semi-Detached', 'Detached'
        min_price: Minimum sold price
        max_price: Maximum sold price
        year_from: Start year
        year_to: End year
        limit: Max records (default 50, max 200)
    """
    try:
        conditions = []
        if postcode:
            conditions.append(
                f"(propertyAddressPostcode ILIKE '%{safe(postcode)}%' "
                f"OR searchPostcode ILIKE '%{safe(postcode)}%')")
        if street:
            conditions.append(f"propertyStreet ILIKE '%{safe(street)}%'")
        if property_type:
            conditions.append(f"propertyType = '{safe(property_type)}'")
        if min_price:
            conditions.append(f"soldPrice_num >= {int(min_price)}")
        if max_price:
            conditions.append(f"soldPrice_num <= {int(max_price)}")
        if year_from:
            conditions.append(f"YEAR(soldDate_ts) >= {int(year_from)}")
        if year_to:
            conditions.append(f"YEAR(soldDate_ts) <= {int(year_to)}")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        limit = min(int(limit), 200)

        return query(f"""
            SELECT propertyAddress, propertyAddressPostcode, propertyType,
                   soldPrice_num, listingPrice_num, priceDifference_num,
                   priceDifferencePercent_num, positiveOrNegativePriceDifference,
                   soldDate_ts, listingDate_ts, num_beds, building_class,
                   soldListingUrl
            FROM read_parquet('{silver()}')
            {where}
            ORDER BY soldDate_ts DESC
            LIMIT {limit}
        """)
    except Exception as e:
        return json.dumps({"error": str(e)})


def main():
    mcp.run(transport="stdio")

if __name__ == "__main__":
    main()