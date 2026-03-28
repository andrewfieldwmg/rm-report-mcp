from typing import Optional
import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("property-comparison", host="0.0.0.0", port=8080)

API_BASE = "http://127.0.0.1:3000"

ALLOWED_GROUP_BY = [
    "area", "postcode", "property_type", "agent",
    "year", "month", "year_month", "beds"
]

ALLOWED_METRICS = [
    "count", "avg_profit_pct", "avg_sold_price",
    "profit_rate", "avg_price_difference"
]

STRIP_FIELDS = [
    "propertyLog", "propertyImageSrc", "streetviewImageSrc",
    "listingBullets", "listingDescription", "addedToDailyReport",
    "createdAt", "updatedAt"
]


def clean_property(prop: dict) -> dict:
    return {k: v for k, v in prop.items() if k not in STRIP_FIELDS}


async def fetch_properties(postcode: str) -> dict:
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{API_BASE}/price-comparison-all/",
            timeout=30.0
        )
        response.raise_for_status()
        data = response.json()

    if isinstance(data.get("priceComparisonData"), list) and postcode:
        norm = lambda s: s.replace(" ", "").upper() if s else ""
        target = norm(postcode)
        filtered = [
            p for p in data["priceComparisonData"]
            if target in norm(p.get("searchPostcode", ""))
            or target in norm(p.get("propertyAddressPostcode", ""))
        ]
        data["priceComparisonData"] = filtered
        if not filtered:
            data["noResults"] = True

    return data


@mcp.tool()
async def get_sold_properties(postcode: str) -> str:
    """
    Get individual sold property listings for a specific full postcode e.g. N16 6AE.
    Use ONLY for looking up specific properties at a full postcode level.
    For any comparison, aggregation, trend, or cross-area question use query_properties instead.

    Args:
        postcode: Full UK postcode e.g. N16 6AE
    """
    data = await fetch_properties(postcode)

    if not data.get("success") or data.get("noResults"):
        return "No results found for this postcode."

    properties = [clean_property(p) for p in data.get("priceComparisonData", [])]
    return str(properties)


@mcp.tool()
async def query_properties(
    group_by: str,
    metric: str = "count",
    postcode: Optional[str] = None,
    property_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 20,
    order: str = "DESC"
) -> str:
    """
    Dynamic analytical query over the full property dataset.
    Aggregates server-side — never returns raw rows — so use this for ANY of:
    - Comparisons between postcodes or areas e.g. E17 vs E10
    - Filtering by property type e.g. Terraced only
    - Trends over time e.g. sales volume by year
    - Rankings e.g. which agents get best profit margins
    - Any question involving counts, averages, or rates across the dataset

    Prefer this over get_sold_properties for any aggregation or comparison.

    Args:
        group_by: Dimension to aggregate by. Options: area, postcode, property_type, agent, year, month, year_month, beds
        metric: What to measure. Options: count, avg_profit_pct, avg_sold_price, profit_rate, avg_price_difference
        postcode: Optional postcode prefix to filter by e.g. E17, E10, N16, SW1
        property_type: Optional property type filter e.g. Flat, Terraced, Semi-Detached, Detached
        date_from: Optional start date YYYY-MM-DD
        date_to: Optional end date YYYY-MM-DD
        limit: Max results to return (default 20, max 100)
        order: ASC or DESC (default DESC)
    """
    if group_by not in ALLOWED_GROUP_BY:
        return f"Invalid group_by '{group_by}'. Must be one of: {ALLOWED_GROUP_BY}"

    if metric not in ALLOWED_METRICS:
        return f"Invalid metric '{metric}'. Must be one of: {ALLOWED_METRICS}"

    params = {
        "group_by": group_by,
        "metric": metric,
        "limit": min(int(limit), 100),
        "order": order if order in ("ASC", "DESC") else "DESC"
    }

    if postcode:
        params["postcode"] = postcode
    if property_type:
        params["property_type"] = property_type
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{API_BASE}/analytics/",
                params=params,
                timeout=30.0
            )
            response.raise_for_status()
            data = response.json()
    except Exception as e:
        return f"API error: {str(e)}"

    if not data.get("success"):
        return f"Query failed: {data.get('error', 'Unknown error')}"

    results = data.get("results", [])
    if not results:
        return "No results found for this query."

    return str({
        "group_by": group_by,
        "metric": metric,
        "postcode": postcode,
        "property_type": property_type,
        "date_from": date_from,
        "date_to": date_to,
        "row_count": len(results),
        "results": results
    })


@mcp.tool()
async def get_analytics_cube() -> str:
    """
    Returns the full aggregated property dataset as compact CSV.
    Covers all areas, property types and years in the database.
    Use this as the FIRST tool for any analytical, trend, comparison,
    or ranking question across the dataset.

    Columns:
    area, type, year, sales, avg_sold, avg_list, avg_profit_pct,
    profits, losses, conversion, mansion, lowrise, midrise,
    highrise, unclassified, total_flats, avg_sold_newer, avg_sold_older

    Building classes apply to Flat type only:
    - conversion:  Victorian/Edwardian house splits, letter flat IDs, lowest cladding risk
    - mansion:     Pre-war purpose built mansion blocks, low cladding risk
    - lowrise:     2-3 storey purpose built, flat/unit numbers 1-6
    - midrise:     4-6 storey estates and named courts, flat numbers 7-17
    - highrise:    6+ storeys, towers, modern developments, highest cladding risk
    - unclassified: small residual (~1-2%) of unusual address formats

    Older builds (low cladding risk) = conversion + mansion
    Newer blocks (cladding risk)     = lowrise + midrise + highrise

    avg_sold_newer: avg sold price for lowrise+midrise+highrise flats only
    avg_sold_older: avg sold price for conversion+mansion flats only
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{API_BASE}/analytics-cube/",
            timeout=30.0
        )
        response.raise_for_status()
        return response.text


@mcp.tool()
async def get_quarterly_cube() -> str:
    """
    Returns quarterly aggregated property data from 2017 onwards as compact CSV.
    Use this for identifying inflection points, quarterly trends, and
    regulatory impact analysis:
    - Grenfell fire: Jun 2017 (Q2 2017)
    - EWS1 form introduced: Apr 2019 (Q2 2019)
    - EWS1 extended to sub-18m buildings: Jan 2020 (Q1 2020)
    - Building Safety Act: Apr 2022 (Q2 2022)

    Columns:
    area, type, year, quarter, sales, avg_sold,
    avg_sold_newer, avg_sold_older, newer_vol, older_vol

    Building class groupings (flats only):
    - newer_vol / avg_sold_newer: lowrise + midrise + highrise (cladding risk)
    - older_vol / avg_sold_older: conversion + mansion (low cladding risk)

    Quarter: 1=Jan-Mar, 2=Apr-Jun, 3=Jul-Sep, 4=Oct-Dec
    Property types stored as title case: Flat, Terraced, Semi-Detached, Detached
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{API_BASE}/analytics-quarterly/",
            timeout=30.0
        )
        response.raise_for_status()
        return response.text


@mcp.tool()
async def get_all_properties() -> str:
    """
    Returns the full property dataset as compact CSV for NLP analysis.
    Use this for any question requiring individual property level detail.
    Columns: addr, pc, type, ld (listing date), sd (sold date), sp (sold price), lp (listing price)
    Property types: FLAT, TERRACED, SEMI-DETACHED, DETACHED
    WARNING: large payload — prefer get_analytics_cube for aggregation questions.
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{API_BASE}/analytics-raw/",
            timeout=60.0
        )
        response.raise_for_status()
        return response.text

@mcp.tool()
async def get_frozen_blocks() -> str:
    """
    Returns blocks with zero sales since April 2022 (post-Building Safety Act)
    that had at least 3 sales before the freeze and were active after Jan 2017.
    Use this to identify cladding-frozen properties and quantify market illiquidity.

    Columns:
    street, postcode, area, building_class, total_sales,
    pre_grenfell, grenfell_to_ews1, ews1_period, post_bsa,
    first_sale, last_sale, days_frozen, avg_price, peak_price

    Freeze trigger classification:
    - BSA-triggered:      last_sale >= 2022-01, ews1_period > 0 — survived EWS1, froze after BSA
    - EWS1-triggered:     last_sale 2019-2021, ews1_period > 0 — froze during EWS1 period
    - Grenfell-triggered: last_sale 2017-2019, grenfell_to_ews1 > 0 — froze post-Grenfell

    Areas covered: N16, E5, E8, E9, N15, N17
    Building classes: highrise, midrise, lowrise, mansion, conversion, other
    Numeric-only street names excluded (house numbers, not block names).
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{API_BASE}/analytics-frozen-blocks/",
            timeout=30.0
        )
        response.raise_for_status()
        return response.text
        
def main():
    mcp.run(transport="sse")


if __name__ == "__main__":
    main()