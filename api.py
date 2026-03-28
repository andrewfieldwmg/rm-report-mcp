from typing import Any, Optional
import re
import httpx
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("property-comparison", host="0.0.0.0", port=8080)

API_BASE = "http://127.0.0.1:3000"

# Fields to strip before returning to Claude
STRIP_FIELDS = [
    "propertyLog",
    "propertyImageSrc", 
    "streetviewImageSrc",
    "listingBullets",
    "listingDescription",
    "addedToDailyReport",
    "createdAt",
    "updatedAt"
]

def clean_property(prop: dict) -> dict:
    return {k: v for k, v in prop.items() if k not in STRIP_FIELDS}

async def fetch_properties(postcode: str) -> dict:
    """
    Fetch price comparison data for a postcode.

    Your Express API exposes `GET /price-comparison-all/` and returns:
      { success: bool, noResults: bool, priceComparisonData: [...] }
    """

    def normalize_pc(value: Any) -> str:
        if value is None:
            return ""
        # Normalize for comparisons: uppercase + strip spaces.
        return re.sub(r"\s+", "", str(value)).upper()

    async with httpx.AsyncClient() as client:
        # The MCP server previously called a non-existent `/properties` route.
        response = await client.get(f"{API_BASE}/price-comparison-all/")
        response.raise_for_status()
        data = response.json()

    # Express returns the full table; filter client-side by postcode so
    # downstream MCP tools return only relevant rows.
    if isinstance(data, dict) and isinstance(data.get("priceComparisonData"), list):
        norm_target = normalize_pc(postcode)
        if norm_target:
            filtered = []
            for prop in data["priceComparisonData"]:
                prop_pc = prop.get("propertyAddressPostcode") or prop.get("searchPostcode")
                norm_prop = normalize_pc(prop_pc)
                # Match like the React "includes" filter: allow partial matches both ways.
                if norm_prop and (norm_target in norm_prop or norm_prop in norm_target):
                    filtered.append(prop)

            data["priceComparisonData"] = filtered
            # Keep the response contract: if nothing matches, mark it as "noResults".
            if data.get("success") and not filtered:
                data["noResults"] = True

    return data

@mcp.tool()
async def get_sold_properties(postcode: str) -> str:
    """Get sold property price comparison data for a postcode.
    
    Args:
        postcode: UK postcode to search e.g. N16 6AE
    """
    data = await fetch_properties(postcode)
    
    if not data.get("success") or data.get("noResults"):
        return "No results found for this postcode."
    
    properties = [clean_property(p) for p in data.get("priceComparisonData", [])]
    return str(properties)

@mcp.tool()
async def get_profit_properties(postcode: str, min_profit_percent: float = 0) -> str:
    """Get properties that sold for a profit above a given percentage threshold.
    
    Args:
        postcode: UK postcode to search
        min_profit_percent: Minimum profit percentage e.g. 10 for 10%
    """
    data = await fetch_properties(postcode)
    
    if not data.get("success") or data.get("noResults"):
        return "No results found for this postcode."
    
    properties = [
        clean_property(p) for p in data.get("priceComparisonData", [])
        if p.get("positiveOrNegativePriceDifference") == "Profit"
        and float(p.get("priceDifferencePercent", 0)) >= min_profit_percent
    ]
    
    if not properties:
        return f"No properties found with profit above {min_profit_percent}%."
    
    return str(properties)

@mcp.tool()
async def summarise_postcode(postcode: str) -> str:
    """Get a summary of sold property stats for a postcode.
    
    Args:
        postcode: UK postcode to search
    """
    data = await fetch_properties(postcode)
    
    if not data.get("success") or data.get("noResults"):
        return "No results found for this postcode."
    
    props = data.get("priceComparisonData", [])
    profits = [p for p in props if p.get("positiveOrNegativePriceDifference") == "Profit"]
    losses = [p for p in props if p.get("positiveOrNegativePriceDifference") != "Profit"]
    
    avg_profit_pct = (
        sum(float(p.get("priceDifferencePercent", 0)) for p in profits) / len(profits)
        if profits else 0
    )
    
    return str({
        "postcode": postcode,
        "total_properties": len(props),
        "profitable_sales": len(profits),
        "loss_sales": len(losses),
        "average_profit_percent": round(avg_profit_pct, 2)
    })

def main():
    mcp.run(transport="sse")

if __name__ == "__main__":
    main()