#!/usr/bin/env python3
"""
RCN GML Parser - Polish Real Estate Transactions

Parses RCN (Rejestr Cen Nieruchomości) GML files efficiently using streaming.
Handles 131MB+ files without loading entire file into memory.

Usage:
    python converter.py <gml_file> [--sample N] [--transactions-only] [--parcels-only]
"""

import argparse
import json
import sys
import re
from typing import Optional, List, Tuple, Dict
from xml.etree import ElementTree as ET
from pathlib import Path
from pyproj import Transformer


# Namespace definitions for RCN GML files
NAMESPACES = {
    "rcn": "urn:gugik:specyfikacje:gmlas:rejestrcennieruchomosci:1.0",
    "gml": "http://www.opengis.net/gml/3.2",
    "xlink": "http://www.w3.org/1999/xlink",
}

# EPSG:2178 (CS2000) to WGS84 transformer
_TRANSFORMER = Transformer.from_crs("EPSG:2178", "EPSG:4326", always_xy=True)


def convert_epsg_2178_to_wgs84(x: float, y: float) -> Tuple[float, float]:
    """Convert EPSG:2178 coordinates to WGS84 (lon, lat).
    
    EPSG:2178 uses (northing, easting) axis order, so we swap inputs.
    """
    return _TRANSFORMER.transform(y, x)


def convert_polygon(coords: List[Tuple[float, float]]) -> List[List[float]]:
    """Convert list of (x, y) EPSG:2178 coords to [[lon, lat], ...] for GeoJSON."""
    return [list(convert_epsg_2178_to_wgs84(x, y)) for x, y in coords]


def parse_transaction_date(wersja_id: str) -> Optional[str]:
    """Extract date from wersjaId field (format: YYYY-MM-DD or YYYY-MM-DDTHH-MM-SS)."""
    if not wersja_id:
        return None
    match = re.search(r"(\d{4}-\d{2}-\d{2})", wersja_id)
    return match.group(1) if match else None


def extract_transaction(elem: ET.Element) -> Optional[dict]:
    """
    Extract transaction data from RCN_Transakcja element.

    Returns:
        dict with keys: id, price, date, type, parcel_ref
    """
    trans_id = elem.get("{http://www.opengis.net/gml/3.2}id")
    if not trans_id:
        return None

    data = {
        "id": trans_id,
        "price": None,
        "date": None,
        "type": None,
        "parcel_ref": None,
    }

    for child in elem:
        tag = child.tag
        if "}" in tag:
            tag = tag.split("}")[1]

        if tag == "IdRCN":
            data["date"] = find_wersja_id(child)
        elif tag == "cenaTransakcjiBrutto":
            try:
                data["price"] = float(child.text) if child.text else None
            except (ValueError, TypeError):
                data["price"] = None
        elif tag == "rodzajTransakcji":
            data["type"] = child.text
        elif tag == "nieruchomosc":
            data["parcel_ref"] = child.get("{http://www.w3.org/1999/xlink}href")

    return data


def find_wersja_id(elem: ET.Element) -> Optional[str]:
    """Recursively find wersjaId element within nested structure."""
    for child in elem:
        tag = child.tag
        if "}" in tag:
            tag = tag.split("}")[1]
        if tag == "wersjaId" and child.text:
            return parse_transaction_date(child.text)
        result = find_wersja_id(child)
        if result:
            return result
    return None


def extract_parcel(elem: ET.Element) -> Optional[dict]:
    """
    Extract parcel data from RCN_Dzialka element.

    Returns:
        dict with keys: id, parcel_number, geometry (EPSG:2178 coords), area
    """
    parcel_id = elem.get("{http://www.opengis.net/gml/3.2}id")
    if not parcel_id:
        return None

    data = {
        "id": parcel_id,
        "parcel_number": None,
        "geometry": None,
        "area": None,
    }

    for child in elem:
        tag = child.tag
        if "}" in tag:
            tag = tag.split("}")[1]

        if tag == "idDzialki":
            data["parcel_number"] = child.text
        elif tag == "geometria":
            data["geometry"] = extract_geometry(child)
        elif tag == "polePowierzchniEwidencyjnej":
            try:
                data["area"] = float(child.text) if child.text else None
            except (ValueError, TypeError):
                data["area"] = None

    return data


def extract_geometry(geom_elem: ET.Element) -> Optional[List[Tuple[float, float]]]:
    """Extract coordinates from gml:Polygon geometry element."""
    for polygon in geom_elem:
        tag = polygon.tag
        if "}" in tag:
            tag = tag.split("}")[1]

        if tag == "Polygon":
            for exterior in polygon:
                ext_tag = exterior.tag
                if "}" in ext_tag:
                    ext_tag = ext_tag.split("}")[1]

                if ext_tag == "exterior":
                    for ring in exterior:
                        ring_tag = ring.tag
                        if "}" in ring_tag:
                            ring_tag = ring_tag.split("}")[1]

                        if ring_tag == "LinearRing":
                            for pos_list in ring:
                                pl_tag = pos_list.tag
                                if "}" in pl_tag:
                                    pl_tag = pl_tag.split("}")[1]

                                if pl_tag == "posList" and pos_list.text:
                                    return parse_pos_list(pos_list.text)
    return None


def parse_pos_list(text: str) -> List[Tuple[float, float]]:
    """
    Parse gml:posList text into list of coordinate tuples.
    Format: "x1 y1 x2 y2 x3 y3 ..."
    Returns: [(x1, y1), (x2, y2), ...]
    """
    if not text:
        return []

    try:
        values = text.strip().split()
        coords = []
        for i in range(0, len(values) - 1, 2):
            x = float(values[i])
            y = float(values[i + 1])
            coords.append((x, y))
        return coords
    except (ValueError, IndexError):
        return []


def stream_gml(filepath: str, mode: str = "both"):
    """
    Stream-parse GML file and yield transactions and/or parcels.

    Args:
        filepath: Path to GML file
        mode: 'transactions', 'parcels', or 'both'

    Yields:
        Tuple of (type, data) where type is 'transaction' or 'parcel'
    """
    context = ET.iterparse(filepath, events=("end",))

    for event, elem in context:
        tag = elem.tag
        if "}" in tag:
            tag = tag.split("}")[1]

        if tag == "RCN_Transakcja" and mode in ("transactions", "both"):
            data = extract_transaction(elem)
            if data:
                yield ("transaction", data)
            elem.clear()

        elif tag == "RCN_Dzialka" and mode in ("parcels", "both"):
            data = extract_parcel(elem)
            if data:
                yield ("parcel", data)
            elem.clear()


def load_gml(filepath: str) -> Tuple[dict, dict]:
    """
    Load GML file and return combined data structure.

    Returns:
        (transactions, parcels) dictionaries
    """
    transactions = {}
    parcels = {}

    for elem_type, data in stream_gml(filepath):
        if elem_type == "transaction":
            transactions[data["id"]] = data
        elif elem_type == "parcel":
            parcels[data["id"]] = data

    return transactions, parcels


def join_data(transactions: dict, parcels: dict) -> dict:
    """Join transactions to parcels and return combined data structure."""
    result = {}

    for trans_id, trans_data in transactions.items():
        parcel_ref = trans_data.get("parcel_ref")
        if not parcel_ref:
            continue

        parcel_data = parcels.get(parcel_ref)
        if not parcel_data:
            continue

        if parcel_ref not in result:
            result[parcel_ref] = {
                "parcel_id": parcel_ref,
                "parcel_number": parcel_data.get("parcel_number"),
                "area": parcel_data.get("area"),
                "geometry": parcel_data.get("geometry"),
                "transactions": [],
            }

        result[parcel_ref]["transactions"].append(
            {
                "transaction_id": trans_id,
                "price": trans_data.get("price"),
                "date": trans_data.get("date"),
                "type": trans_data.get("type"),
            }
        )

    return result


def to_geojson(all_parcels: dict) -> dict:
    """Convert all parcels to GeoJSON FeatureCollection.
    
    Args:
        all_parcels: Dictionary of all parcels (from load_gml or join_data)
    """
    features = []
    for parcel_id, data in all_parcels.items():
        # Skip features without geometry
        geometry = data.get("geometry")
        if not geometry:
            continue
        
        # Get transactions for this parcel (may be empty list)
        transactions = data.get("transactions", [])
        # Use first transaction if available, otherwise None
        trans = transactions[0] if transactions else None
        # Convert coordinates to WGS84
        wgs84_coords = convert_polygon(geometry)
        # Build properties - include transaction data if available
        properties = {
            "price": trans.get("price") if trans else None,
            "date": trans.get("date") if trans else None,
            "type": trans.get("type") if trans else None,
            "area": data.get("area"),
            "parcel_number": data.get("parcel_number"),
            "transactions_count": len(transactions),
        }
        feature = {
            "id": parcel_id,
            "properties": properties,
            "geometry": {
                "type": "Polygon",
                "coordinates": [wgs84_coords],
            },
        }
        features.append(feature)
    
    return {
        "type": "FeatureCollection",
        "features": features,
    }



def print_sample(
    transactions: dict, parcels: dict, n: int = 10, convert_coords: bool = False
):
    """Print sample of parsed data for verification."""
    print(f"=== Sample of {n} Transactions ===")

    trans_items = list(transactions.items())[:n]
    for trans_id, data in trans_items:
        print(f"ID: {trans_id}")
        print(f"  Date: {data.get('date')}")
        print(f"  Price: {data.get('price')}")
        print(f"  Type: {data.get('type')}")
        print(f"  Parcel Ref: {data.get('parcel_ref')}")
        print()

    print(f"=== Sample of {n} Parcels ===")
    parcel_items = list(parcels.items())[:n]
    for parcel_id, data in parcel_items:
        print(f"ID: {parcel_id}")
        print(f"  Parcel #: {data.get('parcel_number')}")
        print(f"  Area: {data.get('area')} m²")
        geom = data.get("geometry")
        if geom:
            if convert_coords:
                wgs84 = convert_polygon(geom)
                print(f"  Geometry (WGS84): {len(wgs84)} points")
                print(f"    First: {wgs84[0]}")
                print(f"    Last: {wgs84[-1]}")
            else:
                print(f"  Geometry (EPSG:2178): {len(geom)} points")
                print(f"    First: {geom[0]}")
                print(f"    Last: {geom[-1]}")
        print()


def main():
    parser = argparse.ArgumentParser(
        description="Parse RCN GML files for Polish Real Estate Transactions"
    )
    parser.add_argument("gml_file", help="Path to GML file")
    parser.add_argument(
        "--sample", type=int, default=0, help="Print N sample records and exit"
    )
    parser.add_argument(
        "--transactions-only", action="store_true", help="Only parse transactions"
    )
    parser.add_argument(
        "--parcels-only", action="store_true", help="Only parse parcels"
    )
    parser.add_argument(
        "--joined", action="store_true", help="Output joined transaction-parcel data"
    )
    parser.add_argument(
        "--wgs84", action="store_true", help="Convert coordinates to WGS84 in output"
    )
    parser.add_argument(
        "--json", action="store_true", help="Output GeoJSON to stdout"
    )
    parser.add_argument(
        "--output", type=str, help="Save output to file (use with --json)"
    )

    args = parser.parse_args()

    if not Path(args.gml_file).exists():
        print(f"Error: File not found: {args.gml_file}", file=sys.stderr)
        sys.exit(1)

    if args.transactions_only:
        mode = "transactions"
    elif args.parcels_only:
        mode = "parcels"
    else:
        mode = "both"

    print(f"Parsing {args.gml_file} (mode: {mode})...", file=sys.stderr)

    transactions, parcels = load_gml(args.gml_file)

    print(
        f"Loaded {len(transactions)} transactions, {len(parcels)} parcels",
        file=sys.stderr,
    )

    if args.sample > 0:
        print_sample(transactions, parcels, args.sample, convert_coords=args.wgs84)
        return

    if args.joined:
        joined = join_data(transactions, parcels)
        print(f"Joined {len(joined)} parcel-transaction pairs", file=sys.stderr)
        for i, (parcel_id, data) in enumerate(joined.items()):
            if i >= 5:
                break
            print(f"\n--- Parcel {i + 1} ---")
            print(f"Parcel ID: {parcel_id}")
            print(f"Parcel #: {data.get('parcel_number')}")
            print(f"Area: {data.get('area')} m²")
            if args.wgs84 and data.get("geometry"):
                wgs = convert_polygon(data["geometry"])
                print(f"Geometry (WGS84): {wgs[:2]}...")
            print(f"Transactions: {len(data.get('transactions', []))}")
            for j, trans in enumerate(data.get("transactions", [])[:3]):
                print(f"  [{j + 1}] {trans.get('date')} - {trans.get('price')} PLN")
        return

    if args.json:
        # Get joined data (parcels with transactions)
        joined = join_data(transactions, parcels)
        # Merge all parcels with their transactions (if any)
        all_parcels_for_geojson = {}
        for parcel_id, parcel_data in parcels.items():
            all_parcels_for_geojson[parcel_id] = {
                "id": parcel_id,
                "parcel_number": parcel_data.get("parcel_number"),
                "area": parcel_data.get("area"),
                "geometry": parcel_data.get("geometry"),
                "transactions": joined.get(parcel_id, {}).get("transactions", []),
            }
        
        geojson = to_geojson(all_parcels_for_geojson)
        output_json = json.dumps(geojson, indent=2)
        if args.output:
            output_path = Path(args.output)
            # Create parent directory if needed
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(output_json)
            print(f"GeoJSON saved to {args.output}", file=sys.stderr)
        else:
            print(output_json)
        return


    print("Data loaded. Use --sample N, --joined, or import as module.")


if __name__ == "__main__":
    main()
