#!/usr/bin/env python3
"""
RCN GML Parser - Polish Real Estate Transactions

Parses RCN (Rejestr Cen Nieruchomości) GML files efficiently using streaming.
Handles 131MB+ files without loading entire file into memory.

Usage:
    python converter.py <gml_file> [--sample N] [--transactions-only] [--parcels-only]
"""

import argparse
import sys
import re
from typing import Optional, List, Tuple, Dict
from xml.etree import ElementTree as ET
from pathlib import Path


# Namespace definitions for RCN GML files
NAMESPACES = {
    "rcn": "urn:gugik:specyfikacje:gmlas:rejestrcennieruchomosci:1.0",
    "gml": "http://www.opengis.net/gml/3.2",
    "xlink": "http://www.w3.org/1999/xlink",
}


def parse_transaction_date(wersja_id: str) -> Optional[str]:
    """Extract date from wersjaId field (format: YYYY-MM-DD or YYYY-MM-DDTHH-MM-SS)."""
    if not wersja_id:
        return None
    # Match YYYY-MM-DD pattern
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
        "geometry": None,  # List of (x, y) coordinate tuples
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
    # Find gml:posList within the geometry
    for polygon in geom_elem:
        tag = polygon.tag
        if "}" in tag:
            tag = tag.split("}")[1]

        if tag == "Polygon":
            # Look for posList in exterior -> LinearRing -> posList
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
    # Use iterparse for streaming - processes element by element
    # events='end' means we process after the element is complete
    context = ET.iterparse(filepath, events=("end",))

    for event, elem in context:
        # Get the tag without namespace
        tag = elem.tag
        if "}" in tag:
            tag = tag.split("}")[1]

        if tag == "RCN_Transakcja" and mode in ("transactions", "both"):
            data = extract_transaction(elem)
            if data:
                yield ("transaction", data)
            # Clear element to free memory
            elem.clear()

        elif tag == "RCN_Dzialka" and mode in ("parcels", "both"):
            data = extract_parcel(elem)
            if data:
                yield ("parcel", data)
            # Clear element to free memory
            elem.clear()


def load_gml(filepath: str) -> Tuple[dict, dict]:
    """
    Load GML file and return combined data structure.

    Returns:
        (transactions, parcels) dictionaries
        - transactions: {id: {transaction_data}}
        - parcels: {id: {parcel_data}}
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
    """
    Join transactions to parcels and return combined data structure.

    The join key is: transaction['parcel_ref'] == parcel['id']

    Returns:
        Dictionary keyed by parcel_id with transaction and geometry data
    """
    result = {}

    for trans_id, trans_data in transactions.items():
        parcel_ref = trans_data.get("parcel_ref")
        if not parcel_ref:
            continue

        # Look up parcel by reference
        parcel_data = parcels.get(parcel_ref)
        if not parcel_data:
            continue

        # Build combined record
        if parcel_ref not in result:
            result[parcel_ref] = {
                "parcel_id": parcel_ref,
                "parcel_number": parcel_data.get("parcel_number"),
                "area": parcel_data.get("area"),
                "geometry": parcel_data.get("geometry"),
                "transactions": [],
            }

        # Add transaction info
        result[parcel_ref]["transactions"].append(
            {
                "transaction_id": trans_id,
                "price": trans_data.get("price"),
                "date": trans_data.get("date"),
                "type": trans_data.get("type"),
            }
        )

    return result


def print_sample(transactions: dict, parcels: dict, n: int = 10):
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
            print(f"  Geometry: {len(geom)} points")
            print(f"    First point: {geom[0] if geom else 'N/A'}")
            print(f"    Last point: {geom[-1] if geom else 'N/A'}")
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

    args = parser.parse_args()

    # Check file exists
    if not Path(args.gml_file).exists():
        print(f"Error: File not found: {args.gml_file}", file=sys.stderr)
        sys.exit(1)

    # Determine mode
    if args.transactions_only:
        mode = "transactions"
    elif args.parcels_only:
        mode = "parcels"
    else:
        mode = "both"

    print(f"Parsing {args.gml_file} (mode: {mode})...", file=sys.stderr)

    # Parse file
    transactions, parcels = load_gml(args.gml_file)

    print(
        f"Loaded {len(transactions)} transactions, {len(parcels)} parcels",
        file=sys.stderr,
    )

    # Handle sample mode
    if args.sample > 0:
        print_sample(transactions, parcels, args.sample)
        return

    # Handle joined mode
    if args.joined:
        joined = join_data(transactions, parcels)
        print(f"Joined {len(joined)} parcel-transaction pairs", file=sys.stderr)
        # Print first few joined records as sample
        for i, (parcel_id, data) in enumerate(joined.items()):
            if i >= 5:
                break
            print(f"\n--- Parcel {i + 1} ---")
            print(f"Parcel ID: {parcel_id}")
            print(f"Parcel #: {data.get('parcel_number')}")
            print(f"Area: {data.get('area')} m²")
            print(f"Transactions: {len(data.get('transactions', []))}")
            for j, trans in enumerate(data.get("transactions", [])[:3]):
                print(f"  [{j + 1}] {trans.get('date')} - {trans.get('price')} PLN")
        return

    # Default: return data structures
    # In a real usage, you'd import and use the functions
    print("Data loaded. Use --sample N, --joined, or import as module.")


if __name__ == "__main__":
    main()
