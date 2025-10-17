#!/usr/bin/env python3
"""Check tracked entity coordinates for a woreda and flag outliers."""
from __future__ import annotations

import csv
import json
import math
import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import requests

API_BASE = "http://pfizer.dhis.et/api"
PROGRAM_ID = "PK5z4GmhKjI"
WOREDA_ORG_UNIT_ID = os.environ.get("WOREDA_ORG_UNIT_ID", "xDjzO8C7aMO")
COORDINATE_ATTRIBUTE_ID = "rnAb1BzIfVV"
NAME_ATTRIBUTE_IDS: Sequence[str] = (
    "jXFBnlt8KyM",
    "hgXcoeoc1UE",
)
DEFAULT_CENTER = (6.663891, 38.155214)
DEFAULT_RADIUS_KM = 24.49

MAPPED_CENTERS: Dict[str, Tuple[float, float]] = {
    "xDjzO8C7aMO": (6.663891, 38.155214),
    "rAGMe1IZ7uy": (4.379710, 40.083732),
    "YqNyKBY20vV": (6.490719, 38.434032),
}

MAPPED_RADII: Dict[str, float] = {
    "xDjzO8C7aMO": 24.49,
    "rAGMe1IZ7uy": 158.6,
    "YqNyKBY20vV": 10.31,
}

WOREDA_CENTER = MAPPED_CENTERS.get(WOREDA_ORG_UNIT_ID, DEFAULT_CENTER)
WOREDA_RADIUS_KM = float(os.environ.get("WOREDA_RADIUS_KM", MAPPED_RADII.get(WOREDA_ORG_UNIT_ID, DEFAULT_RADIUS_KM)))

OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX")
if not OUTPUT_PREFIX:
    default_prefix_map = {
        "xDjzO8C7aMO": "loka_abaya",
        "rAGMe1IZ7uy": "moyale",
        "YqNyKBY20vV": "dara_otilcho",
    }
    OUTPUT_PREFIX = default_prefix_map.get(WOREDA_ORG_UNIT_ID, "woreda")

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_SUBDIR = OUTPUT_DIR / OUTPUT_PREFIX
OUTPUT_SUBDIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = OUTPUT_SUBDIR / "tracked_entity_distances.csv"
MISPLACED_OUTPUT_PATH = OUTPUT_SUBDIR / "misplaced_tracked_entities.csv"
GROUPED_OUTPUT_PATH = OUTPUT_SUBDIR / "tracked_entities_by_facility.json"
MISPLACED_JSON_PATH = OUTPUT_SUBDIR / "misplaced_tracked_entities.json"
PAGE_SIZE = 1000


def get_auth() -> Tuple[str, str]:
    username = os.environ.get("DHIS2_USERNAME")
    password = os.environ.get("DHIS2_PASSWORD")
    if not username or not password:
        raise RuntimeError("Set DHIS2_USERNAME and DHIS2_PASSWORD environment variables before running.")
    return username, password


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius * c


def parse_coordinate(value: object) -> Optional[Tuple[float, float]]:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            text = text.strip("[](){}").replace(";", ",")
            parts = [part.strip() for part in text.split(",") if part.strip()]
            if len(parts) >= 2:
                try:
                    lon = float(parts[0])
                    lat = float(parts[1])
                    return lat, lon
                except ValueError:
                    return None
            return None
        else:
            value = parsed
    if isinstance(value, (list, tuple)):
        try:
            lon = float(value[0])
            lat = float(value[1])
            return lat, lon
        except (ValueError, TypeError, IndexError):
            return None
    return None


def fetch_org_units(session: requests.Session) -> Dict[str, Dict[str, object]]:
    params = {
        "paging": "false",
        "fields": "id,name,geometry,coordinates,ancestors[id]",
        "filter": f"ancestors.id:eq:{WOREDA_ORG_UNIT_ID}",
    }
    response = session.get(f"{API_BASE}/organisationUnits", params=params, timeout=60)
    response.raise_for_status()
    units: Dict[str, Dict[str, object]] = {}
    for unit in response.json().get("organisationUnits", []):
        units[unit.get("id")] = unit
    return units


def fetch_tracked_entities(session: requests.Session) -> Iterable[Dict[str, object]]:
    page = 1
    while True:
        params = {
            "program": PROGRAM_ID,
            "orgUnit": WOREDA_ORG_UNIT_ID,
            "ouMode": "DESCENDANTS",
            "page": page,
            "pageSize": PAGE_SIZE,
            "fields": "trackedEntity,orgUnit,createdAt,updatedAt,geometry,attributes[attribute,value,valueType]",
        }
        response = session.get(f"{API_BASE}/tracker/trackedEntities", params=params, timeout=120)
        response.raise_for_status()
        payload = response.json()
        tracked_entities = payload.get("trackedEntities", [])
        if not tracked_entities:
            break
        for tei in tracked_entities:
            yield tei
        pager = payload.get("pager") or {}
        if page >= pager.get("pageCount", page):
            break
        page += 1


def fetch_attribute_labels(session: requests.Session) -> Dict[str, str]:
    params = {
        "fields": "programTrackedEntityAttributes[trackedEntityAttribute[id,displayName,shortName,code,name]]",
    }
    response = session.get(f"{API_BASE}/programs/{PROGRAM_ID}", params=params, timeout=60)
    response.raise_for_status()
    payload = response.json()
    labels: Dict[str, str] = {}
    for entry in payload.get("programTrackedEntityAttributes", []):
        attr = entry.get("trackedEntityAttribute") or {}
        attr_id = attr.get("id")
        if not attr_id:
            continue
        for key in ("displayName", "shortName", "name", "code"):
            value = attr.get(key)
            if value:
                labels[attr_id] = str(value)
                break
        else:
            labels[attr_id] = attr_id
    return labels


def sanitize_header(label: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", label).strip("_")
    return cleaned


def build_attribute_header_map(labels: Dict[str, str]) -> Dict[str, str]:
    used = set()
    header_map: Dict[str, str] = {}
    for attr_id, label in labels.items():
        base = sanitize_header(label) or attr_id
        candidate = base
        counter = 1
        while candidate in used:
            counter += 1
            candidate = f"{base}_{counter}"
        used.add(candidate)
        header_map[attr_id] = candidate
    return header_map


def main() -> None:
    username, password = get_auth()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with requests.Session() as session:
        session.auth = (username, password)
        session.headers.update({"Accept": "application/json"})

        org_units = fetch_org_units(session)
        tracked_entities = list(fetch_tracked_entities(session))
        attribute_labels = fetch_attribute_labels(session)

    attribute_header_map = build_attribute_header_map(attribute_labels)

    org_unit_coords: Dict[str, Tuple[float, float]] = {}
    org_unit_stats: Dict[str, Dict[str, Optional[float]]] = {}
    for org_id, data in org_units.items():
        coords = None
        geometry = data.get("geometry")
        if isinstance(geometry, dict):
            coords = parse_coordinate(geometry.get("coordinates"))
        if not coords:
            coords = parse_coordinate(data.get("coordinates"))
        if coords:
            org_unit_coords[org_id] = coords
            distance_to_center = haversine_km(
                WOREDA_CENTER[0],
                WOREDA_CENTER[1],
                coords[0],
                coords[1],
            )
            org_unit_stats[org_id] = {
                "latitude": coords[0],
                "longitude": coords[1],
                "distance_to_center": distance_to_center,
            }
        else:
            org_unit_stats[org_id] = {
                "latitude": None,
                "longitude": None,
                "distance_to_center": None,
            }

    rows: List[Dict[str, object]] = []
    missing_coordinate_entities = 0
    missing_org_unit_coordinate = 0
    missing_org_unit_ids = 0
    beyond_woreda_count = 0
    facility_groups: Dict[str, Dict[str, object]] = {}
    missing_coordinate_rows: List[Dict[str, object]] = []
    misplaced_rows: List[Dict[str, object]] = []

    for tei in tracked_entities:
        tei_id = tei.get("trackedEntity")
        org_unit_id = tei.get("orgUnit")
        attributes = tei.get("attributes") or []

        te_coords = None
        name_value = ""
        attribute_values_by_id: Dict[str, Optional[str]] = {}
        attribute_values_by_name: Dict[str, Optional[str]] = {}

        geometry = tei.get("geometry")
        if isinstance(geometry, dict):
            te_coords = parse_coordinate(geometry.get("coordinates"))

        for attr in attributes:
            attr_id = attr.get("attribute")
            value = attr.get("value")
            if not attr_id:
                continue
            if attr_id == COORDINATE_ATTRIBUTE_ID:
                te_coords = te_coords or parse_coordinate(value)
            elif attr_id in NAME_ATTRIBUTE_IDS and not name_value:
                name_value = str(value or "")

            attribute_values_by_id[attr_id] = value
            friendly_key = attribute_header_map.get(attr_id)
            if friendly_key:
                attribute_values_by_name[friendly_key] = value

        if not te_coords:
            missing_coordinate_entities += 1
            missing_row = {
                "trackedEntity": tei_id,
                "name": name_value,
                "orgUnit": org_unit_id,
                "orgUnitName": org_units.get(org_unit_id, {}).get("name", ""),
                "missingReason": "No coordinate in geometry or attribute",
            }
            for attr_id, value in attribute_values_by_id.items():
                if attr_id == COORDINATE_ATTRIBUTE_ID:
                    continue
                friendly_key = attribute_header_map.get(attr_id)
                if not friendly_key:
                    continue
                missing_row[f"attr_{friendly_key}"] = value
            missing_coordinate_rows.append(missing_row)
            continue

        facility_distance_to_center = None
        distance_to_facility = None

        if not org_unit_id:
            missing_org_unit_ids += 1
            facility_coords = None
        else:
            facility_coords = org_unit_coords.get(org_unit_id)
            if not facility_coords:
                missing_org_unit_coordinate += 1
            stats = org_unit_stats.get(org_unit_id)
            if stats:
                facility_distance_to_center = stats.get("distance_to_center")

        lat, lon = te_coords
        distance_to_center = haversine_km(WOREDA_CENTER[0], WOREDA_CENTER[1], lat, lon)
        if facility_coords:
            distance_to_facility = haversine_km(facility_coords[0], facility_coords[1], lat, lon)
        is_misplaced = False
        if distance_to_center > WOREDA_RADIUS_KM:
            beyond_woreda_count += 1
            is_misplaced = True

        combined_distance = None
        if facility_distance_to_center is not None and distance_to_facility is not None:
            combined_distance = facility_distance_to_center + distance_to_facility

        row = {
            "trackedEntity": tei_id,
            "name": name_value,
            "orgUnit": org_unit_id,
            "orgUnitName": org_units.get(org_unit_id, {}).get("name", ""),
            "latitude": f"{lat:.6f}",
            "longitude": f"{lon:.6f}",
            "distance_to_facility_km": f"{distance_to_facility:.2f}" if distance_to_facility is not None else "",
            "distance_to_woreda_center_km": f"{distance_to_center:.2f}",
            "facility_distance_to_center_km": f"{facility_distance_to_center:.2f}" if facility_distance_to_center is not None else "",
            "combined_distance_via_facility_km": f"{combined_distance:.2f}" if combined_distance is not None else "",
        }
        for attr_id, value in attribute_values_by_id.items():
            if attr_id == COORDINATE_ATTRIBUTE_ID:
                continue
            friendly_key = attribute_header_map.get(attr_id)
            if not friendly_key:
                continue
            row[f"attr_{friendly_key}"] = value
        rows.append(row)

        if is_misplaced:
            misplaced_rows.append(row.copy())

        facility_key = org_unit_id or "UNKNOWN"
        facility_entry = facility_groups.get(facility_key)
        if not facility_entry:
            unit_meta = org_units.get(org_unit_id or "", {})
            facility_entry = {
                "orgUnit": org_unit_id,
                "orgUnitName": unit_meta.get("name", "Unknown Facility" if org_unit_id else "Missing Org Unit"),
                "facilityLatitude": None,
                "facilityLongitude": None,
                "facilityDistanceToCenterKm": None,
                "entities": [],
            }
            facility_coords = org_unit_coords.get(org_unit_id) if org_unit_id else None
            if facility_coords:
                facility_entry["facilityLatitude"] = round(facility_coords[0], 6)
                facility_entry["facilityLongitude"] = round(facility_coords[1], 6)
                stats = org_unit_stats.get(org_unit_id, {})
                distance_val = stats.get("distance_to_center")
                if distance_val is not None:
                    facility_entry["facilityDistanceToCenterKm"] = round(distance_val, 2)
            facility_groups[facility_key] = facility_entry

        facility_entry["entities"].append(
            {
                "trackedEntity": tei_id,
                "name": name_value,
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "distanceToFacilityKm": round(distance_to_facility, 2) if distance_to_facility is not None else None,
                "distanceToWoredaCenterKm": round(distance_to_center, 2),
                "facilityDistanceToCenterKm": round(facility_distance_to_center, 2) if facility_distance_to_center is not None else None,
                "combinedDistanceViaFacilityKm": round(combined_distance, 2) if combined_distance is not None else None,
                "attributes": {
                    "byId": dict(attribute_values_by_id),
                    "byName": dict(attribute_values_by_name),
                },
            }
        )

    fieldnames = [
        "trackedEntity",
        "name",
        "orgUnit",
        "orgUnitName",
        "latitude",
        "longitude",
        "distance_to_facility_km",
        "distance_to_woreda_center_km",
        "facility_distance_to_center_km",
    "combined_distance_via_facility_km",
    ]

    # Ensure attribute columns appear in CSV header consistently
    attribute_columns = sorted({key for row in rows for key in row.keys() if key.startswith("attr_")})
    full_fieldnames = fieldnames + attribute_columns

    rows.sort(key=lambda row: (row.get("orgUnitName") or "", row.get("trackedEntity") or ""))

    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=full_fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    missing_coordinates_csv_path: Optional[Path] = None
    if missing_coordinate_rows:
        missing_coordinate_rows.sort(key=lambda row: (row.get("orgUnitName") or "", row.get("trackedEntity") or ""))
        missing_coordinates_csv_path = OUTPUT_SUBDIR / "tracked_entities_missing_coordinates.csv"
        with missing_coordinates_csv_path.open("w", newline="", encoding="utf-8") as handle:
            missing_fieldnames = [
                "trackedEntity",
                "name",
                "orgUnit",
                "orgUnitName",
                "missingReason",
            ]
            missing_attr_columns = sorted({key for row in missing_coordinate_rows for key in row.keys() if key.startswith("attr_")})
            writer = csv.DictWriter(handle, fieldnames=missing_fieldnames + missing_attr_columns)
            writer.writeheader()
            writer.writerows(missing_coordinate_rows)

    if misplaced_rows:
        misplaced_rows.sort(key=lambda row: (row.get("orgUnitName") or "", row.get("trackedEntity") or ""))
        with MISPLACED_OUTPUT_PATH.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=full_fieldnames)
            writer.writeheader()
            writer.writerows(misplaced_rows)
        with MISPLACED_JSON_PATH.open("w", encoding="utf-8") as handle:
            json.dump(misplaced_rows, handle, indent=2)

    grouped_payload = []
    for key in sorted(facility_groups.keys(), key=lambda k: facility_groups[k]["orgUnitName"] or ""):
        entry = facility_groups[key]
        entry_copy = {
            "orgUnit": entry["orgUnit"],
            "orgUnitName": entry["orgUnitName"],
            "facilityLatitude": entry["facilityLatitude"],
            "facilityLongitude": entry["facilityLongitude"],
            "facilityDistanceToCenterKm": entry.get("facilityDistanceToCenterKm"),
            "trackedEntityCount": len(entry["entities"]),
            "entities": entry["entities"],
        }
        grouped_payload.append(entry_copy)

    with GROUPED_OUTPUT_PATH.open("w", encoding="utf-8") as handle:
        json.dump(grouped_payload, handle, indent=2)

    print(f"Wrote {len(rows)} tracked entities to {OUTPUT_PATH}")
    print(f"Tracked entities missing coordinate attribute: {missing_coordinate_entities}")
    print(f"Tracked entities missing org unit id: {missing_org_unit_ids}")
    print(f"Tracked entities with missing facility coordinates: {missing_org_unit_coordinate}")
    print(f"Tracked entities beyond woreda radius ({WOREDA_RADIUS_KM} km): {beyond_woreda_count}")
    if misplaced_rows:
        print(f"Wrote {len(misplaced_rows)} misplaced tracked entities to {MISPLACED_OUTPUT_PATH}")
        print(f"Wrote misplaced tracked entities JSON to {MISPLACED_JSON_PATH}")
    if missing_coordinates_csv_path:
        print(f"Wrote tracked entities missing coordinates to {missing_coordinates_csv_path}")


if __name__ == "__main__":
    main()
