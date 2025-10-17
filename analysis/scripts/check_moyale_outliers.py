#!/usr/bin/env python3
"""Fetch Moyale program events and flag records outside the allowed radius."""
from __future__ import annotations

import csv
import math
import os
import json
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import requests

API_BASE = "http://pfizer.dhis.et/api"
PROGRAM_ID = "PK5z4GmhKjI"
MOYALE_ORG_UNIT_ID = "rAGMe1IZ7uy"
MOYALE_CENTER = (4.417937, 40.141009)  # (lat, lon)
MOYALE_RADIUS_KM = 152.54
PAGE_SIZE = 200
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "output" / "moyale_misplaced_events.csv"


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


def fetch_events(session: requests.Session) -> Iterable[Dict]:  # fetch tracked entity instances
    page = 1
    while True:
        params = {
            "program": PROGRAM_ID,
            "orgUnit": MOYALE_ORG_UNIT_ID,
            "ouMode": "DESCENDANTS",
            "pageSize": PAGE_SIZE,
            "page": page,
            "fields": "event,trackedEntity,programStage,status,orgUnit,orgUnitName,eventDate,occurredAt,storedBy,created,lastUpdated,geometry,coordinate,dataValues[dataElement,value]",
        }
        response = session.get(f"{API_BASE}/events", params=params, timeout=60)
        response.raise_for_status()
        payload = response.json()
        events = payload.get("events", [])
        if not events:
            break
        for event in events:
            yield event
        pager = payload.get("pager") or {}
        if page >= pager.get("pageCount", page):
            break
        page += 1


def extract_coordinates(event: Dict) -> Tuple[float, float] | None:
    geometry = event.get("geometry")
    if geometry and geometry.get("type") == "Point":
        coordinates = geometry.get("coordinates")
        if isinstance(coordinates, list) and len(coordinates) == 2:
            lon, lat = coordinates
            try:
                return float(lat), float(lon)
            except (TypeError, ValueError):
                return None
    coordinate = event.get("coordinate")
    if isinstance(coordinate, dict):
        lat = coordinate.get("latitude")
        lon = coordinate.get("longitude")
        try:
            return float(lat), float(lon)
        except (TypeError, ValueError):
            return None
    if isinstance(coordinate, str):
        try:
            lon_str, lat_str = coordinate.split(",")
            return float(lat_str), float(lon_str)
        except (ValueError, AttributeError):
            return None

    return None


def parse_coordinate_value(value: object) -> Tuple[float, float] | None:
    if value is None:
        return None

    coords_list = None

    if isinstance(value, str):
        cleaned = value.strip()
        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError:
            cleaned = cleaned.strip("[](){}").replace(";", ",")
            parts = [part.strip() for part in cleaned.split(",") if part.strip()]
            if len(parts) >= 2:
                try:
                    coords_list = [float(parts[0]), float(parts[1])]
                except ValueError:
                    return None
        else:
            value = parsed

    if coords_list is None and isinstance(value, (list, tuple)):
        raw = list(value)
        if len(raw) >= 2:
            try:
                coords_list = [float(raw[0]), float(raw[1])]
            except (TypeError, ValueError):
                return None

    if not coords_list or len(coords_list) < 2:
        return None

    lon, lat = coords_list[0], coords_list[1]
    try:
        return float(lat), float(lon)
    except (TypeError, ValueError):
        return None


def parse_org_unit_coordinate(unit: Dict) -> Tuple[float, float] | None:
    geometry = unit.get("geometry")
    if isinstance(geometry, dict) and geometry.get("type") == "Point":
        coords = parse_coordinate_value(geometry.get("coordinates"))
        if coords:
            return coords

    coordinates_field = unit.get("coordinates")
    return parse_coordinate_value(coordinates_field)


def fetch_org_unit_coordinate_map(session: requests.Session) -> Dict[str, Tuple[float, float]]:
    coord_map: Dict[str, Tuple[float, float]] = {}
    params = {
        "paging": "false",
        "fields": "id,name,geometry,coordinates,ancestors[id]",
        "filter": f"ancestors.id:eq:{MOYALE_ORG_UNIT_ID}",
    }
    response = session.get(f"{API_BASE}/organisationUnits", params=params, timeout=60)
    response.raise_for_status()
    payload = response.json()
    for unit in payload.get("organisationUnits", []):
        unit_id = unit.get("id")
        coords = parse_org_unit_coordinate(unit)
        if unit_id and coords:
            coord_map[unit_id] = coords
    return coord_map


def flatten_event(event: Dict) -> Dict[str, str]:
    row: Dict[str, str] = {
        "event": event.get("event", ""),
        "trackedEntity": event.get("trackedEntity", ""),
        "orgUnit": event.get("orgUnit", ""),
        "orgUnitName": event.get("orgUnitName", ""),
        "programStage": event.get("programStage", ""),
        "status": event.get("status", ""),
        "eventDate": event.get("eventDate", ""),
        "occurredAt": event.get("occurredAt", ""),
        "storedBy": event.get("storedBy", ""),
        "created": event.get("created", ""),
        "lastUpdated": event.get("lastUpdated", ""),
    }
    attrs = event.get("dataValues") or []
    for attr in attrs:
        de = attr.get("dataElement") or ""
        value = attr.get("value")
        key = f"dv_{de}" if de else "dv"
        row[key] = value if value is not None else ""
    return row


def main() -> None:
    username, password = get_auth()
    output_path = OUTPUT_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)

    misplaced_rows: List[Dict[str, str]] = []
    all_keys: set[str] = set()
    total_events = 0
    events_with_coordinates = 0
    events_without_coordinates = 0
    events_with_event_geometry = 0
    events_with_orgunit_coordinates = 0

    missing_org_units: set[str] = set()

    with requests.Session() as session:
        session.auth = (username, password)
        session.headers.update({"Accept": "application/json"})
        org_unit_coordinates = fetch_org_unit_coordinate_map(session)

        for event in fetch_events(session):
            total_events += 1
            coords = extract_coordinates(event)
            source = "event" if coords else None
            if not coords:
                org_unit_id = event.get("orgUnit")
                coords = org_unit_coordinates.get(org_unit_id)
                if coords:
                    source = "orgUnit"
                elif org_unit_id:
                    missing_org_units.add(org_unit_id)

            if not coords:
                events_without_coordinates += 1
                continue

            events_with_coordinates += 1
            if source == "orgUnit":
                events_with_orgunit_coordinates += 1
            else:
                events_with_event_geometry += 1
            lat, lon = coords
            distance = haversine_km(MOYALE_CENTER[0], MOYALE_CENTER[1], lat, lon)
            if distance <= MOYALE_RADIUS_KM:
                continue

            row = flatten_event(event)
            row.update({
                "latitude": f"{lat:.6f}",
                "longitude": f"{lon:.6f}",
                "distance_km": f"{distance:.2f}",
            })
            if source:
                row["coordinate_source"] = source
            misplaced_rows.append(row)
            all_keys.update(row.keys())

    if missing_org_units:
        preview = ", ".join(sorted(missing_org_units)[:5])
        if len(missing_org_units) > 5:
            preview += ", ..."
        print(f"Missing coordinates for {len(missing_org_units)} org units (IDs: {preview})")

    if not misplaced_rows:
        print(
            "Checked {} events ({} with coordinates: {} from event geometry, {} from org unit metadata; {} without).".format(
                total_events,
                events_with_coordinates,
                events_with_event_geometry,
                events_with_orgunit_coordinates,
                events_without_coordinates,
            )
        )
        print("No misplaced records found.")
        return

    fieldnames = sorted(all_keys)
    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(misplaced_rows)

    print(f"Wrote {len(misplaced_rows)} misplaced records to {output_path}")
    print(
        "Checked {} events ({} with coordinates: {} from event geometry, {} from org unit metadata; {} without).".format(
            total_events,
            events_with_coordinates,
            events_with_event_geometry,
            events_with_orgunit_coordinates,
            events_without_coordinates,
        )
    )


if __name__ == "__main__":
    main()
