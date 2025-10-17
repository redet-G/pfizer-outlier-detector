#!/usr/bin/env python3
"""Fetch coordinates for specific organisation units and export them."""
from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import requests

API_BASE = "http://pfizer.dhis.et/api"
OUTPUT_CSV = Path(__file__).resolve().parent.parent / "output" / "moyale_orgunit_coordinates.csv"

MOYALE_UNITS: List[Dict[str, str]] = [
    {"name": "Abitu Health Post", "id": "aR73RCBtXRs"},
    {"name": "AF-Goye Health Center", "id": "NXHSBFHcNRa"},
    {"name": "Arda Ola Health Post", "id": "yJXb07N6AIw"},
    {"name": "Galgaludmitu Health Post", "id": "ZLDyotgOhBJ"},
    {"name": "Gulale HP", "id": "vrQXqHVnkIL"},
    {"name": "Halohuluko Health Post", "id": "YRrFiNgI3Uk"},
    {"name": "Lagasure Health Post", "id": "ZN7yYiuD7qI"},
    {"name": "Bede Health Post", "id": "tpLwwHEZcYT"},
    {"name": "Dhukiso Health Center", "id": "uxlEsOvpBjJ"},
    {"name": "Nanawa Health Post", "id": "ieTOtQMVyGx"},
    {"name": "Tuladaye Health Post", "id": "C30WfA8S96I"},
    {"name": "Didguchi Health Post", "id": "f1g3A7pxJzf"},
    {"name": "El-Gof Health Post", "id": "VNhH5EYKFt1"},
    {"name": "El-Ley Health Center", "id": "rVkaF3mIzdj"},
    {"name": "Elfetu Health Post", "id": "krOPXivnCzd"},
    {"name": "Hagardhere Health Post", "id": "JZYZzyPVpqn"},
    {"name": "Jima Comprehensive Health Post", "id": "mOar4JKlg0f"},
    {"name": "Karaya Health Post", "id": "ePMjFhhTUdh"},
    {"name": "Kojuwa Health Post", "id": "t3U56qubxaZ"},
    {"name": "Lagasade Health Post", "id": "O8pSced5bwR"},
    {"name": "Malmal Health Post", "id": "kKhSrBeZuH9"},
    {"name": "Raro Health Post", "id": "n8yB1GEWakk"},
    {"name": "Sororo Health Post", "id": "i4tyzTOwzVx"},
    {"name": "Buladi Health Post", "id": "YU2gigxCxz4"},
    {"name": "Burgab Health Post", "id": "kk5x9uEZmYo"},
    {"name": "Chamuk Health Post", "id": "QJa5K8WhGs2"},
    {"name": "Halgan Health Post", "id": "TC1Jg0mxQ5r"},
    {"name": "Kabanawa Health Post", "id": "QaUEN4XMm5w"},
    {"name": "Madomigo Health Post", "id": "G7ZgowvYyqi"},
    {"name": "Malab Health Post", "id": "KzIu5DN8mcE"},
    {"name": "Moyale Health Center", "id": "rOmJoBjKZr9"},
]


def get_auth() -> Tuple[str, str]:
    username = os.environ.get("DHIS2_USERNAME")
    password = os.environ.get("DHIS2_PASSWORD")
    if not username or not password:
        raise RuntimeError("Set DHIS2_USERNAME and DHIS2_PASSWORD environment variables before running.")
    return username, password


def fetch_coordinates(session: requests.Session, unit_ids: Iterable[str]) -> Dict[str, Tuple[float, float]]:
    coord_map: Dict[str, Tuple[float, float]] = {}
    ids_str = ",".join(unit_ids)
    params = {
        "filter": f"id:in:[{ids_str}]",
        "fields": "id,name,geometry,coordinates",
        "paging": "false",
    }
    response = session.get(f"{API_BASE}/organisationUnits", params=params, timeout=60)
    response.raise_for_status()
    data = response.json()
    for unit in data.get("organisationUnits", []):
        unit_id = unit.get("id")
        coords = None
        geometry = unit.get("geometry")
        if isinstance(geometry, dict) and geometry.get("type") == "Point":
            raw = geometry.get("coordinates")
            if isinstance(raw, list) and len(raw) >= 2:
                lon, lat = raw[0], raw[1]
                try:
                    coords = (float(lat), float(lon))
                except (ValueError, TypeError):
                    coords = None
        if coords is None and unit.get("coordinates"):
            # coordinates stored as string "[lon,lat]" or "lat,lon"
            raw = str(unit["coordinates"]).strip("[]()")
            parts = [p.strip() for p in raw.replace(";", ",").split(",") if p.strip()]
            if len(parts) >= 2:
                try:
                    lon, lat = float(parts[0]), float(parts[1])
                    coords = (lat, lon)
                except ValueError:
                    coords = None
        if unit_id and coords:
            coord_map[unit_id] = coords
    return coord_map


def main() -> None:
    username, password = get_auth()
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    unit_ids = [unit["id"] for unit in MOYALE_UNITS]

    with requests.Session() as session:
        session.auth = (username, password)
        session.headers.update({"Accept": "application/json"})

        coords_map = fetch_coordinates(session, unit_ids)

    rows: List[Dict[str, str]] = []
    missing: List[Dict[str, str]] = []

    for unit in MOYALE_UNITS:
        unit_id = unit["id"]
        name = unit["name"]
        coords = coords_map.get(unit_id)
        if coords:
            lat, lon = coords
            rows.append({
                "id": unit_id,
                "name": name,
                "latitude": f"{lat:.6f}",
                "longitude": f"{lon:.6f}",
            })
        else:
            missing.append(unit)

    if rows:
        with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["id", "name", "latitude", "longitude"])
            writer.writeheader()
            writer.writerows(rows)
        print(f"Saved {len(rows)} unit coordinates to {OUTPUT_CSV}")
    else:
        print("No coordinates were returned for the requested units.")

    if missing:
        print("Missing coordinates for the following IDs:")
        for unit in missing:
            print(f"  {unit['id']} - {unit['name']}")


if __name__ == "__main__":
    main()