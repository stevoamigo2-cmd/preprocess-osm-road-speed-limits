#!/usr/bin/env python3
"""
pbf_to_tiles.py (country-aware; legacy optional)

Usage:
  python3 scripts/pbf_to_tiles.py \
    --pbf data/pt_highways_max.osm.pbf \
    --country pt \
    --zoom 13 \
    --out generated_tiles/tiles

(UK legacy support)
  python3 scripts/pbf_to_tiles.py \
    --pbf data/uk_highways_max.osm.pbf \
    --country uk \
    --zoom 13 \
    --out generated_tiles/tiles \
    --write-legacy
"""

import os, argparse, json, tempfile, shutil
import osmium, mercantile
from shapely.geometry import LineString, mapping

parser = argparse.ArgumentParser()
parser.add_argument('--pbf', required=True)
parser.add_argument('--country', required=True)
parser.add_argument('--zoom', type=int, default=13)
parser.add_argument('--out', required=True)
parser.add_argument('--write-legacy', action='store_true')
args = parser.parse_args()

COUNTRY = args.country.lower()
Z = args.zoom
OUT_BASE = args.out
OUT_COUNTRY = os.path.join(OUT_BASE, COUNTRY)

os.makedirs(OUT_COUNTRY, exist_ok=True)
if args.write_legacy:
    os.makedirs(OUT_BASE, exist_ok=True)

tmpdir = tempfile.mkdtemp(prefix="tiles_")
tile_files = {}

def kmh_to_mph(v): return int(round(v * 0.621371))

def parse_speed(raw):
    if not raw: return -1
    r = raw.lower().strip()
    n = ''.join(c for c in r if c.isdigit() or c == '.')
    if not n: return -1
    v = float(n)
    return int(round(v)) if ('mph' in r or COUNTRY == 'uk') else kmh_to_mph(v)

class Handler(osmium.SimpleHandler):
    def way(self, w):
        if 'highway' not in w.tags or len(w.nodes) < 2:
            return

        coords = [(n.location.lon, n.location.lat)
                  for n in w.nodes if n.location.valid()]
        if len(coords) < 2:
            return

        line = LineString(coords)
        minx, miny, maxx, maxy = line.bounds

        feature = {
            "type": "Feature",
            "geometry": mapping(line),
            "properties": {
                "id": w.id,
                "highway": w.tags.get('highway'),
                "maxspeed_raw": w.tags.get('maxspeed'),
                "maxspeed_mph": parse_speed(w.tags.get('maxspeed'))
            }
        }

        for t in set(mercantile.tiles(minx, miny, maxx, maxy, [Z])):
            key = (Z, t.x, t.y)
            if key not in tile_files:
                tile_files[key] = open(
                    os.path.join(tmpdir, f"{Z}_{t.x}_{t.y}.ndjson"), "a"
                )
            tile_files[key].write(json.dumps(feature) + "\n")

print(f"Reading {args.pbf}")
Handler().apply_file(args.pbf, locations=True)

for f in tile_files.values():
    f.close()

for fn in os.listdir(tmpdir):
    z, x, y = fn.replace(".ndjson", "").split("_")
    with open(os.path.join(tmpdir, fn)) as r:
        features = [json.loads(l) for l in r]

    fc = {"type": "FeatureCollection", "features": features}

    # country path
    out_c = os.path.join(OUT_COUNTRY, z, x)
    os.makedirs(out_c, exist_ok=True)
    out_path = os.path.join(out_c, f"{y}.json")

    # If an existing tile file is present, read and merge features
    if os.path.exists(out_path):
        try:
            with open(out_path, "r", encoding="utf-8") as r:
                existing = json.load(r)
                existing_features = existing.get("features", [])
        except Exception:
            existing_features = []

        # Dedupe by OSM way id
        seen_ids = set()
        merged_features = []

        for feat in existing_features:
            pid = feat.get("properties", {}).get("id")
            if pid is not None:
                seen_ids.add(pid)
            merged_features.append(feat)

        for feat in features:
            pid = feat.get("properties", {}).get("id")
            if pid is not None and pid in seen_ids:
                continue
            merged_features.append(feat)
            if pid is not None:
                seen_ids.add(pid)

        merged_fc = {"type": "FeatureCollection", "features": merged_features}
        with open(out_path, "w", encoding="utf-8") as w:
            json.dump(merged_fc, w)
    else:
        # No existing file — write normally
        with open(out_path, "w", encoding="utf-8") as w:
            json.dump(fc, w)

    # legacy UK
    if args.write_legacy:
        out_l = os.path.join(OUT_BASE, z, x)
        os.makedirs(out_l, exist_ok=True)
        with open(os.path.join(out_l, f"{y}.json"), "w") as w:
            json.dump(fc, w)


shutil.rmtree(tmpdir)
print("Done:", OUT_COUNTRY)
