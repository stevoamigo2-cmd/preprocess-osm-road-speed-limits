#!/usr/bin/env python3
"""
pbf_to_tiles.py
Stream OSM ways from a .osm.pbf and bin them into z/x/y GeoJSON files.
Usage:
  python3 pbf_to_tiles.py --pbf data/uk_highways_max.osm.pbf --zoom 13 --out generated_tiles/tiles
"""
import os
import sys
import argparse
import json
from collections import defaultdict
import math
import tempfile
import shutil
import osmium
import mercantile
from shapely.geometry import LineString, mapping

# -------- parse args --------
parser = argparse.ArgumentParser()
parser.add_argument('--pbf', required=True)
parser.add_argument('--zoom', default=13, type=int)
parser.add_argument('--out', required=True)
parser.add_argument('--verbosity', default='INFO')
args = parser.parse_args()

PBF = args.pbf
Z = args.zoom
OUTDIR = args.out
os.makedirs(OUTDIR, exist_ok=True)

# temporary per-tile NDJSON files (to avoid holding memory)
tmpdir = tempfile.mkdtemp(prefix="tiles_tmp_")
tile_temp_files = {}  # (z,x,y) -> file object

# heuristic inference for highway categories to mph (UK-leaning)
HIGHWAY_INFER = {
    "motorway": 70,
    "trunk": 60,
    "primary": 50,
    "secondary": 40,
    "tertiary": 30,
    "unclassified": 30,
    "residential": 30,
    "service": 10,
    "motorway_link": 60,
    "trunk_link": 50,
    "living_street": 10
}

def parse_maxspeed_to_mph(raw):
    if raw is None:
        return -1
    r = raw.strip().lower()
    if r == '' or r == 'none':
        return -1
    try:
        if 'mph' in r:
            n = ''.join(ch for ch in r if (ch.isdigit() or ch=='.'))
            return int(round(float(n)))
        if 'km/h' in r or 'kph' in r or 'kmh' in r or r.endswith('km'):
            n = ''.join(ch for ch in r if (ch.isdigit() or ch=='.'))
            kmh = float(n)
            return int(round(kmh * 0.621371))
        # plain numeric -> treat as km/h (default) since this is a UK extract but numeric often is km/h globally.
        n = ''.join(ch for ch in r if (ch.isdigit() or ch=='.'))
        if n == '':
            return -1
        v = float(n)
        # Because your extract is Great Britain, you might prefer to treat plain numeric as mph in UK.
        # If you want to treat plain numeric as mph for GB, change below to: return int(round(v))
        # Here we assume numeric is km/h unless 'gb' environ var set; to be safe, treat as km/h -> mph.
        return int(round(v * 0.621371))
    except Exception:
        return -1

class WayHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.num = 0
    def way(self, w):
        # only ways with geometry and at least one node
        if not w.nodes or len(w.nodes) < 1:
            return
        tags = {t.k: t.v for t in w.tags}

        if 'highway' not in tags:
            return
        coords = [(n.location.lon, n.location.lat) for n in w.nodes if n.location.valid()]
        if len(coords) < 2:
            return
        line = LineString(coords)
        if line.is_empty:
            return

        # properties
        ms_raw = tags.get('maxspeed')
        ms_mph = parse_maxspeed_to_mph(ms_raw)
        highway = tags.get('highway')

        # bounding tiles
        minx, miny, maxx, maxy = line.bounds  # lon/lat bounds
        # mercantile wants lat,lon pairs but mercantile.tiles accepts bbox lon/lat order: (west, south, east, north)
        tiles = mercantile.tiles(minx, miny, maxx, maxy, [Z])
        tiles_set = set(tiles)

        feature = {
            "type": "Feature",
            "geometry": mapping(line),
            "properties": {
                "id": w.id,
                "highway": highway,
                "maxspeed_raw": ms_raw if ms_raw is not None else None,
                "maxspeed_mph": ms_mph
            }
        }

        # append to each tile temp file
        for t in tiles_set:
            zx = (Z, t.x, t.y)
            if zx not in tile_temp_files:
                path = os.path.join(tmpdir, f"tile_{Z}_{t.x}_{t.y}.ndjson")
                tile_temp_files[zx] = open(path, "a", encoding="utf-8")
            fout = tile_temp_files[zx]
            fout.write(json.dumps(feature) + "\n")

        self.num += 1
        if self.num % 10000 == 0:
            print("Processed ways:", self.num, flush=True)

print("Reading PBF and binning ways into tiles (this may take a while)...")
handler = WayHandler()
handler.apply_file(PBF, locations=True)
print("Done reading. Finalizing per-tile GeoJSON...")

# close temp files, write final featurecollections
for zx, fh in tile_temp_files.items():
    fh.close()

# create tile output directory structure and convert ndjson -> GeoJSON FeatureCollection
for fname in os.listdir(tmpdir):
    if not fname.endswith(".ndjson"):
        continue
    parts = fname.replace("tile_","").replace(".ndjson","").split("_")
    z = int(parts[0]); x = int(parts[1]); y = int(parts[2])
    tile_dir = os.path.join(OUTDIR, str(z), str(x))
    os.makedirs(tile_dir, exist_ok=True)
    tile_path = os.path.join(tile_dir, f"{y}.json")
    tmpf = os.path.join(tmpdir, fname)
    features = []
    with open(tmpf, "r", encoding="utf-8") as rr:
        for line in rr:
            try:
                features.append(json.loads(line))
            except:
                pass
    fc = {"type":"FeatureCollection", "features": features}
    with open(tile_path, "w", encoding="utf-8") as ww:
        json.dump(fc, ww)
    # optional: compress or minify

# cleanup
shutil.rmtree(tmpdir)
print("Tiles written under:", OUTDIR)

