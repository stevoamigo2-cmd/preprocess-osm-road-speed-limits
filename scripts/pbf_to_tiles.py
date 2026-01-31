#!/usr/bin/env python3
"""
pbf_to_tiles.py (country-aware; legacy writing optional)

Usage:
  python3 scripts/pbf_to_tiles.py --pbf data/pt_highways_max.osm.pbf --country pt --zoom 13 --out generated_tiles/tiles
  (To also write legacy top-level tiles only pass --write-legacy)
"""
import os
import argparse
import json
import tempfile
import shutil
import osmium
import mercantile
from shapely.geometry import LineString, mapping

parser = argparse.ArgumentParser()
parser.add_argument('--pbf', required=True)
parser.add_argument('--country', required=True, help='Country code, e.g. pt, it')
parser.add_argument('--zoom', default=13, type=int)
parser.add_argument('--out', required=True, help='Base output dir; script will write into OUT/{country}/...')
parser.add_argument('--write-legacy', action='store_true', help='If set, write legacy top-level tiles (tiles/{z}/{x}/{y}.json). Default: false')
parser.add_argument('--verbosity', default='INFO')
args = parser.parse_args()

PBF = args.pbf
COUNTRY = args.country.lower()
Z = args.zoom
OUTDIR_BASE = args.out
OUTDIR_COUNTRY = os.path.join(OUTDIR_BASE, COUNTRY)
WRITE_LEGACY = bool(args.write_legacy)

os.makedirs(OUTDIR_COUNTRY, exist_ok=True)
if WRITE_LEGACY:
    os.makedirs(OUTDIR_BASE, exist_ok=True)

tmpdir = tempfile.mkdtemp(prefix="tiles_tmp_")
tile_temp_files = {}

HIGHWAY_INFER_BY_COUNTRY = {
    "uk": {"motorway":70,"trunk":60,"primary":50,"secondary":40,"tertiary":30,"unclassified":30,"residential":30,"service":10},
    "pt": {"motorway":120,"trunk":100,"primary":90,"secondary":70,"tertiary":50,"unclassified":50,"residential":30,"service":10},
    # add more defaults as you like
}
HIGHWAY_INFER = HIGHWAY_INFER_BY_COUNTRY.get(COUNTRY, HIGHWAY_INFER_BY_COUNTRY.get("pt", {}))

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
            kmh = float(n) if n!='' else 0.0
            return int(round(kmh * 0.621371))
        n = ''.join(ch for ch in r if (ch.isdigit() or ch=='.'))
        if n == '':
            return -1
        v = float(n)
        # treat plain numeric as mph for UK, otherwise km/h -> mph
        if COUNTRY == 'uk':
            return int(round(v))
        else:
            return int(round(v * 0.621371))
    except Exception:
        return -1

class WayHandler(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.num = 0
    def way(self, w):
        if not w.nodes or len(w.nodes) < 1:
            return
        tags = {t.k: t.v for t in w.tags}
        if 'highway' not in tags:
            return
        coords = [(n.location.lon, n.location.lat) for n in w.nodes if n.location.valid()]
        if len(coords) < 2:
            return
        line = LineString(coords)
        if line.is_empty: return

        ms_raw = tags.get('maxspeed')
        ms_mph = parse_maxspeed_to_mph(ms_raw)
        highway = tags.get('highway')

        minx, miny, maxx, maxy = line.bounds
        tiles = mercantile.tiles(minx, miny, maxx, maxy, [Z])
        tiles_set = set(tiles)

        feature = {
            "type": "Feature",
            "geometry": mapping(line),
            "properties": {"id": w.id, "highway": highway, "maxspeed_raw": ms_raw if ms_raw is not None else None, "maxspeed_mph": ms_mph}
        }

        for t in tiles_set:
            zx = (Z, t.x, t.y)
            if zx not in tile_temp_files:
                path = os.path.join(tmpdir, f"tile_{Z}_{t.x}_{t.y}.ndjson")
                tile_temp_files[zx] = open(path, "a", encoding="utf-8")
            tile_temp_files[zx].write(json.dumps(feature) + "\n")

        self.num += 1
        if self.num % 10000 == 0:
            print("Processed ways:", self.num, flush=True)

print(f"Reading PBF ({PBF}) for country {COUNTRY} ...")
handler = WayHandler()
handler.apply_file(PBF, locations=True)
print("Finalizing per-tile GeoJSON...")

for fh in tile_temp_files.values(): fh.close()

for fname in os.listdir(tmpdir):
    if not fname.endswith(".ndjson"): continue
    parts = fname.replace("tile_","").replace(".ndjson","").split("_")
    z = int(parts[0]); x = int(parts[1]); y = int(parts[2])
    tmpf = os.path.join(tmpdir, fname)
    features = []
    with open(tmpf, "r", encoding="utf-8") as rr:
        for line in rr:
            try: features.append(json.loads(line))
            except: pass
    fc = {"type":"FeatureCollection", "features": features}

    # country path
    tile_dir_country = os.path.join(OUTDIR_COUNTRY, str(z), str(x))
    os.makedirs(tile_dir_country, exist_ok=True)
    with open(os.path.join(tile_dir_country, f"{y}.json"), "w", encoding="utf-8") as ww:
        json.dump(fc, ww)

    # optional legacy write only if requested
    if WRITE_LEGACY:
        tile_dir_legacy = os.path.join(OUTDIR_BASE, str(z), str(x))
        os.makedirs(tile_dir_legacy, exist_ok=True)
        with open(os.path.join(tile_dir_legacy, f"{y}.json"), "w", encoding="utf-8") as ww:
            json.dump(fc, ww)

shutil.rmtree(tmpdir)
print("Tiles written under:", OUTDIR_COUNTRY, ("and legacy" if WRITE_LEGACY else ""))
