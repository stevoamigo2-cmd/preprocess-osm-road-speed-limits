#!/usr/bin/env python3
"""
pbf_to_tiles.py (FAST version, country-aware; legacy optional)

Usage:
  python3 scripts/pbf_to_tiles.py \
    --pbf data/fr_highways_max.osm.pbf \
    --country fr \
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

import os, argparse, json, tempfile, shutil, gzip
import osmium, mercantile

parser = argparse.ArgumentParser()
parser.add_argument("--pbf", required=True)
parser.add_argument("--country", required=True)
parser.add_argument("--zoom", type=int, default=13)
parser.add_argument("--out", required=True)
parser.add_argument("--write-legacy", action="store_true")
parser.add_argument(
    "--flush",
    type=int,
    default=5000,
    help="How many buffered features before flushing to disk",
)
args = parser.parse_args()

COUNTRY = args.country.lower()
Z = args.zoom
OUT_BASE = args.out
OUT_COUNTRY = os.path.join(OUT_BASE, COUNTRY)

os.makedirs(OUT_COUNTRY, exist_ok=True)
if args.write_legacy:
    os.makedirs(OUT_BASE, exist_ok=True)

tmpdir = tempfile.mkdtemp(prefix="tiles_")

# Key=(z,x,y) -> gzip file handle (opened once, appended many times)
open_files = {}

# Key=(z,x,y) -> list[str] (json lines buffered)
buffers = {}

ways_seen = 0
ways_written = 0


def kmh_to_mph(v):
    return int(round(v * 0.621371))


def parse_speed(raw):
    if not raw:
        return -1

    r = raw.lower().strip()

    # Extract numeric part safely ("50", "50 mph", "50km/h", "50;60")
    num = ""
    for c in r:
        if c.isdigit() or c == ".":
            num += c
        elif num:
            break

    if not num:
        return -1

    try:
        v = float(num)
    except:
        return -1

    # UK or mph tag -> keep mph
    if "mph" in r or COUNTRY == "uk":
        return int(round(v))

    # otherwise assume km/h -> convert to mph
    return kmh_to_mph(v)


def clamp_lat(lat):
    # Mercator safe clamp
    if lat > 85.05112878:
        return 85.05112878
    if lat < -85.05112878:
        return -85.05112878
    return lat


def get_tile_list_for_bounds(minx, miny, maxx, maxy):
    # mercantile.tiles expects lon/lat bounds
    miny = clamp_lat(miny)
    maxy = clamp_lat(maxy)
    return mercantile.tiles(minx, miny, maxx, maxy, [Z])


def flush_buffers(force=False):
    """
    Flush buffered NDJSON lines to gzipped per-tile files.
    This prevents:
      - too many open files
      - slowdowns from constantly opening/closing files
    """
    global buffers, open_files

    if not buffers:
        return

    if not force:
        total_lines = sum(len(v) for v in buffers.values())
        if total_lines < args.flush:
            return

    for key, lines in buffers.items():
        z, x, y = key
        fn = os.path.join(tmpdir, f"{z}_{x}_{y}.ndjson.gz")

        f = open_files.get(key)
        if f is None:
            # open once, append for the whole run
            f = gzip.open(fn, "at", encoding="utf-8")
            open_files[key] = f

        f.writelines(lines)

    buffers.clear()


class Handler(osmium.SimpleHandler):
    def way(self, w):
        global ways_seen, ways_written

        ways_seen += 1

        if "highway" not in w.tags or len(w.nodes) < 2:
            return

        coords = []
        minx = 999.0
        maxx = -999.0
        miny = 999.0
        maxy = -999.0

        # build coords + bounds without shapely (much faster)
        for n in w.nodes:
            if not n.location.valid():
                continue
            lon = n.location.lon
            lat = n.location.lat
            coords.append((lon, lat))
            if lon < minx:
                minx = lon
            if lon > maxx:
                maxx = lon
            if lat < miny:
                miny = lat
            if lat > maxy:
                maxy = lat

        if len(coords) < 2:
            return

        feature = {
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {
                "id": w.id,
                "highway": w.tags.get("highway"),
                "maxspeed_raw": w.tags.get("maxspeed"),
                "maxspeed_mph": parse_speed(w.tags.get("maxspeed")),
            },
        }

        js = json.dumps(feature, separators=(",", ":")) + "\n"

        # Assign to all intersecting tiles
        for t in get_tile_list_for_bounds(minx, miny, maxx, maxy):
            key = (t.z, t.x, t.y)
            if key not in buffers:
                buffers[key] = []
            buffers[key].append(js)

        ways_written += 1

        # flush periodically
        if ways_seen % 2000 == 0:
            flush_buffers(force=False)
            print(
                f"[{COUNTRY}] ways_seen={ways_seen} ways_written={ways_written} open_tiles={len(open_files)}"
            )


print(f"Reading {args.pbf}")
Handler().apply_file(args.pbf, locations=True)

# final flush
flush_buffers(force=True)

# close all open gz files
for f in open_files.values():
    try:
        f.close()
    except:
        pass

print(f"Building final tile JSON files for {COUNTRY}...")

# Convert each tile ndjson.gz -> FeatureCollection json
for fn in os.listdir(tmpdir):
    if not fn.endswith(".ndjson.gz"):
        continue

    z, x, y = fn.replace(".ndjson.gz", "").split("_")

    tile_path_country = os.path.join(OUT_COUNTRY, z, x)
    os.makedirs(tile_path_country, exist_ok=True)

    features = []
    with gzip.open(os.path.join(tmpdir, fn), "rt", encoding="utf-8") as r:
        for line in r:
            if line.strip():
                features.append(json.loads(line))

    fc = {"type": "FeatureCollection", "features": features}

    # country output
    with open(os.path.join(tile_path_country, f"{y}.json"), "w", encoding="utf-8") as w:
        json.dump(fc, w)

    # legacy output (UK old path)
    if args.write_legacy:
        tile_path_legacy = os.path.join(OUT_BASE, z, x)
        os.makedirs(tile_path_legacy, exist_ok=True)
        with open(os.path.join(tile_path_legacy, f"{y}.json"), "w", encoding="utf-8") as w:
            json.dump(fc, w)

shutil.rmtree(tmpdir)
print("Done:", OUT_COUNTRY)
