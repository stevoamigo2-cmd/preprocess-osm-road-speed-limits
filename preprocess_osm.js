/**
 * preprocess_osm.js
 * - expects:
 *   * regions/tiles.json -> array of { z: int, x: int, y: int }
 * - environment variables:
 *   * OVERPASS_ENDPOINT (default https://overpass-api.de/api/interpreter)
 *   * TILE_ZOOM
 *   * THROTTLE_MS
 */

const fs = require('fs');
const fetch = require('node-fetch');
const path = require('path');

const OVERPASS = process.env.OVERPASS_ENDPOINT || 'https://overpass-api.de/api/interpreter';
const TILE_Z = process.argv[2] ? parseInt(process.argv[2], 10) : 13;
console.log("Generating tiles for zoom:", TILE_Z);

const THROTTLE_MS = parseInt(process.env.THROTTLE_MS || '1200', 10);

// read tiles list
const tilesFile = path.join(__dirname, 'regions', 'tiles.json');
if (!fs.existsSync(tilesFile)) {
  console.error('regions/tiles.json not found. Create an array like [{ "z":15,"x":17500,"y":11100 }, ...]');
  process.exit(1);
}
const tiles = JSON.parse(fs.readFileSync(tilesFile, 'utf8'));

// helper converts tile z/x/y to bbox (lat/lon)
function tile2bbox(x, y, z) {
  const n = Math.pow(2, z);
  const lon_left = x / n * 360 - 180;
  const lon_right = (x + 1) / n * 360 - 180;
  const lat_top = Math.atan(Math.sinh(Math.PI * (1 - 2 * y / n))) * 180 / Math.PI;
  const lat_bottom = Math.atan(Math.sinh(Math.PI * (1 - 2 * (y+1) / n))) * 180 / Math.PI;
  return [lat_bottom, lon_left, lat_top, lon_right];
}

// parse maxspeed function
function parseMaxspeedToMph(raw) {
  if (!raw) return -1;
  const s = raw.trim().toLowerCase();
  if (!s || s === 'none') return -1;
  try {
    if (s.includes('mph')) {
      const n = s.replace('mph','').replace(/[^\d.]/g,'');
      return Math.round(parseFloat(n));
    } else if (s.includes('km/h') || s.includes('kph') || s.includes('kmh') || s.includes('km')) {
      const n = s.replace(/[^\d.]/g,'');
      const kmh = parseFloat(n);
      return Math.round(kmh * 0.621371);
    } else {
      const n = s.replace(/[^\d.]/g,'');
      const v = parseFloat(n);
      return Math.round(v * 0.621371);
    }
  } catch(e) {
    return -1;
  }
}

// infer speed from highway type
function inferSpeedFromHighway(highway) {
  if (!highway) return -1;
  const h = highway.toLowerCase();
  switch(h) {
    case 'motorway': return 70;
    case 'trunk': return 60;
    case 'primary': return 50;
    case 'secondary': return 40;
    case 'tertiary': return 30;
    case 'unclassified':
    case 'residential': return 30;
    case 'service': return 10;
    default: return -1;
  }
}

async function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }

async function fetchTile(tile) {
  const { z, x, y } = tile;
  const [s, w, n, e] = tile2bbox(x, y, z);

  const q1 = `[out:json][timeout:25];
    (
      way["highway"]["maxspeed"](${s},${w},${n},${e});
    );
    out tags geom;`;

  const q2 = `[out:json][timeout:25];
    (
      way["highway"](${s},${w},${n},${e});
    );
    out tags geom;`;

  for (const q of [q1, q2]) {
    try {
      const url = `${OVERPASS}?data=${encodeURIComponent(q)}`;
      const res = await fetch(url, { headers: { 'User-Agent': 'preprocess-osm/1.0 (github actions)' } });
      if (!res.ok) {
        console.warn(`Overpass returned ${res.status} for tile z${z} x${x} y${y}`);
        continue;
      }
      const json = await res.json();
      if (!json.elements || json.elements.length === 0) continue;

      const features = [];
      for (const el of json.elements) {
        if (el.type !== 'way') continue;
        const tags = el.tags || {};
        let coords = [];
        if (Array.isArray(el.geometry)) {
          coords = el.geometry.map(p => [p.lat, p.lon]);
        }
        let mph = tags.maxspeed ? parseMaxspeedToMph(tags.maxspeed) : inferSpeedFromHighway(tags.highway);
        features.push({
          id: el.id,
          speed: (mph > 0 ? mph : null),
          highway: tags.highway || null,
          tags: { maxspeed: tags.maxspeed || null },
          coords
        });
      }

      return {
        z, x, y,
        tile_bbox: { south: s, west: w, north: n, east: e },
        fetched_at: (new Date()).toISOString(),
        features
      };
    } catch (e) {
      console.warn('Overpass query failed, trying next fallback', e);
      await sleep(500);
      continue;
    }
  }
  return null;
}

async function saveTileJsonLocally(z, x, y, obj) {
  const dir = path.join(__dirname, 'tiles', `${z}`, `${x}`);
  const filePath = path.join(dir, `${y}.json`);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(obj));
  return filePath;
}

(async () => {
  console.log('Tiles to process:', tiles.length);
  for (let i=0;i<tiles.length;i++) {
    const t = tiles[i];
    console.log(`Processing (${i+1}/${tiles.length}) z${t.z} x${t.x} y${t.y} ...`);
    const obj = await fetchTile(t);
    if (obj) {
      const filePath = await saveTileJsonLocally(t.z, t.x, t.y, obj);
      console.log('Saved tile', filePath);
    } else {
      console.log('No data for tile', t.z, t.x, t.y);
    }
    await sleep(THROTTLE_MS);
  }
  console.log('Done.');
})();
