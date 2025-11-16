/**
 * preprocess_osm.js
 * - expects:
 *   * ./firebase-service-account.json (created by workflow)
 *   * regions/tiles.json -> array of { z: int, x: int, y: int }
 * - environment variables:
 *   * OVERPASS_ENDPOINT (default https://overpass-api.de/api/interpreter)
 *   * FIREBASE_BUCKET (your bucket name)
 *   * TILE_ZOOM
 *   * THROTTLE_MS
 */

const fs = require('fs');
const fetch = require('node-fetch');
const { Storage } = require('@google-cloud/storage');
const path = require('path');

const OVERPASS = process.env.OVERPASS_ENDPOINT || 'https://overpass-api.de/api/interpreter';
const BUCKET = process.env.FIREBASE_BUCKET;
const TILE_Z = parseInt(process.env.TILE_ZOOM || '15', 10);
const THROTTLE_MS = parseInt(process.env.THROTTLE_MS || '1200', 10);

if (!BUCKET) {
  console.error('FIREBASE_BUCKET not set');
  process.exit(1);
}

// init google-cloud storage using service account
const storage = new Storage({ keyFilename: './firebase-service-account.json' });
const bucket = storage.bucket(BUCKET);

// read tiles list
const tilesFile = path.join(__dirname, 'regions', 'tiles.json');
if (!fs.existsSync(tilesFile)) {
  console.error('regions/tiles.json not found. Create an array like [{ "z":15,"x":17500,"y":11100 }, ...]');
  process.exit(1);
}
const tiles = JSON.parse(fs.readFileSync(tilesFile, 'utf8'));

// helper converts tile z/x/y to bbox (lat/lon)
function tile2bbox(x, y, z) {
  // returns [south, west, north, east]
  const n = Math.pow(2, z);
  const lon_left = x / n * 360 - 180;
  const lon_right = (x + 1) / n * 360 - 180;
  const lat_top = Math.atan(Math.sinh(Math.PI * (1 - 2 * y / n))) * 180 / Math.PI;
  const lat_bottom = Math.atan(Math.sinh(Math.PI * (1 - 2 * (y+1) / n))) * 180 / Math.PI;
  return [lat_bottom, lon_left, lat_top, lon_right];
}

// parse maxspeed function (similar logic to your Android)
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
      // bare number -> treat as km/h by default (safer globally)
      const n = s.replace(/[^\d.]/g,'');
      const v = parseFloat(n);
      return Math.round(v * 0.621371);
    }
  } catch(e) {
    return -1;
  }
}

// infer function fallbacks from highway tag (simple)
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
  const bbox = tile2bbox(x,y,z); // [s,w,n,e]
  // Overpass bbox uses south,west,north,east
  const [s,w,n,e] = bbox;

  // Overpass query: ways with highway - prefer those with maxspeed
  const q1 = `[out:json][timeout:25];
    (
      way["highway"]["maxspeed"](${s},${w},${n},${e});
    );
    out tags geom;`;

  // fallback q2: any highway ways
  const q2 = `[out:json][timeout:25];
    (
      way["highway"](${s},${w},${n},${e});
    );
    out tags geom;`;

  // try first query then fallback
  for (const q of [q1,q2]) {
    try {
      const url = `${OVERPASS}?data=${encodeURIComponent(q)}`;
      const res = await fetch(url, { headers: { 'User-Agent': 'preprocess-osm/1.0 (github actions)' } });
      if (!res.ok) {
        console.warn(`Overpass returned ${res.status} for tile z${z} x${x} y${y}`);
        continue;
      }
      const json = await res.json();
      if (!json.elements || json.elements.length === 0) {
        // nothing here
        continue;
      }

      // reduce to features we need
      const features = [];
      for (const el of json.elements) {
        if (el.type !== 'way') continue;
        const tags = el.tags || {};
        const wayId = el.id;
        let coords = [];
        if (Array.isArray(el.geometry)) {
          coords = el.geometry.map(p => [p.lat, p.lon]); // small arrays
        }
        // determine speed
        let mph = -1;
        if (tags.maxspeed) {
          mph = parseMaxspeedToMph(tags.maxspeed);
        }
        if (mph <= 0) {
          mph = inferSpeedFromHighway(tags.highway);
        }
        // only store minimal
        features.push({
          id: wayId,
          speed: (mph > 0 ? mph : null),
          highway: tags.highway || null,
          tags: {
            maxspeed: tags.maxspeed || null
          },
          coords: coords
        });
      }

      // final tile object
      const tileObj = {
        z, x, y,
        tile_bbox: { south: s, west: w, north: n, east: e },
        fetched_at: (new Date()).toISOString(),
        features
      };

      return tileObj;
    } catch (e) {
      console.warn('Overpass query failed, trying next fallback', e);
      // backoff and try next
      await sleep(500);
      continue;
    }
  }
  // nothing
  return null;
}

async function uploadTileJson(z,x,y,obj) {
  const key = `tiles/${z}/${x}/${y}.json`;
  const file = bucket.file(key);
  const contents = JSON.stringify(obj);
  await file.save(contents, {
    metadata: { contentType: 'application/json' },
    resumable: false
  });
  // make public (optional) - requires permissions on service account
  try {
    await file.makePublic();
  } catch(e) {
    console.warn('makePublic failed (check service account rights):', e.message || e);
  }
  return `https://storage.googleapis.com/${BUCKET}/${key}`;
}

(async () => {
  console.log('Tiles to process:', tiles.length);
  for (let i=0;i<tiles.length;i++) {
    const t = tiles[i];
    console.log(`Processing (${i+1}/${tiles.length}) z${t.z} x${t.x} y${t.y} ...`);
    const obj = await fetchTile(t);
    if (obj) {
      await uploadTileJson(t.z,t.x,t.y,obj);
      console.log('Uploaded tile', t.z, t.x, t.y);
    } else {
      console.log('No data for tile', t.z, t.x, t.y);
    }
    // throttle to be polite to Overpass
    await sleep(THROTTLE_MS);
  }
  console.log('Done.');
})();
