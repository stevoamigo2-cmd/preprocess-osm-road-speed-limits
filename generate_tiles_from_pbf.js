/**
 * generate_tiles_from_pbf.js
 * Usage: node generate_tiles_from_pbf.js <pbf-file> [zoom]
 *
 * Requirements:
 *  - osmium (osmium-tool) installed on the machine running this script
 *
 * What it does:
 *  1. creates a filtered PBF containing only ways with highway or maxspeed tags
 *  2. runs `osmium export` with geojsonseq (newline-delimited geojson) and streams stdout
 *  3. for each way feature, maps it to tile coordinates (by point sampling) and appends a compact
 *     feature object to out/tiles/<z>/<x>/<y>.ndjson
 *  4. after the stream, converts each .ndjson -> proper JSON array y.json
 *
 * Notes:
 *  - This avoids loading large GeoJSON into memory.
 *  - The tile assignment uses point-based sampling: each vertex in the way will drop the way into the tile
 *    that contains that vertex. That covers most cases (ways spanning tiles get included into each tile).
 */

const fs = require('fs');
const path = require('path');
const { spawnSync, spawn } = require('child_process');

if (process.argv.length < 3) {
  console.error('Usage: node generate_tiles_from_pbf.js <pbf-file> [zoom]');
  process.exit(2);
}

const PBF_FILE = process.argv[2];
const ZOOM = process.argv[3] ? parseInt(process.argv[3], 10) : 13;

if (!fs.existsSync(PBF_FILE)) {
  console.error('PBF file not found:', PBF_FILE);
  process.exit(2);
}

// helper: convert lon/lat to slippy tile x,y at zoom
function lonLatToTileXY(lon, lat, z) {
  const xtile = Math.floor((lon + 180) / 360 * Math.pow(2, z));
  const latRad = lat * Math.PI / 180;
  const ytile = Math.floor((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * Math.pow(2, z));
  return [xtile, ytile];
}

// minimal parse maxspeed -> mph
function parseMaxspeedToMph(raw) {
  if (!raw) return -1;
  const s = String(raw).trim().toLowerCase();
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
  } catch (e) {
    return -1;
  }
}

// ensure out directory
const OUT_DIR = path.join(__dirname, 'out', 'tiles', String(ZOOM));
fs.mkdirSync(OUT_DIR, { recursive: true });

// 1) create a filtered PBF (ways with highway or maxspeed)
const filteredPbf = path.join(__dirname, path.basename(PBF_FILE, path.extname(PBF_FILE)) + '.filtered.pbf');
console.log('Filtering PBF to ways with highway or maxspeed tags (creates):', filteredPbf);
try {
  // tags-filter <input> w/highway w/maxspeed -o <output>
  const res = spawnSync('osmium', ['tags-filter', PBF_FILE, 'w/highway', 'w/maxspeed', '-o', filteredPbf], { stdio: 'inherit' });
  if (res.status !== 0) throw new Error('osmium tags-filter failed with status ' + res.status);
} catch (err) {
  console.error('Failed to run osmium tags-filter. Make sure osmium-tool is installed and on PATH.');
  console.error(err && err.message ? err.message : err);
  process.exit(3);
}

// 2) export filtered PBF to geojsonseq and stream stdout
console.log('Streaming export from osmium -> geojsonseq (processing features)...');

const osmium = spawn('osmium', ['export', filteredPbf, '-f', 'geojsonseq'], { stdio: ['ignore', 'pipe', 'inherit'] });

let buffer = '';

function flushBufferLines() {
  const lines = buffer.split(/\r?\n/);
  // keep last partial line in buffer
  buffer = lines.pop();
  for (const line of lines) {
    if (!line) continue;
    try {
      const feat = JSON.parse(line);
      processFeature(feat);
    } catch (e) {
      // ignore parse errors for a line, but log occasionally
      // console.warn('Failed to parse feature line (skipped)');
    }
  }
}

// map to store which tile files were touched (so we can convert them later)
const touchedTiles = new Set();

// process feature -> determine tiles -> append to tile ndjson file
function processFeature(feat) {
  // basic sanity
  if (!feat || feat.type !== 'Feature' || !feat.geometry) return;

  const props = feat.properties || {};
  // try to pick a robust ID
  const wayId = props.id || props.osm_id || props['@id'] || props.osm && props.osm.id || null;

  // collect tags (common osmium patterns put tags in properties.tags or directly as properties)
  const tags = props.tags || (() => {
    // collect string properties that look like tags
    const t = {};
    for (const k of Object.keys(props)) {
      if (k === 'id' || k === 'osm' || k === '@id' || k === 'type') continue;
      // include simple primitive properties
      const v = props[k];
      if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') t[k] = v;
    }
    return t;
  })();

  // extract maxspeed and highway tag
  const maxspeedRaw = (tags && (tags.maxspeed || tags['maxspeed'])) || null;
  const highwayTag = (tags && (tags.highway || tags['highway'])) || null;
  const mph = parseMaxspeedToMph(maxspeedRaw);

  // build coords array: flatten MultiLineString -> single array of [lat,lon] sequence (concatenate parts)
  const coords = [];
  const geom = feat.geometry;
  if (geom.type === 'LineString') {
    for (const pt of geom.coordinates) {
      // osmium/geojson coords are [lon, lat]
      coords.push([pt[1], pt[0]]);
    }
  } else if (geom.type === 'MultiLineString') {
    for (const line of geom.coordinates) {
      for (const pt of line) coords.push([pt[1], pt[0]]);
    }
  } else {
    // ignore other geometry types
    return;
  }

  if (!coords.length) return;

  // find unique tiles touched by any vertex
  const tilesTouched = new Set();
  for (const p of coords) {
    const lat = p[0], lon = p[1];
    if (!isFinite(lat) || !isFinite(lon)) continue;
    const [tx, ty] = lonLatToTileXY(lon, lat, ZOOM);
    tilesTouched.add(`${tx},${ty}`);
  }

  if (tilesTouched.size === 0) return;

  // compact feature object to store
  const small = {
    id: wayId || null,
    speed: (mph > 0 ? mph : null),
    highway: highwayTag || null,
    tags: { maxspeed: maxspeedRaw || null },
    coords: coords
  };

  const smallLine = JSON.stringify(small) + '\n';

  // append to each tile ndjson file
  for (const t of tilesTouched) {
    const [tx, ty] = t.split(',').map(Number);
    const dir = path.join(__dirname, 'out', 'tiles', String(ZOOM), String(tx));
    fs.mkdirSync(dir, { recursive: true });
    const ndPath = path.join(dir, `${ty}.ndjson`);
    fs.appendFileSync(ndPath, smallLine, 'utf8');
    touchedTiles.add(`${tx}/${ty}`);
  }
}

// handle streaming
osmium.stdout.on('data', (chunk) => {
  buffer += chunk.toString('utf8');
  // process by lines (keep partial line in buffer)
  flushBufferLines();
});

osmium.on('close', (code) => {
  // process remaining buffered line if any
  if (buffer && buffer.trim()) {
    try {
      const last = JSON.parse(buffer);
      processFeature(last);
    } catch (e) { /* ignore */ }
  }

  if (code !== 0) {
    console.error('osmium export exited with code', code);
    process.exit(4);
  }

  console.log('Stream complete. Converting NDJSON tile files -> final JSON per tile...');

  // walk touched tiles and convert .ndjson -> y.json arrays
  const touched = Array.from(touchedTiles);
  for (const t of touched) {
    const [tx, ty] = t.split('/');
    const ndPath = path.join(__dirname, 'out', 'tiles', String(ZOOM), tx, `${ty}.ndjson`);
    const outPath = path.join(__dirname, 'out', 'tiles', String(ZOOM), tx, `${ty}.json`);
    try {
      const data = fs.readFileSync(ndPath, 'utf8');
      const lines = data.split(/\r?\n/).filter(Boolean);
      const arr = lines.map(l => {
        try { return JSON.parse(l); } catch (e) { return null; }
      }).filter(Boolean);
      fs.writeFileSync(outPath, JSON.stringify({
        z: ZOOM,
        x: parseInt(tx, 10),
        y: parseInt(ty, 10),
        tile_bbox: null, // optional: you can compute bbox later if needed
        fetched_at: (new Date()).toISOString(),
        features: arr
      }, null, 2), 'utf8');
      // remove ndjson to save space
      try { fs.unlinkSync(ndPath); } catch (e) {}
    } catch (e) {
      console.warn('Failed to convert ndjson for tile', tx, ty, e && e.message ? e.message : e);
    }
  }

  // optional: remove filtered PBF to save disk
  try { fs.unlinkSync(filteredPbf); } catch (e) {}

  console.log('Done. Tiles written to out/tiles/' + ZOOM);
  console.log('Tile count (approx):', touched.length);
});
