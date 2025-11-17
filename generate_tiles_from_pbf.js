// generate_tiles_from_pbf.js - more defensive geometry extraction + debug
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

function lonLatToTileXY(lon, lat, z) {
  const xtile = Math.floor((lon + 180) / 360 * Math.pow(2, z));
  const latRad = lat * Math.PI / 180;
  const ytile = Math.floor((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * Math.pow(2, z));
  return [xtile, ytile];
}
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
  } catch (e) { return -1; }
}

const OUT_DIR = path.join(__dirname, 'out', 'tiles', String(ZOOM));
fs.mkdirSync(OUT_DIR, { recursive: true });

const baseName = path.basename(PBF_FILE, path.extname(PBF_FILE));
const filteredPbf = path.join(__dirname, baseName + '.filtered.pbf');
const withNodesPbf = path.join(__dirname, baseName + '.filtered.withnodes.pbf');

console.log('1) Filtering PBF to ways with highway or maxspeed tags (creates):', filteredPbf);
// tags-filter -> produce filteredPbf (ways only)
try {
  const res = spawnSync('osmium', [
    'tags-filter',
    PBF_FILE,
    'w/highway',
    'w/maxspeed',
    '-o',
    filteredPbf
  ], { stdio: 'inherit' });

  if (res.error) throw res.error;
  if (res.status !== 0) throw new Error('osmium tags-filter failed with status ' + res.status);
} catch (err) {
  console.error('Failed to run osmium tags-filter. Ensure osmium-tool installed and on PATH.');
  console.error(err && err.message ? err.message : err);
  process.exit(3);
}

console.log('2) Reconstructing way-node geometry (add locations to ways) ->', withNodesPbf);
// add-locations-to-ways to ensure ways have node coordinates
try {
  const res2 = spawnSync('osmium', [
    'add-locations-to-ways',
    filteredPbf,
    '-o',
    withNodesPbf
  ], { stdio: 'inherit' });

  if (res2.error) throw res2.error;
  if (res2.status !== 0) throw new Error('osmium add-locations-to-ways failed with status ' + res2.status);
} catch (err) {
  console.error('Failed to run osmium add-locations-to-ways. Ensure osmium-tool installed and on PATH.');
  console.error(err && err.message ? err.message : err);
  process.exit(4);
}

console.log('3) Streaming export from osmium -> geojsonseq (processing features)...');
// export geojsonseq
const osmium = spawn('osmium', ['export', withNodesPbf, '-f', 'geojsonseq'], { stdio: ['ignore', 'pipe', 'inherit'] });

let buffer = '';
let totalFeatures = 0, processedFeatures = 0, skippedNoGeom = 0, skippedBadGeom = 0;
let firstFeatureLogged = false;
const touchedTiles = new Set();
const sampleTiles = [];

function safeParseJSON(str) {
  try { return JSON.parse(str); } catch (e) { return null; }
}

function coordsFromPropsNodes(props) {
  // try common props that may contain coordinates: nodes, coordinates, geometry (string)
  if (!props) return null;
  // 1) nodes array with lat/lon objects
  if (Array.isArray(props.nodes) && props.nodes.length) {
    const out = [];
    for (const n of props.nodes) {
      if (n && typeof n.lat === 'number' && typeof n.lon === 'number') out.push([n.lat, n.lon]);
      else if (Array.isArray(n) && n.length >= 2) out.push([n[0], n[1]]);
    }
    if (out.length) return out;
  }
  // 2) geometry stringified JSON
  if (typeof props.geometry === 'string' && props.geometry.trim().startsWith('{')) {
    const parsed = safeParseJSON(props.geometry);
    if (parsed && parsed.type && parsed.coordinates) {
      // parse same as geometry block below
      return coordsFromGeo(parsed);
    }
  }
  // 3) coordinates field (array)
  if (Array.isArray(props.coordinates) && props.coordinates.length) {
    const out = [];
    for (const pt of props.coordinates) {
      if (Array.isArray(pt) && pt.length >= 2) {
        // assume [lon,lat]
        out.push([pt[1], pt[0]]);
      }
    }
    if (out.length) return out;
  }
  return null;
}

function coordsFromGeo(geom) {
  try {
    if (!geom || !geom.type) return null;
    if (geom.type === 'LineString' && Array.isArray(geom.coordinates)) {
      return geom.coordinates.map(pt => [pt[1], pt[0]]).filter(p => Number.isFinite(p[0]) && Number.isFinite(p[1]));
    } else if (geom.type === 'MultiLineString' && Array.isArray(geom.coordinates)) {
      const out = [];
      for (const line of geom.coordinates) {
        for (const pt of line) {
          if (Array.isArray(pt) && pt.length >= 2) out.push([pt[1], pt[0]]);
        }
      }
      return out.filter(p => Number.isFinite(p[0]) && Number.isFinite(p[1]));
    } else if (geom.type === 'GeometryCollection' && Array.isArray(geom.geometries)) {
      const out = [];
      for (const g of geom.geometries) {
        const sub = coordsFromGeo(g);
        if (sub) out.push(...sub);
      }
      return out;
    } else if (geom.type === 'Point' && Array.isArray(geom.coordinates)) {
      return [[geom.coordinates[1], geom.coordinates[0]]];
    }
  } catch (e) {
    return null;
  }
  return null;
}

function extractCoords(feat) {
  if (!feat) return null;
  // Prefer explicit geometry property
  if (feat.geometry) {
    const got = coordsFromGeo(feat.geometry);
    if (got && got.length) return got;
  }
  // Try properties nodes / geometry
  if (feat.properties) {
    const got = coordsFromPropsNodes(feat.properties);
    if (got && got.length) return got;
  }
  // As last resort: try to parse the whole feature for common shapes
  const fstr = JSON.stringify(feat);
  // try to find "coordinates": [...] quickly - but avoid heavy parsing here
  // (we'll skip that optimization — rely on previous methods)
  return null;
}

function flushBufferLines() {
  const lines = buffer.split(/\r?\n/);
  buffer = lines.pop();
  for (const line of lines) {
    if (!line) continue;
    totalFeatures++;
    const feat = safeParseJSON(line);
    if (!feat) { skippedBadGeom++; continue; }
    if (!firstFeatureLogged) {
      try {
        fs.mkdirSync(path.join(__dirname, 'out'), { recursive: true });
        fs.writeFileSync(path.join(__dirname, 'out', 'sample_first_feature.json'), JSON.stringify(feat, null, 2));
        console.log('WROTE out/sample_first_feature.json for inspection');
      } catch (e) { /* ignore */ }
      firstFeatureLogged = true;
    }
    processFeature(feat);
  }
}

function processFeature(feat) {
  // require Feature
  if (!feat || feat.type !== 'Feature') { skippedBadGeom++; return; }

  const tagsFromProps = feat.properties || {};
  // tags may be nested in properties.tags or similar
  let tags = tagsFromProps.tags || tagsFromProps.tag || null;
  if (!tags) {
    // flatten typical tag-like strings present as properties
    tags = {};
    for (const k of Object.keys(tagsFromProps)) {
      if (['id','@id','type','timestamp','version'].includes(k)) continue;
      const v = tagsFromProps[k];
      if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') tags[k] = v;
    }
    if (Object.keys(tags).length === 0) tags = null;
  }

  const maxspeedRaw = (tags && (tags.maxspeed || tags['maxspeed'])) ? (tags.maxspeed || tags['maxspeed']) : (tagsFromProps.maxspeed || tagsFromProps['maxspeed'] || null);
  const highwayTag = (tags && (tags.highway || tags['highway'])) ? (tags.highway || tags['highway']) : (tagsFromProps.highway || tagsFromProps['highway'] || null);
  const mph = parseMaxspeedToMph(maxspeedRaw);

  // extract coordinates
  const coords = extractCoords(feat);
  if (!coords || !coords.length) { skippedBadGeom++; return; }

  // dedupe and ensure numeric
  const cleanCoords = coords.filter(p => Array.isArray(p) && p.length >= 2 && isFinite(p[0]) && isFinite(p[1]));

  if (!cleanCoords.length) { skippedNoGeom++; return; }

  // get unique tiles for this feature
  const tilesTouched = new Set();
  for (const p of cleanCoords) {
    const lat = p[0], lon = p[1];
    const [tx, ty] = lonLatToTileXY(lon, lat, ZOOM);
    tilesTouched.add(`${tx},${ty}`);
  }
  if (tilesTouched.size === 0) { skippedNoGeom++; return; }

  const small = {
    id: tagsFromProps.id || tagsFromProps['@id'] || (tagsFromProps.osm && tagsFromProps.osm.id) || null,
    speed: (mph > 0 ? mph : null),
    highway: highwayTag || null,
    tags: { maxspeed: maxspeedRaw || null },
    coords: cleanCoords
  };
  const smallLine = JSON.stringify(small) + '\n';

  for (const t of tilesTouched) {
    const [tx, ty] = t.split(',').map(Number);
    const dir = path.join(__dirname, 'out', 'tiles', String(ZOOM), String(tx));
    fs.mkdirSync(dir, { recursive: true });
    const ndPath = path.join(dir, `${ty}.ndjson`);
    fs.appendFileSync(ndPath, smallLine, 'utf8');
    touchedTiles.add(`${tx}/${ty}`);
    if (sampleTiles.length < 5) sampleTiles.push({ tx, ty });
  }

  processedFeatures++;
}

osmium.stdout.on('data', (chunk) => {
  buffer += chunk.toString('utf8');
  // periodically flush to avoid unbounded memory use
  if (buffer.length > (1 << 22)) { // ~4MiB chunk
    flushBufferLines();
  } else {
    flushBufferLines();
  }
});

osmium.on('close', (code) => {
  if (buffer && buffer.trim()) {
    const remaining = buffer.split(/\r?\n/).filter(Boolean);
    for (const line of remaining) {
      const feat = safeParseJSON(line);
      if (feat) processFeature(feat);
    }
  }

  if (code !== 0) {
    console.error('osmium export exited with code', code);
    process.exit(4);
  }

  console.log('Stream finished. Totals:');
  console.log('  totalFeatures (streamed lines):', totalFeatures);
  console.log('  processedFeatures (written to tiles):', processedFeatures);
  console.log('  skippedNoGeom:', skippedNoGeom, 'skippedBadGeom:', skippedBadGeom);
  console.log('  touchedTiles count:', touchedTiles.size);

  if (touchedTiles.size === 0) {
    console.warn('No tiles were touched. Possible causes: filtered PBF has no way geometries OR feature geometry/tags are in an unexpected shape.');
    try {
      const stat = fs.statSync(filteredPbf);
      console.log('Filtered PBF size:', (stat.size / (1024*1024)).toFixed(1), 'MB');
    } catch (e) {}
  }

  // convert ndjson -> final JSON files
  for (const tkey of Array.from(touchedTiles)) {
    const [tx, ty] = tkey.split('/');
    const ndPath = path.join(__dirname, 'out', 'tiles', String(ZOOM), tx, `${ty}.ndjson`);
    const outPath = path.join(__dirname, 'out', 'tiles', String(ZOOM), tx, `${ty}.json`);
    try {
      const data = fs.readFileSync(ndPath, 'utf8');
      const lines = data.split(/\r?\n/).filter(Boolean);
      const arr = lines.map(l => JSON.parse(l));
      fs.writeFileSync(outPath, JSON.stringify({
        z: ZOOM,
        x: parseInt(tx, 10),
        y: parseInt(ty, 10),
        tile_bbox: null,
        fetched_at: (new Date()).toISOString(),
        features: arr
      }, null, 2), 'utf8');
      fs.unlinkSync(ndPath);
    } catch (e) {
      console.warn('Failed converting ndjson for', tkey, e && e.message);
    }
  }

  // write debug summary
  const summary = {
    timestamp: new Date().toISOString(),
    totals: { totalFeatures, processedFeatures, skippedNoGeom, skippedBadGeom, touchedTiles: touchedTiles.size },
    sampleTiles,
  };
  try {
    fs.mkdirSync(path.join(__dirname, 'out'), { recursive: true });
    fs.writeFileSync(path.join(__dirname, 'out', 'generate_summary.json'), JSON.stringify(summary, null, 2));
    console.log('Wrote out/generate_summary.json');
  } catch (e) {}
  // cleanup
  try { fs.unlinkSync(filteredPbf); } catch (e) {}
  try { fs.unlinkSync(withNodesPbf); } catch (e) {}
  console.log('Done. Tiles in out/tiles/' + ZOOM);
});
