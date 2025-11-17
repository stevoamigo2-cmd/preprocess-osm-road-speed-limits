// overwrite generate_tiles_from_pbf.js
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

const OUT_DIR = path.join(__dirname, 'out', 'tiles', String(ZOOM));
fs.mkdirSync(OUT_DIR, { recursive: true });

const filteredPbf = path.join(__dirname, path.basename(PBF_FILE, path.extname(PBF_FILE)) + '.filtered.pbf');
const filteredPbfWithNodes = path.join(__dirname, path.basename(PBF_FILE, path.extname(PBF_FILE)) + '.filtered.withnodes.pbf');

// Helper: lon/lat -> tile XY
function lonLatToTileXY(lon, lat, z) {
  const xtile = Math.floor((lon + 180) / 360 * Math.pow(2, z));
  const latRad = lat * Math.PI / 180;
  const ytile = Math.floor((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2 * Math.pow(2, z));
  return [xtile, ytile];
}

// Helper: parse maxspeed to mph
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
      return Math.round(parseFloat(n) * 0.621371);
    } else {
      const n = s.replace(/[^\d.]/g,'');
      return Math.round(parseFloat(n) * 0.621371);
    }
  } catch (e) { return -1; }
}

// Step 1: Filter ways
console.log('1) Filtering PBF to ways with highway or maxspeed tags (creates):', filteredPbf);
let res = spawnSync('osmium', [
  'tags-filter',
  PBF_FILE,
  'w/highway',
  'w/maxspeed',
  '-o', filteredPbf
], { stdio: 'inherit' });
if (res.status !== 0) {
  console.error('osmium tags-filter failed');
  process.exit(3);
}

// Step 2: Add locations (reconstruct way-node geometry)
console.log('2) Reconstructing way-node geometry (add locations to ways) ->', filteredPbfWithNodes);
res = spawnSync('osmium', [
  'add-locations',
  filteredPbf,
  '-o', filteredPbfWithNodes
], { stdio: 'inherit' });
if (res.status !== 0) {
  console.error('osmium add-locations failed');
  process.exit(4);
}

// Step 3: Export to GeoJSONSeq and process tiles
console.log('3) Streaming export from osmium -> geojsonseq (processing features)...');
const osmium = spawn('osmium', ['export', filteredPbfWithNodes, '-f', 'geojsonseq'], { stdio: ['ignore', 'pipe', 'inherit'] });

let buffer = '';
let totalFeatures = 0, processedFeatures = 0, skippedNoGeom = 0, skippedBadGeom = 0;
const touchedTiles = new Set();
let firstFeatureLogged = false;
const sampleTiles = [];

function flushBufferLines() {
  const lines = buffer.split(/\r?\n/);
  buffer = lines.pop();
  for (const line of lines) {
    if (!line) continue;
    totalFeatures++;
    try {
      const feat = JSON.parse(line);
      if (!firstFeatureLogged) {
        fs.writeFileSync(path.join(__dirname, 'out', 'sample_first_feature.json'), JSON.stringify(feat, null, 2));
        console.log('WROTE out/sample_first_feature.json for inspection');
        firstFeatureLogged = true;
      }
      processFeature(feat);
    } catch (e) {}
  }
}

function processFeature(feat) {
  if (!feat || feat.type !== 'Feature' || !feat.geometry) { skippedNoGeom++; return; }

  const props = feat.properties || {};
  let tags = props.tags || props.tag || {};
  const maxspeedRaw = tags.maxspeed || props.maxspeed || null;
  const highwayTag = tags.highway || props.highway || null;
  const mph = parseMaxspeedToMph(maxspeedRaw);

  const coords = [];
  const geom = feat.geometry;
  try {
    if (geom.type === 'LineString') {
      for (const pt of geom.coordinates) coords.push([pt[1], pt[0]]);
    } else if (geom.type === 'MultiLineString') {
      for (const line of geom.coordinates) for (const pt of line) coords.push([pt[1], pt[0]]);
    } else if (geom.type === 'GeometryCollection' && Array.isArray(geom.geometries)) {
      for (const g of geom.geometries) if (g.type === 'LineString') for (const pt of g.coordinates) coords.push([pt[1], pt[0]]);
    } else {
      skippedBadGeom++;
      return;
    }
  } catch (e) { skippedBadGeom++; return; }

  if (!coords.length) { skippedNoGeom++; return; }

  const tilesTouched = new Set();
  for (const p of coords) {
    const lat = p[0], lon = p[1];
    if (!isFinite(lat) || !isFinite(lon)) continue;
    const [tx, ty] = lonLatToTileXY(lon, lat, ZOOM);
    tilesTouched.add(`${tx},${ty}`);
  }
  if (!tilesTouched.size) { skippedNoGeom++; return; }

  const small = {
    id: props.id || props['@id'] || null,
    speed: mph > 0 ? mph : null,
    highway: highwayTag || null,
    tags: { maxspeed: maxspeedRaw || null },
    coords
  };
  const smallLine = JSON.stringify(small) + '\n';

  for (const t of tilesTouched) {
    const [tx, ty] = t.split(',').map(Number);
    const dir = path.join(OUT_DIR, String(tx));
    fs.mkdirSync(dir, { recursive: true });
    fs.appendFileSync(path.join(dir, `${ty}.ndjson`), smallLine, 'utf8');
    touchedTiles.add(`${tx}/${ty}`);
    if (sampleTiles.length < 5) sampleTiles.push({ tx, ty });
  }

  processedFeatures++;
}

osmium.stdout.on('data', (chunk) => { buffer += chunk.toString('utf8'); flushBufferLines(); });

osmium.on('close', (code) => {
  if (buffer && buffer.trim()) {
    try { processFeature(JSON.parse(buffer)); } catch (e) {}
  }
  if (code !== 0) {
    console.error('osmium export exited with code', code);
    process.exit(5);
  }

  console.log('Stream finished. Totals:');
  console.log('  totalFeatures:', totalFeatures);
  console.log('  processedFeatures:', processedFeatures);
  console.log('  skippedNoGeom:', skippedNoGeom, 'skippedBadGeom:', skippedBadGeom);
  console.log('  touchedTiles count:', touchedTiles.size);

  // convert ndjson -> final JSON
  for (const tkey of Array.from(touchedTiles)) {
    const [tx, ty] = tkey.split('/');
    const ndPath = path.join(OUT_DIR, tx, `${ty}.ndjson`);
    const outPath = path.join(OUT_DIR, tx, `${ty}.json`);
    try {
      const arr = fs.readFileSync(ndPath, 'utf8').split(/\r?\n/).filter(Boolean).map(JSON.parse);
      fs.writeFileSync(outPath, JSON.stringify({
        z: ZOOM, x: parseInt(tx,10), y: parseInt(ty,10),
        tile_bbox: null, fetched_at: new Date().toISOString(),
        features: arr
      }, null, 2));
      fs.unlinkSync(ndPath);
    } catch (e) { console.warn('Failed converting ndjson for', tkey, e.message); }
  }

  // write summary
  fs.writeFileSync(path.join(__dirname, 'out', 'generate_summary.json'), JSON.stringify({
    timestamp: new Date().toISOString(),
    totals: { totalFeatures, processedFeatures, skippedNoGeom, skippedBadGeom, touchedTiles: touchedTiles.size },
    sampleTiles
  }, null, 2));
  console.log('Wrote out/generate_summary.json');

  try { fs.unlinkSync(filteredPbf); } catch(e) {}
  try { fs.unlinkSync(filteredPbfWithNodes); } catch(e) {}
  console.log('Done. Tiles in out/tiles/' + ZOOM);
});
