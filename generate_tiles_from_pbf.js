#!/usr/bin/env node
// generate_tiles_from_pbf.js
// Usage: node generate_tiles_from_pbf.js <pbf-file> [zoom]
// Produces: out/tiles/<zoom>/<x>/<y>.json with minimal feature objects.

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
    if (s.includes('mph')) return Math.round(parseFloat(s.replace('mph','').replace(/[^\d.]/g,'')));
    const n = parseFloat(s.replace(/[^\d.]/g,'')) || 0;
    return Math.round(n * 0.621371);
  } catch (e) { return -1; }
}

const OUT_DIR = path.join(__dirname, 'out', 'tiles', String(ZOOM));
fs.mkdirSync(OUT_DIR, { recursive: true });

const baseName = path.basename(PBF_FILE, path.extname(PBF_FILE));
const filteredPbf = path.join(__dirname, baseName + '.filtered.pbf');
const withNodesPbf = path.join(__dirname, baseName + '.filtered.withnodes.pbf');

function runSync(cmd, args, opts = {}) {
  const r = spawnSync(cmd, args, Object.assign({ stdio: 'pipe' }, opts));
  return { status: r.status, stdout: r.stdout ? r.stdout.toString() : '', stderr: r.stderr ? r.stderr.toString() : '' };
}

console.log('1) Filtering PBF to ways with highway or maxspeed tags (creates):', filteredPbf);

// Always filter without add-missing-nodes (modern osmium)
let res = runSync('osmium', ['tags-filter', PBF_FILE, 'w/highway', 'w/maxspeed', '-o', filteredPbf], { stdio: 'inherit' });
if (res.status !== 0) {
  console.error('Failed to create filtered PBF. Aborting.');
  process.exit(4);
}

// === Always reconstruct way geometry ===
console.log('2) Adding locations to ways ->', withNodesPbf);
res = runSync('osmium', ['add-locations-to-ways', filteredPbf, '-o', withNodesPbf], { stdio: 'inherit' });
if (res.status !== 0) {
  console.warn('osmium add-locations-to-ways failed. Exporting may lack coordinates.');
  var usePbfForExport = filteredPbf;
} else {
  var usePbfForExport = withNodesPbf;
}

console.log('3) Streaming export from osmium -> geojsonseq...');
let exportArgs = ['export', usePbfForExport, '-f', 'geojsonseq', '--with-nodes'];
const testRes = runSync('osmium', exportArgs);
if (testRes.status !== 0) {
  console.log('Using plain export without --with-nodes');
  exportArgs = ['export', usePbfForExport, '-f', 'geojsonseq'];
}

const osmium = spawn('osmium', exportArgs, { stdio: ['ignore', 'pipe', 'inherit'] });

let buffer = '';
let totalFeatures = 0, processedFeatures = 0, skippedNoGeom = 0, skippedBadGeom = 0;
const touchedTiles = new Set();
const sampleTiles = [];
let firstFeatureLogged = false;

function flushBufferLines() {
  const lines = buffer.split(/\r?\n/);
  buffer = lines.pop();
  for (const line of lines) {
    if (!line) continue;
    totalFeatures++;
    try {
      const feat = JSON.parse(line);
      if (!firstFeatureLogged) {
        try { fs.writeFileSync(path.join(__dirname, 'out', 'sample_first_feature.json'), JSON.stringify(feat, null, 2)); } catch (e){}
        firstFeatureLogged = true;
      }
      processFeature(feat);
    } catch(e) {}
  }
}

function processFeature(feat) {
  if (!feat || feat.type !== 'Feature' || !feat.geometry) { skippedNoGeom++; return; }

  const props = feat.properties || {};
  const tags = props.tags || props.tag || {};
  const maxspeedRaw = tags.maxspeed || props.maxspeed || null;
  const highwayTag = tags.highway || props.highway || null;
  const mph = parseMaxspeedToMph(maxspeedRaw);

  const geom = feat.geometry;
  const coords = [];
  try {
    if (geom.type === 'LineString') for (const pt of geom.coordinates) coords.push([pt[1], pt[0]]);
    else if (geom.type === 'MultiLineString') for (const line of geom.coordinates) for (const pt of line) coords.push([pt[1], pt[0]]);
    else if (geom.type === 'GeometryCollection' && Array.isArray(geom.geometries)) for (const g of geom.geometries) if (g.type === 'LineString') for (const pt of g.coordinates) coords.push([pt[1], pt[0]]);
    else { skippedBadGeom++; return; }
  } catch (e) { skippedBadGeom++; return; }

  if (!coords.length) { skippedNoGeom++; return; }

  const tilesTouched = new Set();
  for (const [lat, lon] of coords) {
    if (!isFinite(lat) || !isFinite(lon)) continue;
    const [tx, ty] = lonLatToTileXY(lon, lat, ZOOM);
    tilesTouched.add(`${tx},${ty}`);
  }

  if (tilesTouched.size === 0) { skippedNoGeom++; return; }

  const small = { id: props.id || props['@id'] || null, speed: (mph > 0 ? mph : null), highway: highwayTag || null, tags: { maxspeed: maxspeedRaw || null }, coords };
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

osmium.stdout.on('data', chunk => { buffer += chunk.toString('utf8'); flushBufferLines(); });

osmium.on('close', code => {
  if (buffer && buffer.trim()) try { processFeature(JSON.parse(buffer)); } catch(e) {}

  console.log('Stream finished. Totals:');
  console.log('  totalFeatures (streamed lines):', totalFeatures);
  console.log('  processedFeatures (written to tiles):', processedFeatures);
  console.log('  skippedNoGeom:', skippedNoGeom, 'skippedBadGeom:', skippedBadGeom);
  console.log('  touchedTiles count:', touchedTiles.size);

  if (touchedTiles.size === 0) console.warn('No tiles were touched. Likely missing node geometry.');

  for (const tkey of touchedTiles) {
    const [tx, ty] = tkey.split('/');
    const ndPath = path.join(__dirname, 'out', 'tiles', String(ZOOM), tx, `${ty}.ndjson`);
    const outPath = path.join(__dirname, 'out', 'tiles', String(ZOOM), tx, `${ty}.json`);
    try {
      const data = fs.readFileSync(ndPath, 'utf8');
      const arr = data.split(/\r?\n/).filter(Boolean).map(l => JSON.parse(l));
      fs.writeFileSync(outPath, JSON.stringify({ z: ZOOM, x: parseInt(tx,10), y: parseInt(ty,10), tile_bbox: null, fetched_at: new Date().toISOString(), features: arr }, null, 2), 'utf8');
      fs.unlinkSync(ndPath);
    } catch(e){ console.warn('Failed converting ndjson for', tkey, e && e.message); }
  }

 const summary = {
  timestamp: new Date().toISOString(),
  strategy: { triedAddMissingNodes, usePbfForExport, exportArgs: exportCmdArgs },
  totals: { totalFeatures, processedFeatures, skippedNoGeom, skippedBadGeom, touchedTiles: touchedTiles.size },
  sampleTiles,
};

fs.writeFileSync(path.join(__dirname, 'out', 'generate_summary.json'), JSON.stringify(summary, null, 2));
console.log('Wrote out/generate_summary.json');


  try { if (fs.existsSync(withNodesPbf)) fs.unlinkSync(withNodesPbf); } catch(e){}
  console.log('Done. Tiles in out/tiles/' + ZOOM);
});
