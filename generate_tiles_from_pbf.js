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

const filteredPbf = path.join(__dirname, path.basename(PBF_FILE, path.extname(PBF_FILE)) + '.filtered.pbf');
const withNodesPbf = path.join(__dirname, path.basename(PBF_FILE, path.extname(PBF_FILE)) + '.filtered.withnodes.pbf');

function runSync(cmd, args, opts = {}) {
  const r = spawnSync(cmd, args, Object.assign({ stdio: 'pipe' }, opts));
  return { status: r.status, stdout: r.stdout ? r.stdout.toString() : '', stderr: r.stderr ? r.stderr.toString() : '' };
}

console.log('1) Filtering PBF to ways with highway or maxspeed tags (creates):', filteredPbf);

let triedAddMissingNodes = false;
let didFilter = false;
let res;

res = runSync('osmium', ['--version']);
if (res.status !== 0) console.warn('Warning: osmium-tool not found or not on PATH.');

try {
  console.log(' -> attempt: osmium tags-filter ... --add-missing-nodes');
  triedAddMissingNodes = true;
  res = runSync('osmium', [
    'tags-filter',
    PBF_FILE,
    'w/highway',
    'w/maxspeed',
    '-o',
    filteredPbf,
    '--add-missing-nodes'
  ], { stdio: 'inherit' });
  if (res.status === 0) { didFilter = true; console.log('tags-filter + --add-missing-nodes succeeded.'); }
  else console.log('tags-filter + --add-missing-nodes failed (status ' + res.status + '), stderr:\n' + res.stderr);
} catch (e) { console.log('tags-filter + --add-missing-nodes attempt raised:', e && e.message); }

if (!didFilter) {
  try {
    console.log(' -> attempt: osmium tags-filter (without add-missing-nodes)');
    res = runSync('osmium', [
      'tags-filter',
      PBF_FILE,
      'w/highway',
      'w/maxspeed',
      '-o',
      filteredPbf
    ], { stdio: 'inherit' });
    if (res.status === 0) { didFilter = true; console.log('tags-filter succeeded (no add-missing-nodes).'); }
    else console.log('tags-filter failed (status ' + res.status + '), stderr:\n' + res.stderr);
  } catch (e) { console.log('tags-filter attempt raised:', e && e.message); }
}

if (!didFilter) {
  console.error('Failed to create filtered PBF via osmium tags-filter. Aborting.');
  process.exit(4);
}

// === PATCHED: Use add-locations-to-ways instead of old add-locations ===
let usePbfForExport = filteredPbf;
if (!triedAddMissingNodes || !didFilter) {
  // fallback: try add-locations-to-ways to reconstruct geometry
  console.log('2) Attempting to reconstruct way geometry with osmium add-locations-to-ways ->', withNodesPbf);
  res = runSync('osmium', ['add-locations-to-ways', filteredPbf, '-o', withNodesPbf]);
  if (res.status === 0) {
    console.log('osmium add-locations-to-ways succeeded.');
    usePbfForExport = withNodesPbf;
  } else {
    console.log('osmium add-locations-to-ways unavailable/failed (status ' + res.status + '), stderr:\n' + res.stderr);
    usePbfForExport = filteredPbf; // fallback
  }
}

console.log('3) Streaming export from osmium -> geojsonseq (processing features)...');

const exportArgsPrimary = ['export', usePbfForExport, '-f', 'geojsonseq'];
const exportArgsWithNodes = exportArgsPrimary.concat(['--with-nodes']); 

let exportCmdArgs = exportArgsPrimary;
let testRes = runSync('osmium', exportArgsWithNodes);
if (testRes.status === 0) {
  exportCmdArgs = exportArgsWithNodes;
  console.log('Using osmium export with --with-nodes.');
} else console.log('Using plain osmium export (no --with-nodes).');

const osmium = spawn('osmium', exportCmdArgs, { stdio: ['ignore', 'pipe', 'inherit'] });

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
    } catch (e) {}
  }
}

function processFeature(feat) {
  if (!feat || feat.type !== 'Feature' || !feat.geometry) { skippedNoGeom++; return; }

  const props = feat.properties || {};
  let tags = props.tags || props.tag || null;
  if (!tags) {
    tags = {};
    for (const k of Object.keys(props)) {
      if (['id','@id','osm','type','timestamp','version'].includes(k)) continue;
      const v = props[k];
      if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') tags[k] = v;
    }
    if (Object.keys(tags).length === 0) tags = null;
  }

  const maxspeedRaw = tags && (tags.maxspeed || tags['maxspeed']) ? (tags.maxspeed || tags['maxspeed']) : (props.maxspeed || props['maxspeed'] || null);
  const highwayTag = tags && (tags.highway || tags['highway']) ? (tags.highway || tags['highway']) : (props.highway || props['highway'] || null);
  const mph = parseMaxspeedToMph(maxspeedRaw);

  const geom = feat.geometry;
  const coords = [];

  try {
    if (geom.type === 'LineString') for (const pt of geom.coordinates) coords.push([pt[1], pt[0]]);
    else if (geom.type === 'MultiLineString') for (const line of geom.coordinates) for (const pt of line) coords.push([pt[1], pt[0]]);
    else if (geom.type === 'GeometryCollection' && Array.isArray(geom.geometries)) for (const g of geom.geometries) if (g && g.type === 'LineString') for (const pt of g.coordinates) coords.push([pt[1], pt[0]]);
    else { skippedBadGeom++; return; }
  } catch (e) { skippedBadGeom++; return; }

  if (!coords.length) { skippedNoGeom++; return; }

  const tilesTouched = new Set();
  for (const p of coords) {
    const lat = p[0], lon = p[1];
    if (!isFinite(lat) || !isFinite(lon)) continue;
    const [tx, ty] = lonLatToTileXY(lon, lat, ZOOM);
    tilesTouched.add(`${tx},${ty}`);
  }
  if (tilesTouched.size === 0) { skippedNoGeom++; return; }

  const small = {
    id: props.id || props['@id'] || (props.osm && props.osm.id) || null,
    speed: (mph > 0 ? mph : null),
    highway: highwayTag || null,
    tags: { maxspeed: maxspeedRaw || null },
    coords
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

osmium.stdout.on('data', (chunk) => { buffer += chunk.toString('utf8'); flushBufferLines(); });

osmium.on('close', (code) => {
  if (buffer && buffer.trim()) {
    try { processFeature(JSON.parse(buffer)); } catch (e) {}
  }

  console.log('Stream finished. Totals:');
  console.log('  totalFeatures (streamed lines):', totalFeatures);
  console.log('  processedFeatures (written to tiles):', processedFeatures);
  console.log('  skippedNoGeom:', skippedNoGeom, 'skippedBadGeom:', skippedBadGeom);
  console.log('  touchedTiles count:', touchedTiles.size);

  if (touchedTiles.size === 0) console.warn('No tiles were touched. Filtered PBF may have missing node geometry.');

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
    } catch (e) { console.warn('Failed converting ndjson for', tkey, e && e.message); }
  }

  const summary = {
    timestamp: new Date().toISOString(),
    strategy: { triedAddMissingNodes, usedPbfForExport: usePbfForExport, exportArgs: exportCmdArgs },
    totals: { totalFeatures, processedFeatures, skippedNoGeom, skippedBadGeom, touchedTiles: touchedTiles.size },
    sampleTiles,
  };
  fs.writeFileSync(path.join(__dirname, 'out', 'generate_summary.json'), JSON.stringify(summary, null, 2));
  console.log('Wrote out/generate_summary.json');

  try { if (fs.existsSync(withNodesPbf)) fs.unlinkSync(withNodesPbf); } catch(e){}
  console.log('Done. Tiles in out/tiles/' + ZOOM);
});
