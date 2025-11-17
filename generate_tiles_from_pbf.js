/**
 * generate_tiles_from_pbf.js
 * Usage: node generate_tiles_from_pbf.js [zoom]
 * Requires: uk_highways_max.osm.pbf in local machine
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const ZOOM = process.argv[2] ? parseInt(process.argv[2],10) : 13;
const PBF_FILE = path.join(__dirname, 'uk_highways_max.osm.pbf');
const OUTPUT_DIR = path.join(__dirname, 'tiles');

if (!fs.existsSync(PBF_FILE)) {
  console.error('PBF file not found:', PBF_FILE);
  process.exit(1);
}

// convert PBF to JSON lines with osmium
const JSONL_FILE = path.join(__dirname, 'uk_highways_max.jsonl');
console.log('Converting PBF to JSON lines...');
execSync(`osmium export -o ${JSONL_FILE} ${PBF_FILE} --output-format=jsonl --with-tags "highway,maxspeed"`, { stdio: 'inherit' });

// helper: tile calculation
function lon2tile(lon, zoom) {
  return Math.floor((lon + 180) / 360 * Math.pow(2, zoom));
}

function lat2tile(lat, zoom) {
  const rad = lat * Math.PI / 180;
  return Math.floor((1 - Math.log(Math.tan(rad) + 1/Math.cos(rad)) / Math.PI) / 2 * Math.pow(2, zoom));
}

// read JSON lines and generate tiles
console.log('Generating tiles...');
const stream = fs.readFileSync(JSONL_FILE, 'utf8').split(/\r?\n/);

for (const line of stream) {
  if (!line.trim()) continue;
  let obj;
  try { obj = JSON.parse(line); } catch(e){ continue; }

  if (!obj || !obj.geometry || !obj.tags) continue;
  const highway = obj.tags.highway || null;
  if (!highway) continue;

  // bounding box of way
  const lats = obj.geometry.map(p => p.lat);
  const lons = obj.geometry.map(p => p.lon);
  const minLat = Math.min(...lats);
  const maxLat = Math.max(...lats);
  const minLon = Math.min(...lons);
  const maxLon = Math.max(...lons);

  // determine tiles
  const xStart = lon2tile(minLon, ZOOM);
  const xEnd = lon2tile(maxLon, ZOOM);
  const yStart = lat2tile(maxLat, ZOOM);
  const yEnd = lat2tile(minLat, ZOOM);

  for (let x = xStart; x <= xEnd; x++) {
    for (let y = yStart; y <= yEnd; y++) {
      const tileDir = path.join(OUTPUT_DIR, `${ZOOM}`, `${x}`);
      if (!fs.existsSync(tileDir)) fs.mkdirSync(tileDir, { recursive: true });
      const tilePath = path.join(tileDir, `${y}.json`);

      let features = [];
      if (fs.existsSync(tilePath)) {
        features = JSON.parse(fs.readFileSync(tilePath, 'utf8')).features || [];
      }

      features.push({
        id: obj.id,
        highway: obj.tags.highway || null,
        maxspeed: obj.tags.maxspeed || null,
        coords: obj.geometry.map(p => [p.lat, p.lon])
      });

      fs.writeFileSync(tilePath, JSON.stringify({ z: ZOOM, x, y, features }, null, 2));
    }
  }
}

console.log('Done! All tiles saved under tiles/', ZOOM);
