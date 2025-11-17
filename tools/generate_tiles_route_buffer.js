// tools/generate_tiles_route_buffer.js
// usage:
//   node tools/generate_tiles_route_buffer.js --points=points.json --zoom=13 --buffer=300 --out=regions/tiles.json
//
// where points.json is an array of [lat,lon] pairs or an object { "points": [[lat,lon], ...] }

const fs = require('fs');
const path = require('path');

function parseArgs() {
  const a = {};
  process.argv.slice(2).forEach(arg => {
    if (arg.startsWith('--')) {
      const kv = arg.slice(2).split('=');
      a[kv[0]] = kv[1] || true;
    }
  });
  return a;
}

// tile functions
function lonlatToTile(lon, lat, z) {
  const n = Math.pow(2, z);
  const x = (lon + 180) / 360 * n;
  const latRad = lat * Math.PI / 180;
  const y = (1 - Math.log(Math.tan(latRad) + 1/Math.cos(latRad)) / Math.PI) / 2 * n;
  return { x, y };
}
function tileToLonLat(x, y, z) {
  const n = Math.pow(2, z);
  const lon = x / n * 360 - 180;
  const lat = Math.atan(Math.sinh(Math.PI * (1 - 2 * y / n))) * 180 / Math.PI;
  return { lon, lat };
}

// approximate meters per tile at latitude for zoom
function metersPerTile(lat, z) {
  const earthCirc = 40075017; // equatorial circumference (m)
  const n = Math.pow(2, z);
  return Math.abs((earthCirc * Math.cos(lat * Math.PI/180)) / n);
}

const args = parseArgs();
if (!args.points) {
  console.error('Missing --points=points.json');
  process.exit(2);
}
const zoom = args.zoom ? parseInt(args.zoom, 10) : 13;
const bufferMeters = args.buffer ? parseFloat(args.buffer) : 300;
const out = args.out || 'regions/tiles.json';

const raw = JSON.parse(fs.readFileSync(args.points, 'utf8'));
const points = Array.isArray(raw) ? raw : (raw.points || raw.coordinates);

if (!points || !points.length) {
  console.error('No points found in points file');
  process.exit(2);
}

const tiles = new Set();

for (const p of points) {
  const lat = p[0], lon = p[1];
  const t = lonlatToTile(lon, lat, zoom);
  // meters per tile at this lat
  const mpt = metersPerTile(lat, zoom) / 1; // approx for full tile width
  const radiusTiles = Math.ceil(bufferMeters / mpt);

  const xmin = Math.floor(t.x) - radiusTiles;
  const xmax = Math.ceil(t.x) + radiusTiles;
  const ymin = Math.floor(t.y) - radiusTiles;
  const ymax = Math.ceil(t.y) + radiusTiles;

  for (let x = xmin; x <= xmax; x++) {
    for (let y = ymin; y <= ymax; y++) {
      tiles.add(`${zoom}/${x}/${y}`);
    }
  }
}

const outArr = Array.from(tiles).map(s => {
  const parts = s.split('/');
  return { z: parseInt(parts[0],10), x: parseInt(parts[1],10), y: parseInt(parts[2],10) };
});

console.log(`Generated ${outArr.length} tiles for route (zoom=${zoom} buffer=${bufferMeters}m)`);

fs.mkdirSync(path.dirname(out), { recursive: true });
fs.writeFileSync(out, JSON.stringify(outArr, null, 2));
console.log(`Wrote ${out}`);
