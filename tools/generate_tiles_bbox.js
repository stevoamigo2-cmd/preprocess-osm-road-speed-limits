// tools/generate_tiles_bbox.js
// usage:
//   node tools/generate_tiles_bbox.js --bbox=minLon,minLat,maxLon,maxLat --zoom=13 --out=regions/tiles.json
// or just:
//   node tools/generate_tiles_bbox.js --bbox=-10.5,49.5,2.0,59.5 --zoom=13

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

function lonlatToTile(lon, lat, z) {
  const n = Math.pow(2, z);
  const x = Math.floor((lon + 180) / 360 * n);
  const latRad = lat * Math.PI / 180;
  const y = Math.floor((1 - Math.log(Math.tan(latRad) + 1/Math.cos(latRad)) / Math.PI) / 2 * n);
  return { x, y };
}

const args = parseArgs();
if (!args.bbox) {
  console.error('Missing --bbox=minLon,minLat,maxLon,maxLat');
  process.exit(2);
}
const zoom = args.zoom ? parseInt(args.zoom, 10) : 13;
const out = args.out || 'regions/tiles.json';

const [minLon, minLat, maxLon, maxLat] = args.bbox.split(',').map(Number);

// normalize
const lon1 = Math.min(minLon, maxLon);
const lon2 = Math.max(minLon, maxLon);
const lat1 = Math.min(minLat, maxLat);
const lat2 = Math.max(minLat, maxLat);

const t1 = lonlatToTile(lon1, lat2, zoom); // top-left
const t2 = lonlatToTile(lon2, lat1, zoom); // bottom-right

const xmin = Math.min(t1.x, t2.x);
const xmax = Math.max(t1.x, t2.x);
const ymin = Math.min(t1.y, t2.y);
const ymax = Math.max(t1.y, t2.y);

const count = (xmax - xmin + 1) * (ymax - ymin + 1);
console.log(`Zoom ${zoom} tile range x:${xmin}..${xmax} y:${ymin}..${ymax} => ${count} tiles`);

if (count > 200000) {
  console.warn('This is a large number of tiles. Consider using a route-buffer approach to limit tiles.');
}

const arr = [];
for (let x = xmin; x <= xmax; x++) {
  for (let y = ymin; y <= ymax; y++) {
    arr.push({ z: zoom, x: x, y: y });
  }
}

if (args.out) {
  fs.mkdirSync(path.dirname(out), { recursive: true });
  fs.writeFileSync(out, JSON.stringify(arr, null, 2));
  console.log(`Wrote ${out}`);
} else {
  console.log(JSON.stringify(arr));
}
