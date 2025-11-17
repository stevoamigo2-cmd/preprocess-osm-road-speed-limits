/**
 * generate_tiles_from_pbf.js
 *
 * Usage: node generate_tiles_from_pbf.js [zoom]
 * Requires:
 *   - uk_highways_max.osm.pbf (filtered with highway & maxspeed)
 *   - npm install osmium-tool archiver fs path
 *
 * Output:
 *   - tiles/<zoom>/<x>/<y>.json
 *   - tiles_z<zoom>.zip
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const archiver = require('archiver');

const ZOOM = parseInt(process.argv[2], 10) || 13;
const PBF_FILE = path.join(__dirname, 'uk_highways_max.osm.pbf');
const TILE_DIR = path.join(__dirname, 'tiles', `${ZOOM}`);

// Create tiles folder
fs.mkdirSync(TILE_DIR, { recursive: true });

console.log(`Generating tiles for zoom: ${ZOOM}`);

// Helper: convert lat/lon to tile x/y
function lon2tile(lon, zoom) {
  return Math.floor(((lon + 180) / 360) * Math.pow(2, zoom));
}

function lat2tile(lat, zoom) {
  const rad = lat * Math.PI / 180;
  return Math.floor(
    ((1 - Math.log(Math.tan(rad) + 1 / Math.cos(rad)) / Math.PI) / 2) * Math.pow(2, zoom)
  );
}

// Parse maxspeed string to MPH
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

// Infer speed if missing
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

// Run osmium to convert ways to GeoJSON line features
console.log('Extracting ways to GeoJSON...');
const geojsonFile = path.join(__dirname, 'uk_highways_max.geojson');
execSync(`osmium export ${PBF_FILE} -o ${geojsonFile} --geometry-types lines --tags highway,maxspeed`, { stdio: 'inherit' });

// Read GeoJSON
const geojson = JSON.parse(fs.readFileSync(geojsonFile, 'utf8'));
console.log(`Features loaded: ${geojson.features.length}`);

// Generate tiles
for (const feat of geojson.features) {
  if (!feat.geometry || !feat.geometry.coordinates) continue;

  // Determine which tiles this feature belongs to
  const coords = feat.geometry.coordinates; // [[lon, lat], ...]
  const tiles = new Set();

  for (const [lon, lat] of coords) {
    const x = lon2tile(lon, ZOOM);
    const y = lat2tile(lat, ZOOM);
    tiles.add(`${x},${y}`);
  }

  // Save feature in each tile it intersects
  for (const tile of tiles) {
    const [x, y] = tile.split(',').map(Number);
    const dir = path.join(TILE_DIR, `${x}`);
    fs.mkdirSync(dir, { recursive: true });
    const filePath = path.join(dir, `${y}.json`);

    let tileData = [];
    if (fs.existsSync(filePath)) {
      tileData = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    }

    const tags = feat.properties || {};
    let mph = tags.maxspeed ? parseMaxspeedToMph(tags.maxspeed) : inferSpeedFromHighway(tags.highway);

    tileData.push({
      id: feat.id,
      speed: mph > 0 ? mph : null,
      highway: tags.highway || null,
      tags: { maxspeed: tags.maxspeed || null },
      coords: coords.map(([lon, lat]) => [lat, lon])
    });

    fs.writeFileSync(filePath, JSON.stringify(tileData));
  }
}

// Zip tiles
const zipFile = path.join(__dirname, `tiles_z${ZOOM}.zip`);
const output = fs.createWriteStream(zipFile);
const archive = archiver('zip', { zlib: { level: 9 } });

output.on('close', () => {
  console.log(`Tiles zipped: ${zipFile} (${archive.pointer()} bytes)`);
});

archive.on('error', err => { throw err; });
archive.pipe(output);
archive.directory(TILE_DIR, false);
archive.finalize();

console.log('All done!');
