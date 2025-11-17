/**
 * preprocess_osm.js (Batch + Retry + Incremental Save)
 * Usage: node preprocess_osm.js [zoom]
 * Expects: regions/tiles.json -> array of { z:int, x:int, y:int }
 * Env:
 *   OVERPASS_ENDPOINT (default https://overpass-api.de/api/interpreter)
 *   THROTTLE_MS (default 1200)
 *   RETRY_COUNT (default 3)
 */

const fs = require('fs');
const fetch = require('node-fetch');
const path = require('path');

const OVERPASS = process.env.OVERPASS_ENDPOINT || 'https://overpass-api.de/api/interpreter';
const TILE_Z = process.argv[2] ? parseInt(process.argv[2], 10) : 13;
const THROTTLE_MS = parseInt(process.env.THROTTLE_MS || '2000', 10);
const RETRY_COUNT = parseInt(process.env.RETRY_COUNT || '3', 10);

console.log(`Generating tiles for zoom: ${TILE_Z}`);
console.log(`Throttle: ${THROTTLE_MS}ms, Retries: ${RETRY_COUNT}`);

// Read tiles list
const tilesFile = path.join(__dirname, 'regions', 'tiles.json');
if (!fs.existsSync(tilesFile)) {
  console.error('regions/tiles.json not found. Create an array like [{ "z":15,"x":17500,"y":11100 }, ...]');
  process.exit(1);
}
let tiles = JSON.parse(fs.readFileSync(tilesFile, 'utf8'));
tiles.forEach(tile => tile.z = TILE_Z);

// Tile -> bbox
function tile2bbox(x, y, z) {
  const n = Math.pow(2, z);
  const lon_left = x / n * 360 - 180;
  const lon_right = (x + 1) / n * 360 - 180;
  const lat_top = Math.atan(Math.sinh(Math.PI * (1 - 2 * y / n))) * 180 / Math.PI;
  const lat_bottom = Math.atan(Math.sinh(Math.PI * (1 - 2 * (y+1) / n))) * 180 / Math.PI;
  return [lat_bottom, lon_left, lat_top, lon_right];
}

function parseMaxspeedToMph(raw) {
  if (!raw) return -1;
  const s = raw.trim().toLowerCase();
  if (!s || s === 'none') return -1;
  try {
    if (s.includes('mph')) return Math.round(parseFloat(s.replace('mph','').replace(/[^\d.]/g,'')));
    const kmh = parseFloat(s.replace(/[^\d.]/g,''));
    return Math.round(kmh * 0.621371);
  } catch(e){ return -1; }
}

function inferSpeedFromHighway(highway) {
  if (!highway) return -1;
  const h = highway.toLowerCase();
  switch(h){
    case 'motorway': return 70;
    case 'trunk': return 60;
    case 'primary': return 50;
    case 'secondary': return 40;
    case 'tertiary': return 30;
    case 'unclassified': case 'residential': return 30;
    case 'service': return 10;
    default: return -1;
  }
}

async function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

async function fetchTile(tile){
  const { z, x, y } = tile;
  const [s, w, n, e] = tile2bbox(x, y, z);
  const queries = [
    `[out:json][timeout:25];(way["highway"]["maxspeed"](${s},${w},${n},${e}););out tags geom;`,
    `[out:json][timeout:25];(way["highway"](${s},${w},${n},${e}););out tags geom;`
  ];

  for (let attempt = 1; attempt <= RETRY_COUNT; attempt++){
    for (const q of queries){
      try {
        const url = `${OVERPASS}?data=${encodeURIComponent(q)}`;
        const res = await fetch(url, { headers: { 'User-Agent': 'preprocess-osm/1.0 (github actions)' } });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json = await res.json();
        if (!json.elements || json.elements.length === 0) continue;

        const features = json.elements.filter(el => el.type === 'way').map(el => {
          const tags = el.tags || {};
          const coords = Array.isArray(el.geometry) ? el.geometry.map(p => [p.lat, p.lon]) : [];
          const mph = tags.maxspeed ? parseMaxspeedToMph(tags.maxspeed) : inferSpeedFromHighway(tags.highway);
          return {
            id: el.id,
            speed: mph > 0 ? mph : null,
            highway: tags.highway || null,
            tags: { maxspeed: tags.maxspeed || null },
            coords
          };
        });

        return { z, x, y, tile_bbox:{ south:s, west:w, north:n, east:e }, fetched_at:new Date().toISOString(), features };
      } catch(e){
        console.warn(`Attempt ${attempt} failed for tile z${z} x${x} y${y}: ${e.message}`);
        await sleep(THROTTLE_MS * attempt); // exponential backoff
        continue;
      }
    }
  }

  return { z, x, y, tile_bbox:{ south:s, west:w, north:n, east:e }, fetched_at:new Date().toISOString(), features: [] };
}

async function saveTileJsonOut(z,x,y,obj){
  const dir = path.join(__dirname,'out','tiles',`${z}`,`${x}`);
  const filePath = path.join(dir,`${y}.json`);
  fs.mkdirSync(dir,{ recursive:true });
  fs.writeFileSync(filePath, JSON.stringify(obj,null,2));
  return filePath;
}

(async ()=>{
  console.log('Total tiles:', tiles.length);
  if (!tiles.length){ console.log('No tiles defined - exiting.'); process.exit(0); }

  const batchSize = 1000;
  for (let b=0; b < Math.ceil(tiles.length/batchSize); b++){
    const batch = tiles.slice(b*batchSize,(b+1)*batchSize);
    console.log(`Processing batch ${b+1} / ${Math.ceil(tiles.length/batchSize)} (${batch.length} tiles)`);

    for (let i=0;i<batch.length;i++){
      const t = batch[i];
      console.log(`Processing (${i+1}/${batch.length}) z${t.z} x${t.x} y${t.y} ...`);
      try{
        const obj = await fetchTile(t);
        const filePath = await saveTileJsonOut(t.z,t.x,t.y,obj);
        console.log('Saved tile', filePath, 'features=', obj.features.length);
      }catch(err){
        console.error('Failed processing tile', t, err.stack || err);
      }
      await sleep(THROTTLE_MS);
    }

    // Incremental save of batch progress
    const progressFile = path.join(__dirname,'out','tiles','progress.json');
    fs.writeFileSync(progressFile, JSON.stringify({ lastBatch: b+1, timestamp: new Date().toISOString() }, null, 2));
    console.log(`Batch ${b+1} completed, progress saved to ${progressFile}`);
  }

  console.log('All tiles done.');
})();
