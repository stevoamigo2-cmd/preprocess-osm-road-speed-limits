/**
 * preprocess_osm.js (batch-friendly)
 * - usage: node preprocess_osm.js [zoom]
 * - expects: regions/tiles.json OR regions/tiles_batch.json
 * - env:
 *   * OVERPASS_ENDPOINT (default https://overpass-api.de/api/interpreter)
 *   * THROTTLE_MS
 */

const fs = require('fs');
const fetch = require('node-fetch');
const path = require('path');

const OVERPASS = process.env.OVERPASS_ENDPOINT || 'https://overpass-api.de/api/interpreter';
const TILE_Z = process.argv[2] ? parseInt(process.argv[2], 10) : 13;
const THROTTLE_MS = parseInt(process.env.THROTTLE_MS || '1200', 10);

console.log("Generating tiles for zoom:", TILE_Z);

// Prefer batch file if exists
let tilesFile = path.join(__dirname, 'regions', 'tiles_batch.json');
if (!fs.existsSync(tilesFile)) {
    tilesFile = path.join(__dirname, 'regions', 'tiles.json');
    if (!fs.existsSync(tilesFile)) {
        console.error('No tiles file found! Create regions/tiles.json as an array of {z,x,y}.');
        process.exit(1);
    }
}

let tiles = JSON.parse(fs.readFileSync(tilesFile, 'utf8'));
tiles.forEach(tile => tile.z = TILE_Z);

console.log('Tiles to process:', tiles.length);

// Tile -> bbox conversion
function tile2bbox(x, y, z) {
    const n = Math.pow(2, z);
    const lon_left = x / n * 360 - 180;
    const lon_right = (x + 1) / n * 360 - 180;
    const lat_top = Math.atan(Math.sinh(Math.PI * (1 - 2 * y / n))) * 180 / Math.PI;
    const lat_bottom = Math.atan(Math.sinh(Math.PI * (1 - 2 * (y+1) / n))) * 180 / Math.PI;
    return [lat_bottom, lon_left, lat_top, lon_right];
}

// Speed parsing
function parseMaxspeedToMph(raw) {
    if (!raw) return -1;
    const s = raw.trim().toLowerCase();
    if (!s || s === 'none') return -1;
    try {
        if (s.includes('mph')) return Math.round(parseFloat(s.replace('mph','').replace(/[^\d.]/g,'')));
        const n = parseFloat(s.replace(/[^\d.]/g,''));
        return Math.round(n * 0.621371);
    } catch(e) { return -1; }
}

// Infer speed
function inferSpeedFromHighway(highway) {
    if (!highway) return -1;
    switch (highway.toLowerCase()) {
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

async function sleep(ms){ return new Promise(r=>setTimeout(r, ms)); }

async function fetchTile(tile) {
    const { z, x, y } = tile;
    const [s, w, n, e] = tile2bbox(x, y, z);

    const queries = [
        `[out:json][timeout:25];(way["highway"]["maxspeed"](${s},${w},${n},${e}););out tags geom;`,
        `[out:json][timeout:25];(way["highway"](${s},${w},${n},${e}););out tags geom;`
    ];

    for (const q of queries) {
        try {
            const res = await fetch(`${OVERPASS}?data=${encodeURIComponent(q)}`, { headers: { 'User-Agent':'preprocess-osm/1.0' } });
            if (!res.ok) {
                console.warn(`Overpass returned ${res.status} for tile z${z} x${x} y${y}`);
                continue;
            }
            const json = await res.json();
            if (!json.elements || !json.elements.length) continue;

            const features = json.elements.filter(el => el.type==='way').map(el => {
                const tags = el.tags || {};
                const coords = Array.isArray(el.geometry) ? el.geometry.map(p=>[p.lat,p.lon]) : [];
                const mph = tags.maxspeed ? parseMaxspeedToMph(tags.maxspeed) : inferSpeedFromHighway(tags.highway);
                return { id: el.id, speed: (mph>0?mph:null), highway: tags.highway||null, tags:{maxspeed:tags.maxspeed||null}, coords };
            });

            return { z, x, y, tile_bbox:{south:s, west:w, north:n, east:e}, fetched_at:(new Date()).toISOString(), features };
        } catch(e) {
            console.warn('Overpass query failed for tile', z, x, y, e.message||e);
            await sleep(500);
            continue;
        }
    }

    return { z, x, y, tile_bbox:{south:s, west:w, north:n, east:e}, fetched_at:(new Date()).toISOString(), features:[] };
}

async function saveTileJsonOut(z,x,y,obj){
    const dir = path.join(__dirname,'out','tiles',`${z}`,`${x}`);
    fs.mkdirSync(dir,{recursive:true});
    const filePath = path.join(dir,`${y}.json`);
    fs.writeFileSync(filePath,JSON.stringify(obj,null,2));
    return filePath;
}

(async()=>{
    for(let i=0;i<tiles.length;i++){
        const t=tiles[i];
        console.log(`Processing (${i+1}/${tiles.length}) z${t.z} x${t.x} y${t.y} ...`);
        try{
            const obj = await fetchTile(t);
            const filePath = await saveTileJsonOut(t.z,t.x,t.y,obj);
            console.log('Saved tile',filePath,'features=',obj.features?obj.features.length:0);
        }catch(err){
            console.error('Failed processing tile',t,err.stack||err);
        }
        await sleep(THROTTLE_MS);
    }
    console.log('Done.');
})();
