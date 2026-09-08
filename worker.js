/* הגמל · מוניטור גיאופוליטי — Middle East events monitor
   Data: GDELT 2.0 Events export (free, no key, updated every 15 min).
   Precision: ActionGeo_Type grades each event (city / adm1 / country).
   Events with a place name but no coordinates are geocoded via Nominatim
   (cached in KV, max 8 lookups per cron run, ~1.1s apart). */
import { unzipSync, strFromU8 } from "fflate";

const BBOX = { minLat: 12, maxLat: 42, minLon: 24, maxLon: 64 }; // Middle East incl. Red Sea
const ME_COUNTRIES = new Set(["IS","LE","SY","IR","IZ","SA","YE","EG","JO","TU","AE","QA","KU","BH","OM","GZ","WE","DJ","LY","SD","EI"]);
const MAX_EVENTS = 5000;
const MAX_ALERTS = 60;
const WINDOW_KEEP = 96;
const GEOCODE_PER_RUN = 8;

const CATS = {
  conflict:  { he: "עימות צבאי",        color: "#E01F2E" },
  posture:   { he: "איומים / תנועות כוחות", color: "#FF7A1A" },
  protest:   { he: "מחאות",             color: "#F5D90A" },
  economy:   { he: "כלכלה וסחר",        color: "#2ECC71" },
  diplomacy: { he: "דיפלומטיה",         color: "#3E9BFF" },
  other:     { he: "אחר",               color: "#8A8A8A" },
};

const ECON_CODES = new Set(["164","165","166","172","173","174"]);

function classify(code) {
  if (!code) return "other";
  const root = code.slice(0, 2);
  if (ECON_CODES.has(code) || root === "06" || root === "07") return "economy";
  if (root === "18" || root === "19" || root === "20") return "conflict";
  if (root === "13" || root === "15" || root === "17") return "posture";
  if (root === "14") return "protest";
  if (root >= "01" && root <= "12") return "diplomacy";
  return "other";
}

function json(o, s = 200) {
  return new Response(JSON.stringify(o), { status: s, headers: { "Content-Type": "application/json; charset=utf-8" } });
}

function precFromGeoType(gt, hasCoords) {
  if (!hasCoords) return "pending";
  if (gt === "3" || gt === "4") return "city";
  if (gt === "2" || gt === "5") return "adm1";
  return "country";
}

function parseBatch(text) {
  const out = [];
  const lines = text.split("\n");
  for (const l of lines) {
    const c = l.split("\t");
    if (c.length < 61) continue;
    const lat = parseFloat(c[56]), lon = parseFloat(c[57]);
    const hasCoords = isFinite(lat) && isFinite(lon);
    const ctry = c[53] || "";
    if (hasCoords) {
      if (lat < BBOX.minLat || lat > BBOX.maxLat || lon < BBOX.minLon || lon > BBOX.maxLon) continue;
    } else {
      if (!ME_COUNTRIES.has(ctry)) continue;       // unlocatable & not ME -> drop
      if (!c[52]) continue;                        // no place name to geocode -> drop
    }
    const code = c[26] || "";
    out.push({
      id: c[0],
      d: c[59],
      a1: (c[6] || "").slice(0, 60),
      a2: (c[16] || "").slice(0, 60),
      code, root: c[28] || "", quad: c[29] || "",
      gold: parseFloat(c[30]) || 0,
      ment: parseInt(c[31]) || 0,
      arts: parseInt(c[33]) || 0,
      tone: Math.round((parseFloat(c[34]) || 0) * 10) / 10,
      place: (c[52] || "").slice(0, 80),
      ctry,
      lat: hasCoords ? lat : null,
      lon: hasCoords ? lon : null,
      prec: precFromGeoType(c[51], hasCoords),
      url: (c[60] || "").slice(0, 300),
      cat: classify(code),
    });
  }
  return out;
}

function gridKey(e) { return `${Math.round(e.lat)}_${Math.round(e.lon)}`; }
function nowStamp() { return new Date().toISOString().replace(/[-:T]/g, "").slice(0, 14); }
function nowStampMinus(ms) { return new Date(Date.now() - ms).toISOString().replace(/[-:T]/g, "").slice(0, 14); }
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function emptyStore() { return { updated: null, lastfile: null, events: [], windows: [], alerts: [], geoc: {} }; }
async function loadStore(env) {
  const s = await env.MONITOR_KV.get("store");
  if (!s) return emptyStore();
  try { const st = JSON.parse(s); st.geoc = st.geoc || {}; return st; } catch { return emptyStore(); }
}

async function geocodeOne(name) {
  try {
    const u = "https://nominatim.openstreetmap.org/search?format=json&limit=1&q=" + encodeURIComponent(name);
    const r = await fetch(u, { headers: { "User-Agent": "gamal-monitor/1.0 (personal OSINT dashboard)" } });
    if (!r.ok) return null;
    const arr = await r.json();
    if (!arr.length) return { miss: true };
    const lat = parseFloat(arr[0].lat), lon = parseFloat(arr[0].lon);
    if (!isFinite(lat) || !isFinite(lon)) return { miss: true };
    if (lat < BBOX.minLat || lat > BBOX.maxLat || lon < BBOX.minLon || lon > BBOX.maxLon) return { miss: true };
    return { lat, lon };
  } catch { return null; }
}

async function geocodePending(store) {
  const need = [];
  const seenNames = new Set();
  for (const e of store.events) {
    if (e.prec !== "pending" || !e.place) continue;
    const key = e.place.toLowerCase();
    if (key in store.geoc || seenNames.has(key)) continue;
    seenNames.add(key);
    need.push(key);
    if (need.length >= GEOCODE_PER_RUN) break;
  }
  let done = 0;
  for (const key of need) {
    const g = await geocodeOne(key);
    if (g) store.geoc[key] = g;
    done++;
    if (done < need.length) await sleep(1100);
  }
  for (const e of store.events) {
    if (e.prec !== "pending" || !e.place) continue;
    const g = store.geoc[e.place.toLowerCase()];
    if (!g) continue;
    if (g.miss) { e.prec = "unknown"; continue; }
    e.lat = g.lat; e.lon = g.lon; e.prec = "city"; e.geo = "nominatim";
  }
  return need.length;
}

function detectAlerts(batch, store) {
  const fresh = [];
  const cells = {};
  for (const e of batch) {
    if (e.quad !== "4") continue;
    if (e.prec !== "city" && e.prec !== "adm1") continue;   // clusters only on real locations
    const k = gridKey(e);
    cells[k] = cells[k] || { n: 0, place: e.place, ment: 0 };
    cells[k].n++; cells[k].ment += e.ment;
  }
  for (const [k, v] of Object.entries(cells)) {
    if (v.n >= 3) {
      fresh.push({ key: "cluster_" + k, type: "cluster", place: v.place, n: v.n, ment: v.ment,
        text: `אשכול עימותים: ${v.n} אירועי עימות צבאי סביב ${shortPlace(v.place) || "אזור"} ברבע השעה האחרונה` });
    }
  }
  for (const e of batch) {
    if (e.root === "20" && e.ment >= 10) {
      fresh.push({ key: "mass_" + e.id, type: "mass", place: e.place,
        text: `אלימות המונית: אירוע ב-${shortPlace(e.place)} עם ${e.ment} אזכורים תקשורתיים` });
    } else if (e.gold <= -8 && e.ment >= 15) {
      fresh.push({ key: "severe_" + e.id, type: "severe", place: e.place,
        text: `אירוע חריף (ציון חומרה ${e.gold}): ${shortPlace(e.place)}, ${e.ment} אזכורים` });
    }
  }
  const cutoff = nowStampMinus(6 * 3600 * 1000);
  const recentKeys = new Set(store.alerts.filter(a => a.t >= cutoff).map(a => a.key));
  return fresh.filter(a => !recentKeys.has(a.key)).map(a => ({ ...a, t: nowStamp() }));
}

function shortPlace(p) {
  if (!p) return "לא ידוע";
  const parts = p.split(",");
  return (parts.length > 2 ? parts.slice(0, -1).join(",") : p).trim();
}

async function ingest(env) {
  try { return await ingestInner(env); } catch (e) { return { ok: false, error: String(e && e.message || e), stack: String(e && e.stack || "").slice(0, 300) }; }
}
async function ingestInner(env) {
  const store = await loadStore(env);
  const lu = await fetch("https://data.gdeltproject.org/gdeltv2/lastupdate.txt", { cf: { cacheTtl: 0 } });
  if (!lu.ok) return { ok: false, reason: "lastupdate http " + lu.status };
  const txt = await lu.text();
  const line = txt.split("\n").find(l => l.includes(".export.CSV.zip"));
  if (!line) return { ok: false, reason: "no export line" };
  const url = line.trim().split(/\s+/)[2];
  let batch = [];
  if (url !== store.lastfile) {
    const zr = await fetch(url.replace("http://", "https://"));
    if (!zr.ok) return { ok: false, reason: "zip http " + zr.status };
    const buf = new Uint8Array(await zr.arrayBuffer());
    const files = unzipSync(buf);
    const name = Object.keys(files)[0];
    batch = parseBatch(strFromU8(files[name]));

    const seen = new Set(store.events.map(e => e.id));
    store.events = store.events.concat(batch.filter(e => !seen.has(e.id)));

    const cats = {};
    for (const e of batch) cats[e.cat] = (cats[e.cat] || 0) + 1;
    const fileTs = (name.match(/(\d{14})/) || [])[1] || nowStamp();
    store.windows.push({ t: fileTs, n: batch.length, cats });
    store.windows = store.windows.slice(-WINDOW_KEEP);
    store.lastfile = url;
  }

  const geocoded = await geocodePending(store);

  const cutoff = nowStampMinus(26 * 3600 * 1000);
  store.events = store.events.filter(e => e.d >= cutoff).sort((a, b) => (a.d < b.d ? 1 : -1)).slice(0, MAX_EVENTS);

  if (batch.length) {
    const newAlerts = detectAlerts(batch, store);
    store.alerts = newAlerts.concat(store.alerts).slice(0, MAX_ALERTS);
  }
  const geocKeys = Object.keys(store.geoc);
  if (geocKeys.length > 2000) { for (const k of geocKeys.slice(0, geocKeys.length - 2000)) delete store.geoc[k]; }

  store.updated = nowStamp();
  await env.MONITOR_KV.put("store", JSON.stringify(store));
  const precCount = { city: 0, adm1: 0, country: 0, pending: 0, unknown: 0 };
  for (const e of store.events) precCount[e.prec] = (precCount[e.prec] || 0) + 1;
  return { ok: true, added: batch.length, geocoded, prec: precCount, updated: store.updated };
}

function fmtT(stamp) {
  if (!stamp || stamp.length < 14) return stamp || "";
  return `${stamp.slice(8,10)}:${stamp.slice(10,12)} · ${stamp.slice(6,8)}/${stamp.slice(4,6)}`;
}

function buildReport(store) {
  const cutoff = nowStampMinus(24 * 3600 * 1000);
  const evs = store.events.filter(e => e.d >= cutoff);
  const byCat = {}, byPrec = {};
  for (const e of evs) { byCat[e.cat] = (byCat[e.cat] || 0) + 1; byPrec[e.prec] = (byPrec[e.prec] || 0) + 1; }
  const h3 = nowStampMinus(3 * 3600 * 1000), h6 = nowStampMinus(6 * 3600 * 1000);
  const last3 = evs.filter(e => e.d >= h3).length;
  const prev3 = evs.filter(e => e.d >= h6 && e.d < h3).length;
  const trendPct = prev3 ? Math.round(((last3 - prev3) / prev3) * 100) : 0;

  const hot = {};
  for (const e of evs) {
    if (e.cat !== "conflict" && e.cat !== "posture") continue;
    if (e.prec !== "city" && e.prec !== "adm1") continue;
    const k = shortPlace(e.place) || e.ctry || "לא ידוע";
    hot[k] = hot[k] || { n: 0, tone: 0, ment: 0 };
    hot[k].n++; hot[k].tone += e.tone; hot[k].ment += e.ment;
  }
  const hotspots = Object.entries(hot).map(([place, v]) => ({ place, n: v.n, tone: Math.round(v.tone / v.n * 10) / 10, ment: v.ment }))
    .sort((a, b) => b.n - a.n).slice(0, 6);

  const notable = evs.slice().sort((a, b) => b.ment - a.ment).slice(0, 8).map(e => ({
    place: shortPlace(e.place), cat: e.cat, catHe: CATS[e.cat].he, ment: e.ment, gold: e.gold,
    prec: e.prec, a1: e.a1, a2: e.a2, url: e.url, t: fmtT(e.d),
  }));

  const coarse = (byPrec.country || 0) + (byPrec.unknown || 0) + (byPrec.pending || 0);
  const lines = [];
  lines.push(`ב-24 השעות האחרונות תועדו ${evs.length} אירועים גיאופוליטיים במזרח התיכון.`);
  if (byCat.conflict) lines.push(`${byCat.conflict} אירועי עימות צבאי, ${byCat.posture || 0} איומים או תנועות כוחות, ${byCat.economy || 0} אירועי כלכלה וסחר, ${byCat.diplomacy || 0} אירועים דיפלומטיים.`);
  if (prev3) lines.push(trendPct > 10 ? `מגמת הסלמה: עלייה של ${trendPct}% בהיקף האירועים ב-3 השעות האחרונות לעומת שלוש השעות הקודמות.` : trendPct < -10 ? `מגמת רגיעה: ירידה של ${Math.abs(trendPct)}% בהיקף האירועים ב-3 השעות האחרונות.` : `היקף האירועים יציב יחסית (שינוי של ${trendPct}%) ב-3 השעות האחרונות.`);
  for (const h of hotspots.slice(0, 3)) lines.push(`מוקד חם: ${h.place} — ${h.n} אירועי עימות/הצבת כוח, טון ממוצע ${h.tone}.`);
  lines.push(`דיוק מיקום: ${byPrec.city || 0} אירועים ברמת עיר, ${byPrec.adm1 || 0} ברמת מחוז, ${coarse} ללא מיקום מדויק (מסומנים כמשוערים).`);
  if (store.alerts.length) lines.push(`${store.alerts.filter(a => a.t >= cutoff).length} איתותי הסלמה הופעלו במהלך היום האחרון.`);

  return {
    generated: fmtT(store.updated),
    totals: { events: evs.length, byCat, byPrec, last3, prev3, trendPct },
    lines, hotspots, notable,
    disclaimer: "הדוח מבוסס על ניתוח אוטומטי של סיקור חדשותי פתוח (GDELT). איתותים סטטיסטיים בלבד — אינם תחזית ואינם מידע מאומת.",
  };
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/api/data") {
      const store = await loadStore(env);
      return json({ updated: store.updated, events: store.events.slice(0, 1200), windows: store.windows, alerts: store.alerts, cats: CATS });
    }
    if (url.pathname === "/api/report") {
      const store = await loadStore(env);
      return json(buildReport(store));
    }
    if (url.pathname === "/api/refresh") {
      return json(await ingest(env));
    }
    if (url.pathname === "/api/reset") {
      await env.MONITOR_KV.delete("store");
      return json({ ok: true, reset: true });
    }
    if (url.pathname === "/api/status") {
      const store = await loadStore(env);
      const prec = {};
      for (const e of store.events) prec[e.prec] = (prec[e.prec] || 0) + 1;
      return json({ updated: store.updated, events: store.events.length, alerts: store.alerts.length, windows: store.windows.length, prec });
    }
    return env.ASSETS.fetch(request);
  },
  async scheduled(event, env, ctx) {
    ctx.waitUntil(ingest(env));
  },
};
