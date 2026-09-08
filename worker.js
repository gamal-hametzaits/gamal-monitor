/* הגמל · מוניטור גיאופוליטי — Middle East events monitor
   Data: GDELT 2.0 Events export (free, no key, updated every 15 min).
   Cron ingests the latest 15-min batch into KV; API serves the dashboard. */
import { unzipSync, strFromU8 } from "fflate";

const BBOX = { minLat: 12, maxLat: 42, minLon: 24, maxLon: 64 }; // Middle East incl. Red Sea
const MAX_EVENTS = 5000;
const MAX_ALERTS = 60;
const WINDOW_KEEP = 96; // 24h of 15-min windows

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

function parseBatch(text) {
  const out = [];
  const lines = text.split("\n");
  for (const l of lines) {
    const c = l.split("\t");
    if (c.length < 61) continue;
    const lat = parseFloat(c[56]), lon = parseFloat(c[57]);
    if (!isFinite(lat) || !isFinite(lon)) continue;
    if (lat < BBOX.minLat || lat > BBOX.maxLat || lon < BBOX.minLon || lon > BBOX.maxLon) continue;
    const code = c[26] || "";
    out.push({
      id: c[0],
      d: c[59],                       // DATEADDED yyyymmddhhmmss
      a1: (c[6] || "").slice(0, 60),
      a2: (c[16] || "").slice(0, 60),
      code, root: c[28] || "", quad: c[29] || "",
      gold: parseFloat(c[30]) || 0,
      ment: parseInt(c[31]) || 0,
      arts: parseInt(c[33]) || 0,
      tone: Math.round((parseFloat(c[34]) || 0) * 10) / 10,
      place: (c[52] || "").slice(0, 80),
      ctry: c[53] || "",
      lat, lon,
      url: (c[60] || "").slice(0, 300),
      cat: classify(code),
    });
  }
  return out;
}

function gridKey(e) { return `${Math.round(e.lat)}_${Math.round(e.lon)}`; }
function nowStamp() { const d = new Date(); return d.toISOString().replace(/[-:T]/g, "").slice(0, 14); }

async function emptyStore() { return { updated: null, lastfile: null, events: [], windows: [], alerts: [] }; }

async function loadStore(env) {
  const s = await env.MONITOR_KV.get("store");
  if (!s) return emptyStore();
  try { return JSON.parse(s); } catch { return emptyStore(); }
}

function detectAlerts(batch, store) {
  const fresh = [];
  const cells = {};
  for (const e of batch) {
    if (e.quad !== "4") continue;
    const k = gridKey(e);
    cells[k] = cells[k] || { n: 0, place: e.place, ment: 0, worst: 0 };
    cells[k].n++; cells[k].ment += e.ment; cells[k].worst = Math.min(cells[k].worst, e.gold);
  }
  for (const [k, v] of Object.entries(cells)) {
    if (v.n >= 3) {
      fresh.push({ key: "cluster_" + k, type: "cluster", place: v.place, n: v.n, ment: v.ment,
        text: `אשכול עימותים: ${v.n} אירועי עימות צבאי סביב ${v.place || "אזור"} ברבע השעה האחרונה` });
    }
  }
  for (const e of batch) {
    if (e.root === "20" && e.ment >= 10) {
      fresh.push({ key: "mass_" + e.id, type: "mass", place: e.place,
        text: `אלימות המונית: אירוע ב-${e.place} עם ${e.ment} אזכורים תקשורתיים` });
    } else if (e.gold <= -8 && e.ment >= 15) {
      fresh.push({ key: "severe_" + e.id, type: "severe", place: e.place,
        text: `אירוע חריף (ציון חומרה ${e.gold}): ${e.place}, ${e.ment} אזכורים` });
    }
  }
  // dedupe against alerts from the last 6h
  const cutoff = nowStampMinus(6 * 3600 * 1000);
  const recentKeys = new Set(store.alerts.filter(a => a.t >= cutoff).map(a => a.key));
  const accepted = fresh.filter(a => !recentKeys.has(a.key)).map(a => ({ ...a, t: nowStamp() }));
  return accepted;
}

function nowStampMinus(ms) {
  const d = new Date(Date.now() - ms);
  return d.toISOString().replace(/[-:T]/g, "").slice(0, 14);
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
  if (url === store.lastfile) return { ok: true, skipped: true, updated: store.updated };
  const zr = await fetch(url.replace("http://", "https://"));
  if (!zr.ok) return { ok: false, reason: "zip http " + zr.status };
  const buf = new Uint8Array(await zr.arrayBuffer());
  const files = unzipSync(buf);
  const name = Object.keys(files)[0];
  const csv = strFromU8(files[name]);
  const batch = parseBatch(csv);

  const seen = new Set(store.events.map(e => e.id));
  const merged = store.events.concat(batch.filter(e => !seen.has(e.id)));
  const cutoff = nowStampMinus(26 * 3600 * 1000);
  store.events = merged.filter(e => e.d >= cutoff).sort((a, b) => (a.d < b.d ? 1 : -1)).slice(0, MAX_EVENTS);

  const cats = {};
  for (const e of batch) cats[e.cat] = (cats[e.cat] || 0) + 1;
  const fileTs = (name.match(/(\d{14})/) || [])[1] || nowStamp();
  store.windows.push({ t: fileTs, n: batch.length, cats });
  store.windows = store.windows.slice(-WINDOW_KEEP);

  const newAlerts = detectAlerts(batch, store);
  store.alerts = newAlerts.concat(store.alerts).slice(0, MAX_ALERTS);

  store.lastfile = url;
  store.updated = nowStamp();
  await env.MONITOR_KV.put("store", JSON.stringify(store));
  return { ok: true, added: batch.length, alerts: newAlerts.length, updated: store.updated };
}

function fmtT(stamp) {
  if (!stamp || stamp.length < 14) return stamp || "";
  return `${stamp.slice(8,10)}:${stamp.slice(10,12)} · ${stamp.slice(6,8)}/${stamp.slice(4,6)}`;
}

function buildReport(store) {
  const cutoff = nowStampMinus(24 * 3600 * 1000);
  const evs = store.events.filter(e => e.d >= cutoff);
  const byCat = {};
  for (const e of evs) byCat[e.cat] = (byCat[e.cat] || 0) + 1;
  const h3 = nowStampMinus(3 * 3600 * 1000), h6 = nowStampMinus(6 * 3600 * 1000);
  const last3 = evs.filter(e => e.d >= h3).length;
  const prev3 = evs.filter(e => e.d >= h6 && e.d < h3).length;
  const trendPct = prev3 ? Math.round(((last3 - prev3) / prev3) * 100) : 0;

  const hot = {};
  for (const e of evs) {
    if (e.cat !== "conflict" && e.cat !== "posture") continue;
    const k = e.place || e.ctry || "לא ידוע";
    hot[k] = hot[k] || { n: 0, tone: 0, ment: 0 };
    hot[k].n++; hot[k].tone += e.tone; hot[k].ment += e.ment;
  }
  const hotspots = Object.entries(hot).map(([place, v]) => ({ place, n: v.n, tone: Math.round(v.tone / v.n * 10) / 10, ment: v.ment }))
    .sort((a, b) => b.n - a.n).slice(0, 6);

  const notable = evs.slice().sort((a, b) => b.ment - a.ment).slice(0, 8).map(e => ({
    place: e.place, cat: e.cat, catHe: CATS[e.cat].he, ment: e.ment, gold: e.gold,
    a1: e.a1, a2: e.a2, url: e.url, t: fmtT(e.d),
  }));

  const lines = [];
  lines.push(`ב-24 השעות האחרונות תועדו ${evs.length} אירועים גיאופוליטיים במזרח התיכון.`);
  if (byCat.conflict) lines.push(`${byCat.conflict} אירועי עימות צבאי, ${byCat.posture || 0} איומים או תנועות כוחות, ${byCat.economy || 0} אירועי כלכלה וסחר, ${byCat.diplomacy || 0} אירועים דיפלומטיים.`);
  if (prev3) lines.push(trendPct > 10 ? `מגמת הסלמה: עלייה של ${trendPct}% בהיקף האירועים ב-3 השעות האחרונות לעומת שלוש השעות הקודמות.` : trendPct < -10 ? `מגמת רגיעה: ירידה של ${Math.abs(trendPct)}% בהיקף האירועים ב-3 השעות האחרונות.` : `היקף האירועים יציב יחסית (שינוי של ${trendPct}%) ב-3 השעות האחרונות.`);
  for (const h of hotspots.slice(0, 3)) lines.push(`מוקד חם: ${h.place} — ${h.n} אירועי עימות/הצבת כוח, טון ממוצע ${h.tone}.`);
  if (store.alerts.length) lines.push(`${store.alerts.filter(a => a.t >= cutoff).length} איתותי הסלמה הופעלו במהלך היום האחרון.`);

  return {
    generated: fmtT(store.updated),
    totals: { events: evs.length, byCat, last3, prev3, trendPct },
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
      const r = await ingest(env);
      return json(r);
    }
    if (url.pathname === "/api/status") {
      const store = await loadStore(env);
      return json({ updated: store.updated, events: store.events.length, alerts: store.alerts.length, windows: store.windows.length });
    }
    return env.ASSETS.fetch(request);
  },
  async scheduled(event, env, ctx) {
    ctx.waitUntil(ingest(env));
  },
};
