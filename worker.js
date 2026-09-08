/* הגמל · מוניטור גיאופוליטי — Middle East events monitor
   Data: GDELT 2.0 Events export (free, no key, updated every 15 min).
   Precision: ActionGeo_Type grades each event (city / adm1 / country).
   Events with a place name but no coordinates are geocoded via Nominatim
   (cached in KV, max 8 lookups per cron run, ~1.1s apart). */
import { unzipSync, strFromU8 } from "fflate";

const BBOX = { minLat: 12, maxLat: 42, minLon: 24, maxLon: 64 }; // Middle East incl. Red Sea
let ADSB_DEBUG = [];
const ME_COUNTRIES = new Set(["IS","LE","SY","IR","IZ","SA","YE","EG","JO","TU","AE","QA","KU","BH","OM","GZ","WE","DJ","LY","SD","EI"]);
const MAX_EVENTS = 5000;
const MAX_ALERTS = 60;
const WINDOW_KEEP = 96;
const GEOCODE_PER_RUN = 8;

const ADSB_POINTS = [  // lat, lon, radius(nm) <= 250
  [33.5, 36.0, 200],  // Levant
  [31.0, 44.5, 220],  // Iraq
  [26.5, 51.5, 220],  // Gulf
  [15.5, 42.5, 200],  // Red Sea south
  [34.0, 53.5, 220],  // Iran
];
const MIL_TYPES = new Set(["K35R","K35E","KC35","K46A","C17","C5M","C30J","C130","E3TF","E3CF","E6","E8","RC35","R135","P8","P3","F15","F16","F18","F22","F35","EUFI","RFAL","MIR2","MRTT","A330","GLEX","GLF5","GLF6","E550","B703","B762","B744","A400","V22","H60","UH60","CH47","H47","Q4","RQ4","MQ4","MQ9","Q9","Q1","HERON","HER1","EITM"]);
const MIL_CALL = /^(RCH|CNV|DUKE|LAGR|TOPCT|NATO|QID|IAM|BAF|GAF|HAF|TURK|ASENA|IAF|ISF|RSF|EVAC|MEDEX|SAM|SPAR|PAT|JAKE|HOMER|FORTE|REBEL|VIPER|DRAGN)/i;

async function openskyToken(env, store) {
  const now = Date.now() / 1000;
  if (store.oadsb && store.oadsb.tok && store.oadsb.exp > now + 60) return store.oadsb.tok;
  if (!env.OPENSKY_CLIENT_ID || !env.OPENSKY_CLIENT_SECRET) return null;
  const r = await fetch("https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token", {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: "grant_type=client_credentials&client_id=" + encodeURIComponent(env.OPENSKY_CLIENT_ID) + "&client_secret=" + encodeURIComponent(env.OPENSKY_CLIENT_SECRET),
  });
  ADSB_DEBUG.push("opensky:tok:" + r.status);
  if (!r.ok) return null;
  const d = await r.json();
  store.oadsb = { tok: d.access_token, exp: now + (d.expires_in || 1800) };
  return d.access_token;
}

async function fetchAdsbOpenSky(env, store) {
  try {
    const u = `https://opensky-network.org/api/states/all?lamin=${BBOX.minLat}&lomin=${BBOX.minLon}&lamax=${BBOX.maxLat}&lomax=${BBOX.maxLon}`;
    const tok = await openskyToken(env, store);
    const headers = { "User-Agent": "gamal-monitor/1.0" };
    if (tok) headers["Authorization"] = "Bearer " + tok;
    const r = await fetch(u, { headers });
    ADSB_DEBUG.push("opensky:" + r.status + (tok ? ":auth" : ":anon"));
    if (!r.ok) return null;
    const d = await r.json();
    const out = [];
    for (const st of d.states || []) {
      const cs = (st[1] || "").trim(), lat = st[6], lon = st[5];
      if (lat == null || lon == null || st[8]) continue;         // skip on-ground
      if (!MIL_CALL.test(cs)) continue;                            // callsign-pattern ID only
      out.push({
        id: "ads-" + st[0], d: nowStamp(), a1: cs || st[0], a2: st[2] || "",
        code: "", root: "", quad: "", gold: 0, ment: 0, arts: 0, tone: 0,
        place: `${cs || st[0]} · ${Math.round((st[7] || 0) * 3.281)} ft · ${st[2] || ""}`,
        ctry: "", lat, lon, prec: "city", geo: "adsb",
        url: "https://opensky-network.org/network/explorer?icao24=" + st[0], cat: "military_air", src: "adsb",
      });
      if (out.length >= 120) break;
    }
    return out;
  } catch (e) { ADSB_DEBUG.push("opensky:err:" + String(e && e.message || e).slice(0, 50)); return null; }
}

async function fetchAdsb(env, store) {
  const os = await fetchAdsbOpenSky(env, store);
  if (os) return os;
  const out = [];
  const seen = new Set();
  for (const [lat, lon, dist] of ADSB_POINTS) {
    try {
      let r = null, key = "ac", srcTag = "lol";
      r = await fetch(`https://api.adsb.lol/v2/point/${lat}/${lon}/${dist}`, { headers: { "User-Agent": "gamal-monitor/1.0 (personal OSINT dashboard)" } });
      ADSB_DEBUG.push(lat + ":lol:" + r.status);
      ADSB_DEBUG.push(lat + ":" + srcTag + ":" + r.status);
      if (!r.ok) continue;
      const d = await r.json();
      d.ac = d.ac || d[key] || [];
      ADSB_DEBUG.push(lat + ":n" + d.ac.length);
      for (const a of d.ac || []) {
        if (a.lat == null || a.lon == null) continue;
        if (seen.has(a.hex)) continue;
        const isMil = ((a.dbFlags || 0) & 1) || MIL_TYPES.has((a.t || "").toUpperCase()) || MIL_CALL.test((a.flight || "").trim());
        if (!isMil) continue;
        if (a.lat < BBOX.minLat || a.lat > BBOX.maxLat || a.lon < BBOX.minLon || a.lon > BBOX.maxLon) continue;
        seen.add(a.hex);
        out.push({
          id: "ads-" + a.hex, d: nowStamp(), a1: (a.flight || a.hex).trim(), a2: a.r || "",
          code: "", root: "", quad: "", gold: 0, ment: 0, arts: 0, tone: 0,
          place: `${(a.flight || a.hex).trim()} · ${a.t || "?"} · ${Math.round(a.alt_baro === "ground" ? 0 : a.alt_baro || 0)} ft`,
          ctry: "", lat: a.lat, lon: a.lon, prec: "city", geo: "adsb",
          url: "https://globe.adsb.lol/?icao=" + a.hex, cat: "military_air", src: "adsb",
        });
      }
    } catch (e) { ADSB_DEBUG.push(lat + ":err:" + String(e && e.message || e).slice(0, 50)); }
    await sleep(1000);
  }
  return out.slice(0, 140);
}

const CATS = {
  military_air: { he: "תעופה צבאית", color: "#00E5FF" },
  natural:   { he: "טבע ואסונות",       color: "#B06BFF" },
  conflict:  { he: "עימות צבאי",        color: "#E01F2E" },
  posture:   { he: "איומים / תנועות כוחות", color: "#FF7A1A" },
  protest:   { he: "מחאות",             color: "#F5D90A" },
  economy:   { he: "כלכלה וסחר",        color: "#2ECC71" },
  diplomacy: { he: "דיפלומטיה",         color: "#3E9BFF" },
  other:     { he: "אחר",               color: "#8A8A8A" },
};

const ECON_CODES = new Set(["164","165","166","172","173","174"]);

const RSS_FEEDS = [
  { url: "https://www.ynet.co.il/Integration/StoryRss1854.xml", name: "ynet מבזקים", me: true },
  { url: "https://www.ynet.co.il/Integration/StoryRss2.xml", name: "ynet", me: false },
  { url: "https://www.jpost.com/Rss/RssFeedsHeadlines.aspx", name: "Jerusalem Post", me: true },
  { url: "https://feeds.bbci.co.uk/news/world/middle_east/rss.xml", name: "BBC מזרח תיכון", me: true },
  { url: "https://www.aljazeera.com/xml/rss/all.xml", name: "אל ג'זירה EN", me: false },
];

const CAMERAS = [
  { id: "kotel-aish", name: "הכותל המערבי, ירושלים", lat: 31.7767, lon: 35.2345, kind: "link", url: "https://aish.com/western-wall-page/", src: "Aish Kotel Cam", note: "עמוד שידור חי חיצוני (YouTube) · רענון רציף · אין סנפשוט מוטבע" },
  { id: "kotel-earthcam", name: "הכותל המערבי (מבט רחב), ירושלים", lat: 31.7783, lon: 35.2354, kind: "link", url: "https://www.earthcam.com/world/israel/jerusalem/?cam=jerusalem", src: "EarthCam", note: "עמוד שידור חי חיצוני · רענון רציף · אין סנפשוט מוטבע" },
  { id: "tlv-west-beach", name: "חוף תל אביב המערבי (מלון 7EVEN)", lat: 32.0764, lon: 34.7635, kind: "link", url: "https://beachcam.co.il/en/hamaaravi.html", src: "BeachCam Israel", note: "עמוד שידור חי חיצוני · רענון רציף · אין סנפשוט מוטבע" },
  { id: "tlv-marina", name: "המרינה והטיילת, תל אביב", lat: 32.0856, lon: 34.7674, kind: "link", url: "https://www.webcamtaxi.com/en/israel/tel-aviv/marina-beachfront.html", src: "WebcamTaxi", note: "עמוד שידור חי חיצוני · רענון רציף · אין סנפשוט מוטבע" },
  { id: "giza-pyramids", name: "הפירמידות, גיזה", lat: 29.9792, lon: 31.1342, kind: "link", url: "https://www.skylinewebcams.com/en/webcam/egypt/cairo/cairo/great-pyramid-of-giza.html", src: "SkylineWebcams", note: "עמוד שידור חי חיצוני · רענון רציף · אין סנפשוט מוטבע" },
];

const GAZ = [
["Tehran|טהרן",35.69,51.39],["Isfahan|איספהאן",32.65,51.67],["Shiraz|שיראז",29.59,52.58],["Tabriz|תבריז",38.08,46.29],["Qom",34.64,50.88],["Bandar Abbas",27.18,56.27],["Ahvaz",31.32,48.67],["Kermanshah",34.31,47.06],["Karaj",35.84,50.99],
["Jerusalem|ירושלים",31.77,35.21],["Tel Aviv|תל אביב",32.08,34.78],["Haifa|חיפה",32.79,34.99],["Beersheba|Be'er Sheva|באר שבע",31.25,34.79],["Eilat|אילת",29.56,34.95],["Netanya|נתניה",32.33,34.86],["Ashdod|אשדוד",31.80,34.65],["Ashkelon|אשקלון",31.67,34.57],
["Gaza City|Gaza|עזה",31.50,34.47],["Rafah|רפיח",31.29,34.24],["Khan Younis|Khan Yunis|חאן יונס",31.35,34.30],["Deir al-Balah|דיר אל-בלח",31.42,34.35],["Jabalia|ג'באליה",31.53,34.48],["Hebron|חברון",31.53,35.10],["Nablus|שכם",32.22,35.26],["Ramallah|רמאללה",31.90,35.20],["Jenin|ג'נין",32.46,35.30],
["Beirut|ביירות",33.89,35.50],["Sidon|צידון",33.56,35.37],["Tyre|צור",33.27,35.20],["Tripoli, Leb|Tripoli, Lebanon|טריפולי",34.43,35.85],["Baalbek|בעלבק",34.00,36.21],["Nabatieh|נבטיה",33.38,35.48],
["Damascus|דמשק",33.51,36.29],["Aleppo|חלב",36.20,37.13],["Homs|חומס",34.73,36.71],["Hama|חאמה",35.13,36.75],["Latakia|לטקיה",35.51,35.78],["Tartus|טרטוס",34.89,35.89],["Deir ez-Zor|Deir Ezzor|דיר א-זור",35.33,40.14],["Raqqa|רקה",35.95,39.01],["Idlib|אידליב",35.93,36.63],["Daraa|דרעא",32.62,36.10],["Qamishli|קמישלי",37.05,41.22],["Hasakah",36.51,40.75],
["Baghdad|בגדד",33.31,44.36],["Mosul|מוסול",36.34,43.13],["Erbil|ארביל",36.19,44.01],["Basra|בצרה",30.51,47.78],["Najaf|נג'ף",32.03,44.35],["Karbala|כרבלא",32.60,44.02],["Kirkuk|כירכוכ",35.47,44.39],["Sulaymaniyah",35.56,45.43],["Fallujah",33.35,43.79],["Ramadi",33.42,43.30],
["Sanaa|Sana'a|צנעא",15.35,44.21],["Hodeida|Hodeidah|Hudaydah|חודיידה",14.80,42.95],["Aden|עדן",12.79,45.04],["Taiz|תעז",13.58,44.02],["Marib|מארב",15.47,45.33],["Mukalla",14.54,49.13],["Saada|צעדה",16.94,43.76],
["Riyadh|ריאד",24.71,46.68],["Jeddah|Jiddah|ג'דה",21.49,39.19],["Mecca|מכה",21.42,39.83],["Medina|אל-מדינה",24.47,39.61],["Dammam",26.43,50.10],["Khamis Mushait|ח'מיס מושייט",18.30,42.73],["Abha|עבהא",18.22,42.51],["Jazan|Jizan|ג'זאן",16.89,42.55],["Najran",17.49,44.13],["Tabuk",28.38,36.57],
["Doha|דוחה",25.29,51.53],["Abu Dhabi|אבו דאבי",24.45,54.38],["Dubai|דובאי",25.20,55.27],["Sharjah",25.35,55.42],["Kuwait City|כווית",29.38,47.99],["Manama|מנאמה",26.23,50.59],["Muscat|מוסקט",23.59,58.41],
["Amman|עמאן",31.95,35.93],["Zarqa|זרקא",32.07,36.09],["Irbid",32.56,35.85],["Aqaba|עקבה",29.53,35.01],
["Cairo|קהיר",30.04,31.24],["Alexandria|אלכסנדריה",31.20,29.92],["Giza",30.01,31.21],["Suez|סואץ",29.97,32.55],["Port Said|פורט סעיד",31.26,32.30],["Arish|אל-עריש",31.13,33.80],["Rafah, Egypt",31.24,34.20],
["Ankara|אנקרה",39.93,32.86],["Istanbul|איסטנבול",41.01,28.98],["Izmir|איזמיר",38.42,27.14],["Gaziantep",37.07,37.38],["Diyarbakir",37.91,40.24],["Hatay|Antakya",36.20,36.16],
["West Bank|Judea|Samaria|יהודה ושומרון|איו\"ש|הגדה המערבית",31.90,35.26],
["غزة",31.50,34.47],["رفح",31.29,34.24],["خان يونس|خانيونس",31.35,34.30],["جباليا",31.53,34.48],["النصيرات",31.45,34.39],
["بيروت",33.89,35.50],["صيدا",33.56,35.37],["صور",33.27,35.20],["النبطية",33.38,35.48],["بعلبك",34.00,36.21],["طرابلس",34.43,35.85],
["دمشق",33.51,36.29],["حلب",36.20,37.13],["حمص",34.73,36.71],["حماة",35.13,36.75],["اللاذقية",35.51,35.78],["طرطوس",34.89,35.89],["دير الزور",35.33,40.14],["الرقة",35.95,39.01],["إدلب",35.93,36.63],["درعا",32.62,36.10],["القامشلي",37.05,41.22],["الحسكة",36.51,40.75],
["بغداد",33.31,44.36],["الموصل",36.34,43.13],["أربيل",36.19,44.01],["البصرة",30.51,47.78],["النجف",32.03,44.35],["كربلاء",32.60,44.02],["كركوك",35.47,44.39],["الفلوجة",33.35,43.79],
["صنعاء",15.35,44.21],["الحديدة",14.80,42.95],["عدن",12.79,45.04],["تعز",13.58,44.02],["مأرب",15.47,45.33],["صعدة",16.94,43.76],
["الرياض",24.71,46.68],["جدة",21.49,39.19],["أبها",18.30,42.73],["جازان",16.89,42.55],["نجران",17.49,44.13],["خميس مشيط",18.30,42.73],
["طهران",35.69,51.39],["أصفهان",32.65,51.67],["شيراز",29.59,52.58],["تبريز",38.08,46.29],["قم",34.64,50.88],["نتنز",33.72,51.73],["فوردو",34.88,50.99],["بندر عباس",27.18,56.27],["الأحواز",31.32,48.67],["كرمانشاه",34.31,47.06],
["القدس",31.77,35.21],["تل أبيب",32.08,34.78],["حيفا",32.79,34.99],["بئر السبع",31.25,34.79],["إيلات",29.56,34.95],["أسدود|أشدود",31.80,34.65],["عسقلان",31.67,34.57],["نتانيا",32.33,34.86],["الخليل",31.53,35.10],["نابلس",32.22,35.26],["رام الله",31.90,35.20],["جنين",32.46,35.30],
["عمّان",31.95,35.93],["الزرقاء",32.07,36.09],["إربد",32.56,35.85],["العقبة",29.53,35.01],
["القاهرة",30.04,31.24],["الإسكندرية",31.20,29.92],["السويس",29.97,32.55],["العريش",31.13,33.80],["بور سعيد",31.26,32.30],
["الدوحة",25.29,51.53],["أبو ظبي",24.45,54.38],["دبي",25.20,55.27],["الكويت",29.38,47.99],["المنامة",26.23,50.59],["مسقط",23.59,58.41],
["أنقرة",39.93,32.86],["إسطنبول|اسطنبول",41.01,28.98],["إزمير",38.42,27.14],["غازي عنتاب",37.07,37.38],["أنطاكيا",36.20,36.16],
["باب المندب",12.58,43.33],["مضيق هرمز",26.57,56.25],["البحر الأحمر",20.0,38.5],["سيناء",29.5,34.0],["الجولان",33.0,35.75],["ديمونة",31.07,35.03],
["Bab al-Mandeb|באב אל-מנדב",12.58,43.33],["Strait of Hormuz|Hormuz|מצרי הורמוז",26.57,56.25],["Red Sea|ים סוף",20.0,38.5],["Sinai|סיני",29.5,34.0],["Golan|גולן",33.0,35.75],["Natanz|נתנז",33.72,51.73],["Fordow|פורדו",34.88,50.99],["Isfahan",32.65,51.67],["Dimona|דימונה",31.07,35.03],["Nevatim|נבטים",31.21,34.88],
];
const GAZ_RE = GAZ.map(([re,lat,lon]) => [new RegExp("(?<![\\p{L}\\p{N}])[\\u05D5\\u05D1\\u05DC\\u05DB\\u05DE\\u05E9\\u05D4\\u0648\\u0641\\u0628\\u0643\\u0644\\u0633]?(?:" + re + ")(?![\\p{L}\\p{N}])", "iu"), lat, lon]);
function gazLocate(title) {
  for (const [re, lat, lon] of GAZ_RE) if (re.test(title)) return { lat, lon };
  return null;
}

function kineticType(code) {
  if (!code) return null;
  const K = [
    ["1952", "מתקפת מל\"טים", "strike"], ["1951", "תקיפה אווירית מדויקת", "strike"], ["195", "תקיפה אווירית", "strike"],
    ["194", "אש ארטילרית / שריון", "fight"], ["193", "קרב ירי", "fight"], ["192", "כיבוש שטח", "fight"],
    ["191", "סגר", "fight"], ["196", "הפרת הפסקת אש", "fight"], ["190", "לחימה", "fight"],
    ["1833", "פיצוץ מטען צד", "strike"], ["1832", "פיצוץ רכב תופת", "strike"], ["1831", "פיגוע התאבדות", "strike"], ["183", "פיגוע פיצוץ", "strike"],
    ["186", "חיסול", "strike"], ["185", "ניסיון חיסול", "strike"], ["181", "חטיפה", "fight"], ["182", "תקיפה", "fight"], ["184", "מגן אנושי", "fight"], ["180", "אלימות", "fight"], ["18", "תקיפה", "fight"],
    ["204", "טיהור אתני", "mass"], ["203", "טבח", "mass"], ["202", "פשעי מלחמה", "mass"], ["201", "זוועות", "mass"], ["200", "אלימות המונית", "mass"], ["20", "אלימות המונית", "mass"],
  ];
  for (const [pre, he, kind] of K) if (code.startsWith(pre)) return { he, kind };
  return null;
}

function rssEtype(t) {
  if (/intercept|יירט|יירוט|יירוטו|יורט/i.test(t)) return { he: "יירוט", kind: "intercept" };
  if (/נפילה|נפילות|פגיעה ישירה|direct hit|impacts?|hit (?:a |the )?/i.test(t)) return { he: "התקלה / פגיעה", kind: "impact" };
  if (/launch|שיגור|שוגר|שוגרו|מטח|salvo|rockets? (?:fired|fire)|ירי רקט/i.test(t)) return { he: "שיגור", kind: "launch" };
  if (/drone|uav|מל\"ט|כטב\"ם|כטבם/i.test(t)) return { he: "מתקפת מל\"טים", kind: "strike" };
  if (/strike|airstrike|תקיפה|תקף|תקפה|חיסול|חוסל|התקיפ/i.test(t)) return { he: "תקיפה", kind: "strike" };
  return null;
}

const ME_TERMS = /israel|gaza|iran|syria|lebanon|yemen|houthi|hezbollah|hamas|saudi|gulf|red sea|netanyahu|ישראל|עזה|איראן|סוריה|לבנון|חיזבאללה|חמאס|חות|תימן|סעוד|נטניהו|חיסול|טיל/i;

function classifyText(t) {
  t = t || "";
  if (/quake|earthquake|רעידת/i.test(t)) return "natural";
  if (/missile|strike|airstrike|attack|kill|bomb|drone|rocket|war|clash|assault|תקיפה|חיסול|טיל|רקט|הרג|לחימה|מתקפ/i.test(t)) return "conflict";
  if (/threat|deploy|warship|carrier|troops|military|warning|mobiliz|איום|כוחות|צבא|פריסה|תמרון/i.test(t)) return "posture";
  if (/protest|demonstrat|riot|הפגנ|מחאה/i.test(t)) return "protest";
  if (/sanction|oil|trade|econom|tariff|embargo|deal worth|סנקצי|נפט|סחר|כלכל/i.test(t)) return "economy";
  if (/talks|summit|agreement|ceasefire|diplomat|negotiat|minister|visit|פסגה|הסכם|דיפלומט|שר החוץ|ביקור|משא ומתן|שביתת אש/i.test(t)) return "diplomacy";
  return "other";
}

function hashId(s) { let h = 0; for (let i = 0; i < s.length; i++) { h = (h * 31 + s.charCodeAt(i)) >>> 0; } return h.toString(36); }
function toStamp(ms) { return new Date(ms).toISOString().replace(/[-:T]/g, "").slice(0, 14); }

function nearestGaz(lat, lon) {
  let best = null, bd = 1e9;
  for (const [, la, lo] of GAZ_RE) {
    const d = (la - lat) * (la - lat) + (lo - lon) * (lo - lon);
    if (d < bd) { bd = d; best = [la, lo]; }
  }
  return bd <= 0.09 ? best : null; // ~within 30km-ish
}

async function fetchFirms() {
  try {
    const r = await fetch("https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_Global_24h.csv");
    if (!r.ok) return [];
    const txt = await r.text();
    const lines = txt.split("\n");
    const cands = [];
    for (let i = 1; i < lines.length; i++) {
      const p = lines[i].split(",");
      if (p.length < 13) continue;
      const lat = parseFloat(p[0]), lon = parseFloat(p[1]);
      if (!isFinite(lat) || !isFinite(lon)) continue;
      if (lat < BBOX.minLat || lat > BBOX.maxLat || lon < BBOX.minLon || lon > BBOX.maxLon) continue;
      const frp = parseFloat(p[11]) || 0;
      if (p[8] !== "h" && frp < 10) continue;          // high-confidence or energetic only
      cands.push({ lat, lon, frp, date: p[5], time: p[6], dn: p[12] });
    }
    cands.sort((a, b) => b.frp - a.frp);
    return cands.slice(0, 150).map(c => {
      const near = nearestGaz(c.lat, c.lon);
      const ms = Date.parse(c.date + "T" + c.time.padStart(4, "0").slice(0, 2) + ":" + c.time.padStart(4, "0").slice(2) + ":00Z");
      return {
        id: "frm-" + hashId(c.lat + "," + c.lon + "," + c.date + c.time), d: isFinite(ms) ? toStamp(ms) : nowStamp(),
        a1: "NASA FIRMS", a2: "", code: "", root: "", quad: "", gold: -Math.min(10, Math.round(c.frp / 10)),
        ment: 0, arts: 0, tone: 0, frp: Math.round(c.frp * 10) / 10,
        place: near ? `חדל\"פ תרמי ליד ריכוז (${c.lat.toFixed(2)}, ${c.lon.toFixed(2)})` : `חדל\"פ תרמי בשטח פתוח (${c.lat.toFixed(2)}, ${c.lon.toFixed(2)})`,
        ctry: "", lat: c.lat, lon: c.lon, prec: "city", geo: "firms",
        url: "https://firms.modaps.eosdis.nasa.gov/map/", cat: "natural", src: "firms",
      };
    });
  } catch { return []; }
}

async function fetchUsgs() {
  try {
    const r = await fetch("https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson");
    if (!r.ok) return [];
    const d = await r.json();
    const out = [];
    for (const f of d.features || []) {
      const p = f.properties || {}, g = f.geometry || {};
      const lon = g.coordinates && g.coordinates[0], lat = g.coordinates && g.coordinates[1];
      if (lat == null || lon == null) continue;
      if (lat < BBOX.minLat || lat > BBOX.maxLat || lon < BBOX.minLon || lon > BBOX.maxLon) continue;
      out.push({
        id: "usg-" + f.id, d: toStamp(p.time || Date.now()), a1: "USGS", a2: "",
        code: "", root: "", quad: "", gold: -Math.round((p.mag || 0) * 2), ment: 0, arts: 0,
        tone: -(p.mag || 0), place: p.place || "", ctry: "", lat, lon, prec: "city",
        url: p.url || "", cat: "natural", src: "usgs", mag: p.mag,
      });
    }
    return out;
  } catch { return []; }
}

function unxml(t) { return t.replace(/<!\[CDATA\[|\]\]>/g, "").replace(/<[^>]+>/g, "").replace(/&amp;/g, "&").replace(/&#0?39;|&apos;|&#x27;/g, "'").replace(/&quot;/g, '"').trim(); }

async function fetchRss(feed) {
  try {
    const r = await fetch(feed.url, { headers: { "User-Agent": "Mozilla/5.0 (compatible; gamal-monitor/1.0)" } });
    if (!r.ok) return [];
    const xml = await r.text();
    const out = [];
    const items = xml.match(/<item>[\s\S]*?<\/item>/g) || [];
    for (const it of items.slice(0, 15)) {
      const t = (it.match(/<title[^>]*>([\s\S]*?)<\/title>/) || [])[1] || "";
      const ln = (it.match(/<link[^>]*>([\s\S]*?)<\/link>/) || [])[1] || "";
      const pd = (it.match(/<pubDate[^>]*>([\s\S]*?)<\/pubDate>/) || [])[1] || "";
      const title = unxml(t).slice(0, 200);
      if (!title) continue;
      const cat = classifyText(title);
      if (!feed.me && cat === "other" && !ME_TERMS.test(title)) continue;
      const g = gazLocate(title);
      if (!g) continue;                                  // no resolved physical location -> drop
      const et = rssEtype(title);
      const ts = Date.parse(pd);
      out.push({
        id: "rss-" + hashId(ln || title), d: isFinite(ts) ? toStamp(ts) : nowStamp(),
        a1: feed.name, a2: "", code: "", root: "", quad: "", gold: 0, ment: 0, arts: 0,
        tone: 0, place: title, ctry: "", lat: g.lat, lon: g.lon, prec: "city", geo: "gazetteer",
        url: unxml(ln).slice(0, 300), cat: et ? "conflict" : (cat === "other" ? "diplomacy" : cat), src: "rss", etype: et,
      });
    }
    return out;
  } catch { return []; }
}

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
    const prec = precFromGeoType(c[51], hasCoords);
    if (prec === "country") continue;                    // no country-centroid fallbacks
    const cat0 = classify(code);
    if (cat0 === "other") continue;                      // cut generic noise
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
      prec,
      url: (c[60] || "").slice(0, 300),
      cat: cat0, etype: kineticType(code),
    });
  }
  // cap diplomacy noise: keep the 25 most-covered per batch
  const dip = out.filter(e => e.cat === "diplomacy").sort((a, b) => b.ment - a.ment);
  const keepDip = new Set(dip.slice(0, 25).map(e => e.id));
  return out.filter(e => e.cat !== "diplomacy" || keepDip.has(e.id));
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


const TG_LABEL_SRC = "telegram";
const DEFAULT_TG = ["middle_east_spectator", "abualiexpress", "osintdefender", "war_monitoring"];
const TG_PER_RUN = 2;
let TG_DEBUG = [];

function tgChannels(store) {
  if (!Array.isArray(store.tgChannels) || !store.tgChannels.length) store.tgChannels = DEFAULT_TG.slice();
  return store.tgChannels;
}

async function fetchTelegram(store) {
  const chans = tgChannels(store);
  const out = [];
  if (!chans.length) return out;
  store.tgCursor = (store.tgCursor || 0) % chans.length;
  const picks = [];
  for (let i = 0; i < Math.min(TG_PER_RUN, chans.length); i++) picks.push(chans[(store.tgCursor + i) % chans.length]);
  store.tgCursor = (store.tgCursor + TG_PER_RUN) % chans.length;
  for (const h of picks) {
    try {
      const r = await fetch("https://t.me/s/" + h, { headers: { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36" } });
      if (!r.ok) { TG_DEBUG.push(h + ":" + r.status); continue; }
      const html = await r.text();
      TG_DEBUG.push(h + ":len" + html.length + ":dp" + (html.split("data-post=").length - 1) + ":tx" + (html.split("tgme_widget_message_text").length - 1));
      const blocks = html.match(/<div class="tgme_widget_message\s[^>]*data-post="[^"]*"[\s\S]*?(?=<div class="tgme_widget_message\s[^>]*data-post=|$)/g) || [];
      let n = 0, txts = 0;
      for (const b of blocks) {
        const dp = (b.match(/data-post="([^"]+)"/) || [])[1];
        const tm = (b.match(/datetime="([^"]+)"/) || [])[1];
        const txm = b.match(/<div class="tgme_widget_message_text[^"]*"[^>]*>([\s\S]*?)<\/div>/);
        if (!dp || !txm) continue;
        const text = unxml(txm[1].replace(/<br[^>]*>/gi, " ")).replace(/\s+/g, " ").trim().slice(0, 400);
        if (text.length < 25) continue;
        txts++;
        const g = gazLocate(text);
        if (!g) continue;                                   // iron rule: no resolved location -> drop
        const ts = Date.parse(tm || "");
        const postId = dp.split("/").pop();
        const et = rssEtype(text);
        const cat = classifyText(text);
        out.push({
          id: "tg-" + h + "-" + postId, d: isFinite(ts) ? toStamp(ts) : nowStamp(),
          a1: "@" + h, a2: "", code: "", root: "", quad: "", gold: 0, ment: 0, arts: 0, tone: 0,
          place: text.slice(0, 120), ctry: "", lat: g.lat, lon: g.lon, prec: "city", geo: "gazetteer",
          url: "https://t.me/" + dp, cat: et ? "conflict" : (cat === "other" ? "diplomacy" : cat), src: TG_LABEL_SRC, etype: et,
        });
        n++;
      }
      TG_DEBUG.push(h + ":" + blocks.length + "blk:" + txts + "txt:" + n + "loc");
    } catch (e) { TG_DEBUG.push(h + ":err:" + String(e && e.message || e).slice(0, 40)); }
  }
  return out;
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

  ADSB_DEBUG = [];
  const adsb = await fetchAdsb(env, store);
  const adsbIds = new Set(adsb.map(e => e.id));
  store.events = store.events.filter(e => e.src !== "adsb" || !adsbIds.has(e.id));  // movers get fresh positions
  TG_DEBUG = [];
  const extras = adsb.concat(await fetchUsgs(), await fetchFirms(), await fetchTelegram(store), ...(await Promise.all(RSS_FEEDS.map(fetchRss))));
  const seen2 = new Set(store.events.map(e => e.id));
  const freshExtras = extras.filter(e => !seen2.has(e.id));
  store.events = store.events.concat(freshExtras);
  for (const e of freshExtras) {
    if (e.src === "usgs" && (e.mag || 0) >= 5) {
      const key = "quake_" + e.id;
      if (!store.alerts.some(a => a.key === key)) store.alerts.unshift({ key, type: "quake", place: e.place, t: nowStamp(), text: `רעידת אדמה בעוצמה ${e.mag} — ${e.place}` });
    }
  }

  const geocoded = await geocodePending(store);

  const cutoff = nowStampMinus(26 * 3600 * 1000);
  const adsbCut = nowStampMinus(40 * 60 * 1000);
  store.events = store.events.filter(e => e.lat != null && e.prec !== "unknown");
  store.events = store.events.filter(e => e.src !== "adsb" || e.d >= adsbCut);
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
  const srcs = {};
  for (const e of store.events) srcs[e.src || "gdelt"] = (srcs[e.src || "gdelt"] || 0) + 1;
  return { ok: true, added: batch.length, extras: freshExtras.length, adsb: adsb.length, adsbDebug: ADSB_DEBUG, tgDebug: TG_DEBUG, geocoded, prec: precCount, srcs, updated: store.updated };
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
  const srcCount = {};
  for (const e of evs) srcCount[e.src || "gdelt"] = (srcCount[e.src || "gdelt"] || 0) + 1;
  const kin = evs.filter(e => e.etype);
  const byKind = {};
  for (const e of kin) byKind[e.etype.kind] = (byKind[e.etype.kind] || 0) + 1;
  if (kin.length) lines.push(`${kin.length} אירועים קינטיים: ${byKind.strike || 0} תקיפות/שיגורים, ${byKind.intercept || 0} יירוטים, ${byKind.impact || 0} התקלות, ${byKind.fight || 0} לחימה, ${byKind.mass || 0} אלימות המונית. מבוסס דיווחים — לא אישור רשמי.`);
  lines.push(`מקורות איסוף: GDELT (${srcCount.gdelt || 0} אירועים), חדל\"פים תרמיים NASA FIRMS (${srcCount.firms || 0}), רעידות אדמה USGS (${srcCount.usgs || 0}), תעופה צבאית ADS-B (${srcCount.adsb || 0}), כותרות חיות ממוקמות (${srcCount.rss || 0}).`);
  lines.push(`כיסוי וכנות: שכבת התעופה (ADS-B) מושבתת כרגע — adsb.lol, adsb.fi ו-OpenSky חוסמים גישה משרתי ענן; היא תופעל אוטומטית אם הגישה תיפתח. AIS לספינות אינו זמין חינם ללא מפתח ולכן אינו מוצג. תצלומי לווין טקטיים של תנועות כוחות דורשים ספקים בתשלום (Planet/Maxar) — שכבת NASA GIBS היא רזולוציה נמוכה ולא טקטית.`);
  lines.push(`דיוק מיקום: כל ${evs.length} האירועים ממוקמים ברמת עיר/אתר בלבד — אירועים ללא מיקום פיזי מזוהה לא נכנסים ללוח.`);
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
      return json({ updated: store.updated, events: store.events.slice(0, 1200), windows: store.windows, alerts: store.alerts, cats: CATS, tgChannels: tgChannels(store), cams: CAMERAS });
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
    if (url.pathname === "/api/tg") {
      const store = await loadStore(env);
      return json({ channels: tgChannels(store), cursor: store.tgCursor || 0, label: "טלגרם · ערוץ ציבורי · לא מאומת" });
    }
    if (url.pathname === "/api/tg/add") {
      const h = (url.searchParams.get("h") || "").trim().replace(/^@/, "").toLowerCase();
      if (!/^[a-z0-9_]{3,32}$/.test(h)) return json({ ok: false, reason: "bad handle" }, 400);
      const store = await loadStore(env);
      const chans = tgChannels(store);
      if (!chans.includes(h)) { chans.push(h); await env.MONITOR_KV.put("store", JSON.stringify(store)); }
      return json({ ok: true, channels: chans });
    }
    if (url.pathname === "/api/tg/remove") {
      const h = (url.searchParams.get("h") || "").trim().replace(/^@/, "").toLowerCase();
      const store = await loadStore(env);
      store.tgChannels = tgChannels(store).filter(c => c !== h);
      store.tgCursor = 0;
      await env.MONITOR_KV.put("store", JSON.stringify(store));
      return json({ ok: true, channels: store.tgChannels });
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
