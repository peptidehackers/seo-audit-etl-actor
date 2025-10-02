import AdmZip from 'adm-zip';
import Papa from 'papaparse';
import iconv from 'iconv-lite';
import { emptyNormalized } from './schema.js';
import { computeScores } from './scoring.js';

// CSV: try UTF-8; if empty/garbled, fallback to UTF-16 + tab (Ahrefs)
function parseCsvSmart(buffer) {
  let text = buffer.toString('utf8');
  let res  = Papa.parse(text, { header: true });
  const bad = (res.errors?.length > 5) || (!res.data || res.data.length === 0);
  if (bad) {
    text = iconv.decode(buffer, 'utf16le');
    res  = Papa.parse(text, { header: true, delimiter: '\t' });
  }
  return res.data || [];
}

function readEntry(zip, name, manifest) {
  const ent = zip.getEntry(name);
  if (!ent) { manifest[name] = { status: 'missing' }; return null; }
  const buf = ent.getData();
  manifest[name] = { status: 'present', size: buf.length };
  return buf;
}

// Find a column among several possible header names (case-insensitive)
function pickCol(row, candidates) {
  const keys = Object.keys(row || {});
  for (const want of candidates) {
    const hit = keys.find(k => k.toLowerCase() === want.toLowerCase());
    if (hit) return hit;
  }
  return null;
}

function maxNum(rows, col) {
  const xs = rows.map(r => Number(r[col])).filter(n => !Number.isNaN(n));
  return xs.length ? Math.max(...xs) : null;
}

export async function processZip({ client, domain, runDate, zipUrl, fetchImpl }) {
  const manifest = {};

  // Download ZIP
  const res = await fetchImpl(zipUrl);
  if (!res.ok) throw new Error(`Download failed: ${res.status}`);
  const zipBuf = Buffer.from(await res.arrayBuffer());
  const zip    = new AdmZip(zipBuf);

  const out  = emptyNormalized(client, domain, runDate);
  const prov = out.provenance;

  // -------- Ahrefs Keywords
  let buf = readEntry(zip, 'ahrefs_keywords.csv', manifest);
  if (buf) {
    const rows = parseCsvSmart(buf);
    if (rows.length) {
      const posCol = pickCol(rows[0], ['current position','position']);
      const pos = posCol
        ? rows.map(r => Number(r[posCol])).filter(n => !Number.isNaN(n))
        : [];
      out.onsite.keywords.top3   = pos.filter(p => p <= 3).length;
      out.onsite.keywords.top10  = pos.filter(p => p <= 10).length;
      out.onsite.keywords.top100 = pos.filter(p => p <= 100).length;
      prov.ahrefs = true;
      manifest['ahrefs_keywords.csv'].rows = rows.length;
    } else manifest['ahrefs_keywords.csv'].status = 'partial';
  }

  // -------- Ahrefs Top Pages
  buf = readEntry(zip, 'ahrefs_top_pages.csv', manifest);
  if (buf) {
    const rows = parseCsvSmart(buf);
    if (rows.length) {
      const urlCol = pickCol(rows[0], ['url','page url','address']);
      out.onsite.content.pages_total =
        out.onsite.content.pages_total ??
        (urlCol ? new Set(rows.map(r => r[urlCol])).size : rows.length);
      prov.ahrefs = true;
      manifest['ahrefs_top_pages.csv'].rows = rows.length;
    } else manifest['ahrefs_top_pages.csv'].status = 'partial';
  }

  // -------- Ahrefs Referring Domains
  buf = readEntry(zip, 'ahrefs_backlinks.csv', manifest);
  if (buf) {
    const rows = parseCsvSmart(buf);
    if (rows.length) {
      const drCol = pickCol(rows[0], ['dr','domain rating']);
      out.backlinks.ref_domains = rows.length;
      if (drCol) {
        const nums = rows.map(r => Number(r[drCol])).filter(n => !Number.isNaN(n));
        if (nums.length) out.backlinks.dr = nums.reduce((a,b)=>a+b, 0) / nums.length;
      }
      prov.ahrefs = true;
      manifest['ahrefs_backlinks.csv'].rows = rows.length;
    } else manifest['ahrefs_backlinks.csv'].status = 'partial';
  }

  // -------- Ahrefs Site Audit (nested zip)
  buf = readEntry(zip, 'ahrefs_site_audit.zip', manifest);
  if (buf) {
    try {
      const inner = new AdmZip(buf);
      const issues = {
        "4xx":             ["Error-4XX_page.csv","Error-404_page.csv"],
        "5xx":             ["Error-5XX_page.csv"],
        "redirect_chains": ["Error-Redirect_chain.csv","Warning-3XX_redirect.csv"],
        "canonical":       ["Error-indexable-Canonical_chain.csv","Warning-Canonical_to_redirected_URL.csv"],
        "duplicate_titles":["Warning-indexable-Title_tag_duplicate.csv"],
        "thin":            ["Warning-indexable-Content_thin.csv"],
        "orphan_pages":    ["Error-indexable-Orphan_page.csv"]
      };
      for (const [key, files] of Object.entries(issues)) {
        let c = 0;
        for (const f of files) {
          const ent = inner.getEntry(f);
          if (!ent) continue;
          const d = parseCsvSmart(ent.getData());
          c += d.length;
        }
        out.onsite.errors[key] += c;
      }
      prov.ahrefs = true;
      manifest['ahrefs_site_audit.zip'].status = 'full';
    } catch (e) {
      manifest['ahrefs_site_audit.zip'].status = 'partial';
      manifest['ahrefs_site_audit.zip'].note   = String(e);
    }
  }

  // -------- Screaming Frog: internal all
  buf = readEntry(zip, 'sf_internal_all.csv', manifest);
  if (buf) {
    const rows = parseCsvSmart(buf);
    if (rows.length) {
      prov.screamingfrog = true;
      manifest['sf_internal_all.csv'].rows = rows.length;
      const scCol = pickCol(rows[0], ['status code','status']);
      if (scCol) {
        const sc = rows.map(r => Number(r[scCol])).filter(n => !Number.isNaN(n));
        out.onsite.errors['4xx'] += sc.filter(n => n >= 400 && n < 500).length;
        out.onsite.errors['5xx'] += sc.filter(n => n >= 500).length;
      }
      out.onsite.content.pages_total = out.onsite.content.pages_total ?? rows.length;
    } else manifest['sf_internal_all.csv'].status = 'partial';
  }

  // Structured data (booleans)
  buf = readEntry(zip, 'sf_structured_data.csv', manifest);
  if (buf) {
    const rows = parseCsvSmart(buf);
    if (rows.length) {
      const elCol = pickCol(rows[0], ['element','type']);
      if (elCol) {
        const el = rows.map(r => String(r[elCol]).toLowerCase());
        const has = s => el.some(e => e.includes(s));
        out.onsite.schema.organization  = has('organization');
        out.onsite.schema.localbusiness = has('localbusiness');
        out.onsite.schema.service       = has('service');
        out.onsite.schema.faq           = has('faq');
        out.onsite.schema.review        = has('review');
        prov.screamingfrog = true;
        manifest['sf_structured_data.csv'].rows = rows.length;
      } else manifest['sf_structured_data.csv'].status = 'partial';
    }
  }

  // Duplicates (info only)
  buf = readEntry(zip, 'sf_duplicates.csv', manifest);
  if (buf) manifest['sf_duplicates.csv'].rows = parseCsvSmart(buf).length;

  // Images (info only)
  buf = readEntry(zip, 'sf_images.csv', manifest);
  if (buf) manifest['sf_images.csv'].rows = parseCsvSmart(buf).length;

  // -------- Lighthouse JSONs
  const lhFiles = ['lighthouse_home.json','lighthouse_service.json','lighthouse_city.json'];
  const lh = [];
  for (const f of lhFiles) {
    const ent = zip.getEntry(f);
    if (!ent) { manifest[f] = { status: 'missing' }; continue; }
    try {
      const obj = JSON.parse(ent.getData().toString('utf8'));
      const audits = obj?.audits || {};
      const perf   = obj?.categories?.performance?.score ?? null;
      const getNum = k => audits[k]?.numericValue ?? null;
      lh.push({
        lcp_ms:  getNum('largest-contentful-paint'),
        cls:     getNum('cumulative-layout-shift'),
        inp_ms:  audits['interactive']?.numericValue ?? null,
        ttfb_ms: audits['server-response-time']?.numericValue ?? null,
        perf_score: perf
      });
      manifest[f] = { status: 'full' };
      prov.lighthouse = true;
    } catch (e) {
      manifest[f] = { status: 'partial', note: String(e) };
    }
  }
  if (lh.length) {
    const nums = a => a.filter(v => v !== null);
    const p75  = a => {
      if (!a.length) return 'missing';
      const s = [...a].sort((x,y)=>x-y);
      const i = Math.floor(0.75*(s.length-1));
      return s[i];
    };
    const lcp = nums(lh.map(x=>x.lcp_ms));
    const cls = nums(lh.map(x=>x.cls));
    const inp = nums(lh.map(x=>x.inp_ms));
    out.onsite.cwv.lcp_p75 = lcp.length ? p75(lcp) : 'missing';
    out.onsite.cwv.cls_p75 = cls.length ? p75(cls) : 'missing';
    out.onsite.cwv.inp_p75 = inp.length ? p75(inp) : 'missing';
    // pass rate across URLs
    let pass=0, total=0;
    for (const m of lh) {
      if (m.lcp_ms===null || m.cls===null || m.inp_ms===null) continue;
      total++;
      if (m.lcp_ms<=2500 && m.cls<=0.1 && m.inp_ms<=200) pass++;
    }
    out.onsite.cwv.pass_rate = total ? pass/total : 'missing';
  }

  // -------- BrightLocal Ranks (flexible)
  buf = readEntry(zip, 'brightlocal_ranks.csv', manifest);
  if (buf) {
    const rows = parseCsvSmart(buf);
    if (rows.length) {
      prov.brightlocal = true;
      manifest['brightlocal_ranks.csv'].rows = rows.length;

      const posCol = pickCol(rows[0], ['position','rank','serp position','pos']);
      if (posCol) {
        const posVals = rows.map(r => Number(r[posCol])).filter(n => !Number.isNaN(n) && n > 0);
        if (posVals.length) {
          const avg = posVals.reduce((a,b)=>a+b, 0) / posVals.length;
          out.local.rank.avg_pos  = Math.round(avg * 10) / 10;
          out.local.rank.pct_top3 = posVals.filter(n => n <= 3).length / posVals.length;
          out.local.rank.keywords_tracked = posVals.length;
        } else {
          out.local.rank.keywords_tracked = rows.length;
        }
      }
    }
  }

  // -------- BrightLocal Citations (flexible)
  buf = readEntry(zip, 'brightlocal_citations.csv', manifest);
  if (buf) {
    const rows = parseCsvSmart(buf);
    if (rows.length) {
      prov.brightlocal = true;
      manifest['brightlocal_citations.csv'].rows = rows.length;

      const cCol = pickCol(rows[0], ['consistency','nap consistency','score','citation score','accuracy']);
      if (cCol) {
        const nums = rows
          .map(r => Number(String(r[cCol]).replace('%','')))
          .filter(n => !Number.isNaN(n));
        if (nums.length) {
          const val = nums.reduce((a,b)=>a+b, 0) / nums.length;
          out.local.citations.consistency = (val > 1) ? (val / 100) : val; // 0..1
        }
      }
    }
  }

  // -------- BL Reviews (often placeholder)
  buf = readEntry(zip, 'brightlocal_reviews.csv', manifest);
  if (buf) {
    const rows = parseCsvSmart(buf);
    if (rows.length && !(rows[0].status && rows[0].message)) {
      prov.brightlocal = true;
      manifest['brightlocal_reviews.csv'].rows = rows.length;
      // If your export includes total/rating columns, map them here.
    } else {
      manifest['brightlocal_reviews.csv'].status = 'placeholder';
      manifest['brightlocal_reviews.csv'].note   = 'login_required';
    }
  }

  // -------- BL/GBP public listing: read only public metrics
  buf = readEntry(zip, 'brightlocal_gbp_insights.csv', manifest);
  if (buf) {
    const rows = parseCsvSmart(buf);
    if (rows.length) {
      const colReviews = pickCol(rows[0], ['review count','reviews','reviews_total']);
      const colRating  = pickCol(rows[0], ['star rating','rating','reviews_average_rating']);
      const colPhotos  = pickCol(rows[0], ['photos','photos_total']);
      if (colReviews) out.local.reviews.count_total = maxNum(rows, colReviews);
      if (colRating)  out.local.reviews.avg_rating  = maxNum(rows, colRating);
      if (colPhotos)  out.local.gbp.photos_total    = maxNum(rows, colPhotos);
      prov.brightlocal = true;
      manifest['brightlocal_gbp_insights.csv'] = {
        status: 'partial',
        rows: rows.length,
        note: 'public listing only; true Insights (calls/clicks/directions) missing'
      };
    }
  }

  // -------- GBP categories/photos (safe)
  buf = readEntry(zip, 'gbp_categories.csv', manifest);
  if (buf) {
    const rows = parseCsvSmart(buf);
    const prim = rows
      .filter(r => String(r['category_type']).toLowerCase() === 'primary')
      .map(r => r['category_name'])
      .filter(Boolean);
    out.local.gbp.primary_category = prim.length ? String(prim[0]) : null;
    out.local.gbp.secondary_categories = rows
      .filter(r => String(r['category_type']).toLowerCase() === 'secondary')
      .map(r => r['category_name'])
      .filter(Boolean);
    prov.gbp_public = true;
    manifest['gbp_categories.csv'].rows = rows.length;
  }

  buf = readEntry(zip, 'gbp_photos.csv', manifest);
  if (buf) {
    const rows = parseCsvSmart(buf);
    const totalRow = rows.find(r => String(r['photo_type']).toLowerCase() === 'total');
    if (totalRow) out.local.gbp.photos_total = Number(totalRow['count']);
    prov.gbp_public = true;
    manifest['gbp_photos.csv'].rows = rows.length;
  }

  // -------- Login-required placeholders (mark present/missing)
  for (const name of [
    'surfer_page_queue.csv','gsc_queries_28d.csv','gsc_pages_28d.csv',
    'ga4_pages.csv','ga4_conversions.csv','ga4_channels.csv',
    'leadsnap_leads.csv','leadsnap_calls.csv','leadsnap_reviews.csv'
  ]) {
    const ent = zip.getEntry(name);
    if (!ent) { manifest[name] = { status: 'missing' }; continue; }
    const rows = parseCsvSmart(ent.getData());
    if (rows.length && !(rows[0].status && rows[0].message)) manifest[name] = { status: 'full', rows: rows.length };
    else manifest[name] = { status: 'placeholder', note: 'access_required_or_empty' };
  }

  // -------- Presence flags for GSC/GA4 if real tables exist
  for (const name of ['gsc_queries_28d.csv','gsc_pages_28d.csv']) {
    const ent = zip.getEntry(name);
    if (!ent) continue;
    const rows = parseCsvSmart(ent.getData());
    if (rows.length && !(rows[0].status && rows[0].message)) out.provenance.gsc = 'present';
  }
  for (const name of ['ga4_pages.csv','ga4_conversions.csv','ga4_channels.csv']) {
    const ent = zip.getEntry(name);
    if (!ent) continue;
    const rows = parseCsvSmart(ent.getData());
    if (rows.length && !(rows[0].status && rows[0].message)) out.provenance.ga4 = 'present';
  }

  // -------- Compute proportional scores
  const scores = computeScores(out);

  return { normalized_audit: out, scores, manifest };
}
