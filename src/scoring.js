export function computeScores(out) {
  // Weights
  const ossW = { gsc_clicks: 30, kw_top10: 20, site_health: 20, cwv_pass: 15, indexed_valid: 15 };
  const lssW = { avg_local_rank: 40, pct_top3: 25, citations: 15, reviews: 10, gbp_actions: 10 };

  // Availability
  const ossAvail = {
    kw_top10: out.onsite.keywords.top10 !== null,
    site_health: true,
    cwv_pass: out.onsite.cwv.pass_rate !== "missing",
    gsc_clicks: false,
    indexed_valid: false
  };

  // Raw 0..1 component scores
  const s = {};
  try {
    const top10 = out.onsite.keywords.top10 || 0;
    const top100 = Math.max(out.onsite.keywords.top100 || 1, 1);
    s.kw_top10 = Math.min(top10 / top100, 1);
  } catch { s.kw_top10 = null; }

  s.cwv_pass = (out.onsite.cwv.pass_rate !== "missing") ? out.onsite.cwv.pass_rate : null;

  // Errors per page → site health (0 when EPP >= 0.5)
  const errs = out.onsite.errors;
  const totalErr = Object.values(errs).filter(v => Number.isInteger(v)).reduce((a,b)=>a+b, 0);
  const pages = out.onsite.content.pages_total || 100;
  const epp   = totalErr / pages;
  const BAD_EPP = 0.5;
  let health = 1 - (epp / BAD_EPP);
  health = Math.max(0, Math.min(1, health));
  s.site_health = health;

  s.gsc_clicks    = null;
  s.indexed_valid = null;

  const aggregate = (weights, avail, raw) => {
    const total = Object.values(weights).reduce((a,b)=>a+b, 0);
    let used = 0, acc = 0;
    for (const [k,w] of Object.entries(weights)) {
      if (avail[k] && raw[k] !== null) { used += w; acc += w * Number(raw[k]); }
    }
    const score = used ? Math.round((acc / used) * 1000) / 10 : 0;
    return { score, used, total };
  };

  const { score: oss, used: usedOss, total: totalOss } = aggregate(ossW, ossAvail, s);

  // LSS availability & raw
  const lAvail = {
    avg_local_rank: true,
    pct_top3: out.local.rank.pct_top3 !== null,
    citations: out.local.citations.consistency !== null && out.local.citations.consistency !== "missing",
    reviews: (out.local.reviews.avg_rating !== null || out.local.reviews.count_total !== null),
    gbp_actions: false
  };

  const ls = {};
  const avgPos = out.local.rank.avg_pos || 20;
  ls.avg_local_rank = Math.max(0, Math.min(1, 1 - (avgPos - 1) / 19));
  ls.pct_top3 = out.local.rank.pct_top3 || 0;
  const cons = out.local.citations.consistency;
  ls.citations = (cons !== null && cons !== "missing") ? Number(cons) : null;
  ls.reviews = (out.local.reviews.avg_rating !== null)
    ? Math.max(0, Math.min(1, (Number(out.local.reviews.avg_rating) - 3.5) / 1.5))
    : null;
  ls.gbp_actions = null;

  const { score: lss, used: usedLss, total: totalLss } = aggregate(lssW, lAvail, ls);

  return {
    oss, oss_coverage: Math.round((usedOss / totalOss) * 100) / 100, oss_weight_used: usedOss, oss_weight_total: totalOss,
    lss, lss_coverage: Math.round((usedLss / totalLss) * 100) / 100, lss_weight_used: usedLss, lss_weight_total: totalLss,
    components: { oss: { raw: s }, lss: { raw: ls } }
  };
}
