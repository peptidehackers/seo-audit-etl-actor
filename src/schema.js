export function emptyNormalized(client, domain, runDate) {
  return {
    meta: { client, domain, run_date: runDate },
    onsite: {
      site_health: null,
      errors: { "4xx": 0, "5xx": 0, redirect_chains: 0, canonical: 0, thin: 0, duplicate_titles: 0, orphan_pages: 0 },
      meta: { missing_title: 0, missing_description: 0, weak_title: 0 },
      schema: { organization: false, localbusiness: false, service: false, faq: false, review: false },
      cwv: { lcp_p75: "missing", cls_p75: "missing", inp_p75: "missing", pass_rate: "missing" },
      content: { pages_total: null, service_pages: null, location_pages: null, blog_posts: null, content_gap_terms: null },
      keywords: { top3: null, top10: null, top100: null }
    },
    local: {
      rank: { avg_pos: null, pct_top3: null, keywords_tracked: null },
      citations: { consistency: null, dupes: null, top_dirs_ok: null, top_dirs_total: null },
      reviews: { avg_rating: null, count_total: null, count_90d: null, response_rate: null },
      gbp: {
        primary_category: null,
        secondary_categories: [],
        photos_total: null,
        insights_calls: "missing",
        insights_directions: "missing",
        insights_website_clicks: "missing"
      }
    },
    backlinks: { ref_domains: null, new_90d: null, lost_90d: null, dr: null, anchor_brand_pct: null },
    provenance: {
      ahrefs: false,
      screamingfrog: false,
      lighthouse: false,
      brightlocal: false,
      gbp_public: false,
      gsc: "missing",
      ga4: "missing",
      leadsnap: "missing"
    }
  };
}
