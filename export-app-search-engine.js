const { Client } = require('@elastic/enterprise-search');
const fs = require('fs/promises');

// CHANGED: Added SDK pagination helper and REST crawler export (uses global fetch)

async function exportAppSearchEngine(engineName, options) {
  console.log(`Exporting App Search engine ${engineName}, host: ${options.appSearchEndpoint}`);
  const client = new Client({
    url: options.appSearchEndpoint,
    auth: { token: options.appSearchPrivateKey }
  });

  const engine = await client.app.getEngine({ engine_name: engineName });
  if (engine.errors) {
    console.error(engine.errors);
    process.exit(1);
  }

  const engineJson = {
    read_only: { name: engine.name, type: engine.type, language: engine.language }
  };

  // CHANGED: run exports in sequence (preserve ordering)
  engineJson.schema = await exportSchema(client, engineName);
  engineJson.synonyms = await exportSynonyms(client, engineName);
  engineJson.curations = await exportCurations(client, engineName);
  engineJson.searchSettings = await exportSearchSettings(client, engineName); // includes result_fields
  engineJson.crawler = await exportCrawlerConfigViaRest(engineName, options); // REST fallback (may be {})

  console.log(`Writing engine JSON to file ${options.outputJson}`);
  await fs.writeFile(options.outputJson, JSON.stringify(engineJson, undefined, 2));

  console.dir(engineJson);
}

/* -----------------------
   SDK helpers (pagination for list endpoints)
   ----------------------- */

async function exportSchema(client, engineName) {
  const schema = await client.app.getSchema({ engine_name: engineName });
  if (schema.errors) {
    console.error(schema.errors);
    process.exit(1);
  }
  return schema;
}

async function exportSearchSettings(client, engineName) {
  const searchSettings = await client.app.getSearchSettings({ engine_name: engineName });
  if (searchSettings.errors) {
    console.error(searchSettings.errors);
    process.exit(1);
  }
  return searchSettings;
}

// CHANGED: generic SDK pagination helper for client.app.* list endpoints
async function fetchAllPagesSDK(fetchFn, initialParams = {}) {
  const allResults = [];
  let page = 1;

  while (true) {
    const params = Object.assign({}, initialParams, { page: { current: page } });
    const resp = await fetchFn(params);

    if (!resp) break;
    if (resp.errors) {
      console.error(resp.errors);
      process.exit(1);
    }

    const pageResults = resp.results || resp.synonym_sets || resp.curations || [];
    allResults.push(...pageResults);

    const metaPage = resp.meta && resp.meta.page;
    if (!metaPage || page >= metaPage.total_pages) break;
    page += 1;
  }

  return allResults;
}

async function exportSynonyms(client, engineName) {
  const fetchFn = (params) => client.app.listSynonymSets(Object.assign({ engine_name: engineName }, params));
  return await fetchAllPagesSDK(fetchFn);
}

async function exportCurations(client, engineName) {
  const fetchFn = (params) => client.app.listCurations(Object.assign({ engine_name: engineName }, params));
  return await fetchAllPagesSDK(fetchFn);
}

/* -----------------------
   REST helpers (for crawler endpoints not available in SDK)
   Uses global fetch (Node v18+)
   ----------------------- */

// CHANGED: REST pagination helper using query params page[current] & page[size]
async function restFetchAllPages(urlBase, apiKey, pageSize = 100) {
  const all = [];
  let page = 1;
  const base = urlBase.replace(/\/$/, '');

  while (true) {
    const url = new URL(base);
    url.searchParams.set('page[current]', String(page));
    url.searchParams.set('page[size]', String(pageSize));

    const resp = await fetch(url.toString(), {
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json'
      }
    });

    if (!resp.ok) {
      const text = await resp.text().catch(() => '<no body>');
      throw new Error(`REST fetch failed ${resp.status} ${resp.statusText}: ${text}`);
    }

    const json = await resp.json();
    const pageResults = json.results || json.synonym_sets || json.curations || json.domains || json.entry_points || json.crawl_rules || json.sitemaps || [];
    all.push(...pageResults);

    const metaPage = json.meta && json.meta.page;
    if (!metaPage || page >= metaPage.total_pages) break;
    page += 1;
  }

  return all;
}

// CHANGED: smarter crawler export - prefer /domains nested data, fallback to separate endpoints
async function exportCrawlerConfigViaRest(engineName, options) {
  const baseRoot = options.appSearchEndpoint.replace(/\/$/, '') +
    `/api/as/v1/engines/${encodeURIComponent(engineName)}/crawler`;
  const apiKey = options.appSearchPrivateKey;

  // safe wrapper that returns [] on non-OK/404
  async function safeRestFetchAll(url) {
    try {
      return await restFetchAllPages(url, apiKey);
    } catch (err) {
      console.warn(`Could not fetch ${url}:`, err.message || err);
      return [];
    }
  }

  // 1) Try /domains first (may contain nested entry_points, crawl_rules, sitemaps)
  const domainsUrl = `${baseRoot}/domains`;
  let domains = [];
  try {
    domains = await restFetchAllPages(domainsUrl, apiKey);
  } catch (err) {
    console.warn('Could not fetch crawler domains via /domains:', err.message || err);
    domains = [];
  }

  // Determine if domains already contain nested arrays we can use
  const hasNested = domains.length > 0 && domains.every(d =>
    (Array.isArray(d.entry_points) || Array.isArray(d.entryPoints)) &&
    (Array.isArray(d.crawl_rules) || Array.isArray(d.crawlRules)) &&
    (Array.isArray(d.sitemaps) || Array.isArray(d.sitemaps))
  );

  if (hasNested) {
    const entryPoints = [];
    const crawlRules = [];
    const sitemaps = [];

    const normalizedDomains = domains.map(d => {
      const normalized = Object.assign({}, d);
      normalized.entry_points = normalized.entry_points || normalized.entryPoints || [];
      normalized.crawl_rules = normalized.crawl_rules || normalized.crawlRules || [];
      normalized.sitemaps = normalized.sitemaps || normalized.sitemaps || [];

      normalized.entry_points.forEach(ep => entryPoints.push(Object.assign({ domain_id: normalized.id }, ep)));
      normalized.crawl_rules.forEach(cr => crawlRules.push(Object.assign({ domain_id: normalized.id }, cr)));
      normalized.sitemaps.forEach(sm => sitemaps.push(Object.assign({ domain_id: normalized.id }, sm)));

      return normalized;
    });
    console.log(normalizedDomains);
    console.log("---------------");
    console.log(entryPoints);
    console.log("---------------");
    console.log(crawlRules);
    console.log("---------------");
    console.log(sitemaps);
    return { domains: normalizedDomains, entryPoints, crawlRules, sitemaps };
  }

  console.log()
  // 2) Fallback: fetch entry_points, crawl_rules, sitemaps separately (paginated)
  const [entryPoints, crawlRules, sitemaps] = await Promise.all([
    safeRestFetchAll(`${baseRoot}/entry_points`),
    safeRestFetchAll(`${baseRoot}/crawl_rules`),
    safeRestFetchAll(`${baseRoot}/sitemaps`)
  ]);

  // If domains empty, try to populate them via safe fetch (some tenants need it)
  if (!domains || domains.length === 0) {
    domains = await safeRestFetchAll(domainsUrl);
  }

  return { domains, entryPoints, crawlRules, sitemaps };
}

module.exports = exportAppSearchEngine;
