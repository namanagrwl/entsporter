const { Client } = require('@elastic/enterprise-search');
const fs = require('fs/promises');

// Logging helper - respects quiet mode
function log(message, quiet = false) {
  if (!quiet) {
    console.log(message);
  }
}

async function exportAppSearchEngine(engineName, options) {
  const quiet = options.quiet || false;
  
  log(`Exporting App Search engine ${engineName}, host: ${options.appSearchEndpoint}`, quiet);
  
  const client = new Client({
    url: options.appSearchEndpoint,
    auth: { token: options.appSearchPrivateKey }
  });

  const engine = await client.app.getEngine({ engine_name: engineName });
  if (engine.errors) {
    console.error('Export failed:', engine.errors);
    throw new Error(`Failed to get engine: ${JSON.stringify(engine.errors)}`);
  }

  const engineJson = {
    read_only: { name: engine.name, type: engine.type, language: engine.language }
  };

  // Run exports in sequence
  engineJson.schema = await exportSchema(client, engineName, quiet);
  engineJson.synonyms = await exportSynonyms(client, engineName, quiet);
  engineJson.curations = await exportCurations(client, engineName, quiet);
  engineJson.searchSettings = await exportSearchSettings(client, engineName, quiet);
  engineJson.crawler = await exportCrawlerConfigViaRest(engineName, options, quiet);

  log(`Writing to ${options.outputJson}`, quiet);
  await fs.writeFile(options.outputJson, JSON.stringify(engineJson, undefined, 2));
  
  log(`Export complete`, quiet);
}

/* -----------------------
   SDK helpers
   ----------------------- */

async function exportSchema(client, engineName, quiet = false) {
  const schema = await client.app.getSchema({ engine_name: engineName });
  if (schema.errors) {
    console.error('Schema export failed:', schema.errors);
    throw new Error(`Schema export failed: ${JSON.stringify(schema.errors)}`);
  }
  
  const fieldCount = Object.keys(schema).length;
  log(`  Schema: ${fieldCount} fields`, quiet);
  
  return schema;
}

async function exportSearchSettings(client, engineName, quiet = false) {
  const searchSettings = await client.app.getSearchSettings({ engine_name: engineName });
  if (searchSettings.errors) {
    console.error('Search settings export failed:', searchSettings.errors);
    throw new Error(`Search settings export failed: ${JSON.stringify(searchSettings.errors)}`);
  }
  
  log(`  Search settings: exported`, quiet);
  return searchSettings;
}

// Generic SDK pagination helper
async function fetchAllPagesSDK(fetchFn, initialParams = {}) {
  const allResults = [];
  let page = 1;

  while (true) {
    const params = Object.assign({}, initialParams, { page: { current: page } });
    const resp = await fetchFn(params);

    if (!resp) break;
    if (resp.errors) {
      console.error('Pagination error:', resp.errors);
      throw new Error(`Pagination failed: ${JSON.stringify(resp.errors)}`);
    }

    const pageResults = resp.results || resp.synonym_sets || resp.curations || [];
    allResults.push(...pageResults);

    const metaPage = resp.meta && resp.meta.page;
    if (!metaPage || page >= metaPage.total_pages) break;
    page += 1;
  }

  return allResults;
}

async function exportSynonyms(client, engineName, quiet = false) {
  const fetchFn = (params) => client.app.listSynonymSets(Object.assign({ engine_name: engineName }, params));
  const synonyms = await fetchAllPagesSDK(fetchFn);
  
  log(`  Synonyms: ${synonyms.length} sets`, quiet);
  return synonyms;
}

async function exportCurations(client, engineName, quiet = false) {
  const fetchFn = (params) => client.app.listCurations(Object.assign({ engine_name: engineName }, params));
  const curations = await fetchAllPagesSDK(fetchFn);
  
  log(`  Curations: ${curations.length}`, quiet);
  return curations;
}

/* -----------------------
   REST helpers for crawler
   ----------------------- */

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

async function exportCrawlerConfigViaRest(engineName, options, quiet = false) {
  const baseRoot = options.appSearchEndpoint.replace(/\/$/, '') +
    `/api/as/v1/engines/${encodeURIComponent(engineName)}/crawler`;
  const apiKey = options.appSearchPrivateKey;

  // Safe wrapper that returns [] on non-OK/404
  async function safeRestFetchAll(url) {
    try {
      return await restFetchAllPages(url, apiKey);
    } catch (err) {
      // Only log warnings in verbose mode
      if (!quiet) {
        console.warn(`Could not fetch ${url}:`, err.message || err);
      }
      return [];
    }
  }

  // 1) Try /domains first (may contain nested data)
  const domainsUrl = `${baseRoot}/domains`;
  let domains = [];
  try {
    domains = await restFetchAllPages(domainsUrl, apiKey);
  } catch (err) {
    if (!quiet) {
      console.warn('Could not fetch crawler domains:', err.message || err);
    }
    domains = [];
  }

  // Check if domains contain nested arrays
  const hasNested = domains.length > 0 && domains.every(d =>
    d &&
    (Array.isArray(d.entry_points) || Array.isArray(d.entryPoints)) &&
    (Array.isArray(d.crawl_rules) || Array.isArray(d.crawlRules)) &&
    Array.isArray(d.sitemaps)
  );

  if (hasNested) {
    const entryPoints = [];
    const crawlRules = [];
    const sitemaps = [];

    const normalizedDomains = domains.map(d => {
      const normalized = Object.assign({}, d);
      normalized.entry_points = normalized.entry_points || normalized.entryPoints || [];
      normalized.crawl_rules = normalized.crawl_rules || normalized.crawlRules || [];
      normalized.sitemaps = normalized.sitemaps || d.sitemaps || [];

      normalized.entry_points.forEach(ep => entryPoints.push(Object.assign({ domain_id: normalized.id }, ep)));
      normalized.crawl_rules.forEach(cr => crawlRules.push(Object.assign({ domain_id: normalized.id }, cr)));
      normalized.sitemaps.forEach(sm => sitemaps.push(Object.assign({ domain_id: normalized.id }, sm)));

      return normalized;
    });
    
    log(`  Crawler: ${domains.length} domains, ${entryPoints.length} entry points, ${crawlRules.length} rules, ${sitemaps.length} sitemaps`, quiet);
    
    return { domains: normalizedDomains, entryPoints, crawlRules, sitemaps };
  }

  // 2) Fallback: fetch separately
  const [entryPoints, crawlRules, sitemaps] = await Promise.all([
    safeRestFetchAll(`${baseRoot}/entry_points`),
    safeRestFetchAll(`${baseRoot}/crawl_rules`),
    safeRestFetchAll(`${baseRoot}/sitemaps`)
  ]);

  // If domains empty, try to populate
  if (!domains || domains.length === 0) {
    domains = await safeRestFetchAll(domainsUrl);
  }

  log(`  Crawler: ${domains.length} domains, ${entryPoints.length} entry points, ${crawlRules.length} rules, ${sitemaps.length} sitemaps`, quiet);

  return { domains, entryPoints, crawlRules, sitemaps };
}

module.exports = exportAppSearchEngine;