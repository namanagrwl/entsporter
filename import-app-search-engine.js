
const { Client } = require('@elastic/enterprise-search')
const fs = require('fs/promises');

async function deleteEngine(client, engineName) {
  console.log(`Deleting existing engine ${engineName}...`);
  try {
    const result = await client.app.deleteEngine({
      engine_name: engineName
    });
    if (result.errors) {
      console.error('Error deleting engine:', result.errors);
      throw new Error('Failed to delete engine');
    }
    // Wait a moment for the deletion to propagate
    await new Promise(resolve => setTimeout(resolve, 5000));
    console.log(`✓ Engine ${engineName} deleted successfully`);
    
  } catch (err) {
    // If engine doesn't exist, that's actually fine for --force
    if (err.statusCode === 404 || err.message?.includes('not found')) {
      console.log(`Engine ${engineName} does not exist, proceeding...`);
    } else {
      throw err;
    }
  }
}


async function importAppSearchEngine(engineName, options) {
  console.log(`Importing App Search engine settings into ${engineName}, host: ${options.appSearchEndpoint}`);
  const client = new Client({
    url: options.appSearchEndpoint,
    auth: {
      token: options.appSearchPrivateKey
    }
  });

  console.log(`Reading engine settings from ${options.inputJson}`);
  const engineJson = JSON.parse(await fs.readFile(options.inputJson, { encoding: 'utf8' }));
  console.dir(engineJson);

  await createEngine(client, engineName, engineJson, options);
  await importSchema(client, engineName, engineJson);
  await importSynonyms(client, engineName, engineJson);
  await importCurations(client, engineName, engineJson);
  await importSearchSettings(client, engineName, engineJson);

  // CHANGED: import crawler via REST if crawler data present
  if (engineJson.crawler) {
    try {
      await importCrawlerViaRest(engineName, engineJson.crawler, options);
    } catch (err) {
      console.error('Crawler import failed:', err && err.message ? err.message : err);
      // Do not exit the whole process — warn and continue. If you prefer to abort, uncomment next line:
      // process.exit(1);
    }
  }
}

async function createEngine(client, engineName, engineJson, options) {
  console.log(`Creating engine ${engineName}`);

  // Check if engine exists
  let engineExists = false;
  try {
    const existingEngine = await client.app.getEngine({
      engine_name: engineName
    });
    engineExists = true;
  } catch (err) {
    // Engine does not exist - this is expected for new imports
    if (err.statusCode === 404 || err.message?.includes('not found')) {
      engineExists = false;
    } else {
      // Unexpected error (network, auth, etc.)
      console.error('Error checking if engine exists:', err.message || err);
      throw err;
    }
  }

  // Handle existing engine based on --force flag
  if (engineExists) {
    if (options.force) {
      console.log(`Engine ${engineName} already exists. --force flag detected, deleting...`);
      await deleteEngine(client, engineName);
    } else {
      console.error(`Engine ${engineName} already exists. Use --force to delete and recreate.`);
      process.exit(1);
    }
  }

  // Create the engine
  const newEngineSettings = {
    name: engineName,
  }
  if (engineJson.read_only?.language) {
    newEngineSettings.language = engineJson.read_only.language;
  }
  console.log(`New engine settings:`);
  console.dir(newEngineSettings);
  const result = await client.app.createEngine(newEngineSettings);
  if (result.errors) {
    console.error('Error creating engine:', result.errors);
    process.exit(1);
  }

  console.log(`Engine ${engineName} created successfully`);
}

// async function importSchema(client, engineName, engineJson) {
//   console.log(`Updating schema`);
//   const result = await client.app.putSchema({
//     engine_name: engineName,
//     schema: engineJson.schema
//   });
//   if (result.errors) {
//     console.error(result.errors)
//     process.exit(1)
//   }
// }

async function importSchema(client, engineName, engineJson) {
  console.log(`Updating schema in batches of 64 fields...`);

  const fullSchema = engineJson.schema;
  const fieldNames = Object.keys(fullSchema);

  // Split into chunks of 64 fields
  const chunkSize = 64;
  for (let i = 0; i < fieldNames.length; i += chunkSize) {
    const chunk = fieldNames.slice(i, i + chunkSize);
    
    // Build a schema object containing only this batch
    const batchSchema = {};
    for (const f of chunk) {
      batchSchema[f] = fullSchema[f];
    }

    console.log(`Pushing schema batch: fields ${i + 1} to ${i + chunk.length}`);

    const result = await client.app.putSchema({
      engine_name: engineName,
      schema: batchSchema
    });

    if (result.errors) {
      console.error("Error in schema batch:", result.errors);
      process.exit(1);
    }
  }

  console.log("Schema import completed successfully.");
}

async function importSynonyms(client, engineName, engineJson) {
  console.log('Importing synonyms');
  if (!Array.isArray(engineJson.synonyms)) {
    console.log('No synonyms to import.');
    return;
  }
  for (const synonymSet of engineJson.synonyms) {
    try {
      const result = await client.app.createSynonymSet({
        engine_name: engineName,
        synonyms: synonymSet.synonyms,
      });
      if (result && result.errors) {
        console.error(result.errors)
        process.exit(1)
      }
    } catch (err) {
      console.error('createSynonymSet failed:', err && err.message ? err.message : err);
      process.exit(1);
    }
  }
  console.log('Synonyms import complete.');
}

async function importCurations(client, engineName, engineJson) {
  console.log(`Importing curations`);
  if (!Array.isArray(engineJson.curations)) {
    console.log('No curations to import.');
    return;
  }
  for (const curation of engineJson.curations) {
    try {
      const result = await client.app.createCuration({
        engine_name: engineName,
        queries: curation.queries,
        promoted_doc_ids: curation.promoted,
        hidden_doc_ids: curation.hidden,
      });
      if (result && result.errors) {
        console.error(result.errors)
        process.exit(1)
      }
    } catch (err) {
      console.error('createCuration failed:', err && err.message ? err.message : err);
      process.exit(1);
    }
  }
  console.log('Curations import complete.');
}

async function importSearchSettings(client, engineName, engineJson) {
  console.log(`Importing search settings`);
  const searchSettings = {
    engine_name: engineName,
    body: {},
  }
  if (engineJson.searchSettings.search_fields) {
    searchSettings.body.search_fields = engineJson.searchSettings.search_fields;
  }
  if (engineJson.searchSettings.result_fields) {
    searchSettings.body.result_fields = engineJson.searchSettings.result_fields;
  }
  if (engineJson.searchSettings.boosts) {
    searchSettings.body.boosts = engineJson.searchSettings.boosts;
  }
  if (engineJson.searchSettings.precision) {
    searchSettings.body.precision = engineJson.searchSettings.precision;
  }
  const result = await client.app.putSearchSettings(searchSettings);
  if (result.errors) {
    console.error(result.errors)
    process.exit(1)
  }
}

/* -------------------------
   CRAWLER import via REST (domains, entry_points, crawl_rules, sitemaps)
   ------------------------- */
async function importCrawlerViaRest(engineName, crawlerObj, options) {
  console.log('Importing crawler config via REST (domains, entry_points, crawl_rules, sitemaps)...');

  // ensure fetch exists (Node 18+)
  const _fetch = global.fetch || (await import('node-fetch').then(m => m.default));

  const baseRoot = options.appSearchEndpoint.replace(/\/$/, '') +
    `/api/as/v1/engines/${encodeURIComponent(engineName)}/crawler`;
  const apiKey = options.appSearchPrivateKey;

  // Generic top-level POST (for domains only)
  async function restPost(path, body) {
    const url = `${baseRoot}${path}`;
    const resp = await _fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    });
    if (!resp.ok) {
      const text = await resp.text().catch(() => '<no body>');
      throw new Error(`POST ${url} => ${resp.status} ${resp.statusText}: ${text}`);
    }
    return resp.json();
  }

  // Domain-scoped POST: /domains/{id}/entry_points, /crawl_rules, /sitemaps
  async function restPostDomain(domainId, subpath, body) {
    const url = `${baseRoot}/domains/${encodeURIComponent(domainId)}${subpath}`;
    const resp = await _fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    });
    if (!resp.ok) {
      const text = await resp.text().catch(() => '<no body>');
      throw new Error(`POST ${url} => ${resp.status} ${resp.statusText}: ${text}`);
    }
    return resp.json();
  }

  /* --------------------
     1) IMPORT DOMAINS
     -------------------- */
  const domains = crawlerObj.domains || [];
  const createdDomainMap = {};

  for (const d of domains) {
    const domainBody = { name: d.name || d.url || d.domain || '' };
    if (d.default_crawl_rule) domainBody.default_crawl_rule = d.default_crawl_rule;

    try {
      const created = await restPost('/domains', domainBody);
      const createdId = created.id;
      const origKey = d.id || d.name || d.url;

      if (origKey) createdDomainMap[origKey] = createdId;

      console.log(`Created domain ${domainBody.name} => id ${createdId}`);
    } catch (err) {
      console.warn('Could not create domain (continuing):', err.message);
    }
  }

  /* --------------------
     2) IMPORT ENTRY POINTS
     -------------------- */
  const entryPoints = crawlerObj.entryPoints || crawlerObj.entry_points || [];
  for (const ep of entryPoints) {
    const domainId = createdDomainMap[ep.domain_id] || ep.domain_id;
    if (!domainId) {
      console.warn(`Skipping entry_point "${ep.value}" — no domain_id`);
      continue;
    }

    const body = { value: ep.value };

    try {
      const created = await restPostDomain(domainId, '/entry_points', body);
      console.log(`Created entry_point ${ep.value} under domain ${domainId}`);
    } catch (err) {
      console.warn('Could not create entry_point:', err.message);
    }
  }

  /* --------------------
     3) IMPORT CRAWL RULES
     -------------------- */
  const crawlRules = crawlerObj.crawlRules || crawlerObj.crawl_rules || [];
  for (const cr of crawlRules) {
    const domainId = createdDomainMap[cr.domain_id] || cr.domain_id;
    if (!domainId) {
      console.warn(`Skipping crawl_rule — no domain_id`);
      continue;
    }

    const body = {
      policy: cr.policy,
      rule: cr.rule,
      pattern: cr.pattern,
      order: cr.order
    };

    try {
      const created = await restPostDomain(domainId, '/crawl_rules', body);
      console.log(`Created crawl_rule under domain ${domainId}`);
    } catch (err) {
      console.warn('Could not create crawl_rule:', err.message);
    }
  }

  /* --------------------
     4) IMPORT SITEMAPS
     -------------------- */
  const sitemaps = crawlerObj.sitemaps || [];
  for (const sm of sitemaps) {
    const domainId = createdDomainMap[sm.domain_id] || sm.domain_id;
    if (!domainId) {
      console.warn(`Skipping sitemap — no domain_id`);
      continue;
    }

    const body = { url: sm.url || sm.value };

    try {
      const created = await restPostDomain(domainId, '/sitemaps', body);
      console.log(`Created sitemap ${body.url} under domain ${domainId}`);
    } catch (err) {
      console.warn('Could not create sitemap:', err.message);
    }
  }

  console.log('Crawler import (REST) finished.');
}


module.exports = importAppSearchEngine;
