const { Client } = require('@elastic/enterprise-search')
const fs = require('fs/promises');

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Logging helper - respects quiet mode
function log(message, quiet = false) {
  if (!quiet) {
    console.log(message);
  }
}

/**
 * Delete engine and wait for it to be truly gone
 */
async function deleteEngine(client, engineName, quiet = false) {
  log(`Deleting existing engine ${engineName}...`, quiet);
  
  // Step 1: Delete it
  try {
    await client.app.deleteEngine({ engine_name: engineName });
    log('  Deletion command sent', quiet);
  } catch (err) {
    if (err.statusCode === 404 || err.message?.includes('not found')) {
      log('  Engine does not exist', quiet);
      return;
    }
    throw err;
  }
  
  // Step 2: Wait until we get 404
  log('  Waiting for engine to be deleted...', quiet);
  const maxWait = 90000;
  const startTime = Date.now();
  
  while ((Date.now() - startTime) < maxWait) {
    await sleep(3000);
    
    try {
      await client.app.getEngine({ engine_name: engineName });
      const elapsed = Math.floor((Date.now() - startTime) / 1000);
      // Only log every 15 seconds in verbose mode
      if (!quiet && elapsed % 15 === 0) {
        console.log(`    Still exists... (${elapsed}s elapsed)`);
      }
    } catch (err) {
      if (err.statusCode === 404 || err.message?.includes('not found')) {
        const elapsed = Math.floor((Date.now() - startTime) / 1000);
        log(`  Engine returned 404 after ${elapsed}s`, quiet);
        break;
      }
    }
  }
  
  // Step 3: Wait additional time for backend to release the name
  log('  Waiting additional 15 seconds for name to be released...', quiet);
  await sleep(30000);
  
  log('  Deletion complete', quiet);
}

/**
 * Create engine with retry
 */
async function createEngine(client, engineName, engineJson, options) {
  const quiet = options.quiet || false;
  
  log(`\nCreating engine ${engineName}`, quiet);
  
  // Check if exists
  let exists = false;
  try {
    await client.app.getEngine({ engine_name: engineName });
    exists = true;
  } catch (err) {
    if (err.statusCode !== 404 && !err.message?.includes('not found')) {
      throw err;
    }
  }
  
  // Delete if exists and force
  if (exists) {
    if (options.force) {
      log('  Engine exists. --force flag detected, deleting...', quiet);
      await deleteEngine(client, engineName, quiet);
    } else {
      if (!quiet) {
        console.error(`\nEngine ${engineName} already exists. Use --force to overwrite.`);
      }
      throw new Error(`Engine ${engineName} already exists`);
    }
  }
  
  // Prepare settings
  const settings = {
    name: engineName,
  };
  if (engineJson.read_only?.language) {
    settings.language = engineJson.read_only.language;
  }
  
  // Try to create with retries
  const maxRetries = 5;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const result = await client.app.createEngine(settings);
      
      // Check for errors in response
      if (result?.errors) {
        const errorStr = JSON.stringify(result.errors);
        
        // Name still taken
        if (errorStr.includes('already taken') || errorStr.includes('already exists')) {
          if (attempt < maxRetries) {
            log(`  Name still taken, waiting 20s... (attempt ${attempt}/${maxRetries})`, quiet);
            await sleep(30000);
            continue;
          }
          throw new Error(`Engine "${engineName}" - Name still taken after ${maxRetries} attempts and ${maxRetries * 30}s wait`);
        }
        
        throw new Error(`Creation failed: ${errorStr}`);
      }
      
      log(`  Engine created successfully`, quiet);
      return;
      
    } catch (err) {
      if (attempt === maxRetries) {
        throw err;
      }
      log(`  Attempt ${attempt} failed, waiting 20s...`, quiet);
      await sleep(20000);
    }
  }
}

async function importAppSearchEngine(engineName, options) {
  const quiet = options.quiet || false;
  
  if (!quiet) {
    console.log(`\nImporting App Search engine: ${engineName}`);
    console.log(`Host: ${options.appSearchEndpoint}`);
    console.log(`Force mode: ${options.force ? 'ENABLED' : 'DISABLED'}\n`);
  }
  
  const client = new Client({
    url: options.appSearchEndpoint,
    auth: { token: options.appSearchPrivateKey }
  });

  const engineJson = JSON.parse(await fs.readFile(options.inputJson, 'utf8'));

  await createEngine(client, engineName, engineJson, options);
  await importSchema(client, engineName, engineJson, quiet);
  await importSynonyms(client, engineName, engineJson, quiet);
  await importCurations(client, engineName, engineJson, quiet);
  await importSearchSettings(client, engineName, engineJson, quiet);

  if (engineJson.crawler) {
    try {
      await importCrawlerViaRest(engineName, engineJson.crawler, options, quiet);
    } catch (err) {
      if (!quiet) {
        console.error('\nCrawler import failed:', err.message);
      }
    }
  }

  if (!quiet) {
    console.log('\n✅ Import complete!\n');
  }
}

async function importSchema(client, engineName, engineJson, quiet = false) {
  const schema = engineJson.schema;
  const fields = Object.keys(schema);
  
  if (fields.length === 0) return;

  const chunkSize = 64;
  for (let i = 0; i < fields.length; i += chunkSize) {
    const chunk = fields.slice(i, i + chunkSize);
    const batchSchema = {};
    chunk.forEach(f => batchSchema[f] = schema[f]);

    const result = await client.app.putSchema({
      engine_name: engineName,
      schema: batchSchema
    });

    if (result.errors) {
      throw new Error(`Schema import failed: ${JSON.stringify(result.errors)}`);
    }
  }
  
  log(`  Schema: ${fields.length} fields`, quiet);
}

async function importSynonyms(client, engineName, engineJson, quiet = false) {
  const synonyms = engineJson.synonyms;
  if (!Array.isArray(synonyms) || synonyms.length === 0) return;

  for (const syn of synonyms) {
    const result = await client.app.createSynonymSet({
      engine_name: engineName,
      synonyms: syn.synonyms,
    });

    if (result?.errors) {
      throw new Error(`Synonym import failed: ${JSON.stringify(result.errors)}`);
    }
  }
  
  log(`  Synonyms: ${synonyms.length} sets`, quiet);
}

async function importCurations(client, engineName, engineJson, quiet = false) {
  const curations = engineJson.curations;
  if (!Array.isArray(curations) || curations.length === 0) return;

  for (const c of curations) {
    const result = await client.app.createCuration({
      engine_name: engineName,
      queries: c.queries,
      promoted_doc_ids: c.promoted,
      hidden_doc_ids: c.hidden,
    });

    if (result?.errors) {
      throw new Error(`Curation import failed: ${JSON.stringify(result.errors)}`);
    }
  }
  
  log(`  Curations: ${curations.length}`, quiet);
}

async function importSearchSettings(client, engineName, engineJson, quiet = false) {
  if (!engineJson.searchSettings) return;

  const settings = { engine_name: engineName, body: {} };
  
  if (engineJson.searchSettings.search_fields) {
    settings.body.search_fields = engineJson.searchSettings.search_fields;
  }
  if (engineJson.searchSettings.result_fields) {
    settings.body.result_fields = engineJson.searchSettings.result_fields;
  }
  if (engineJson.searchSettings.boosts) {
    settings.body.boosts = engineJson.searchSettings.boosts;
  }
  if (engineJson.searchSettings.precision) {
    settings.body.precision = engineJson.searchSettings.precision;
  }

  const result = await client.app.putSearchSettings(settings);
  if (result.errors) {
    throw new Error(`Search settings failed: ${JSON.stringify(result.errors)}`);
  }
  
  log('  Search settings: imported', quiet);
}

async function importCrawlerViaRest(engineName, crawlerObj, options, quiet = false) {
  const fetch = global.fetch || (await import('node-fetch')).default;
  const baseUrl = options.appSearchEndpoint.replace(/\/$/, '') +
    `/api/as/v1/engines/${encodeURIComponent(engineName)}/crawler`;
  const apiKey = options.appSearchPrivateKey;

  async function post(path, body) {
    const resp = await fetch(`${baseUrl}${path}`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    });
    if (!resp.ok) {
      const text = await resp.text().catch(() => '');
      throw new Error(`POST ${path} failed: ${resp.status}`);
    }
    return resp.json();
  }

  // Domains
  const domains = crawlerObj.domains || [];
  const domainMap = {};
  
  for (const d of domains) {
    try {
      const body = { name: d.name || d.url || d.domain || '' };
      if (d.default_crawl_rule) body.default_crawl_rule = d.default_crawl_rule;
      
      const created = await post('/domains', body);
      if (d.id) domainMap[d.id] = created.id;
    } catch (err) {
      // Silent in quiet mode
    }
  }
  
  // Entry points
  const entryPoints = crawlerObj.entryPoints || crawlerObj.entry_points || [];
  for (const ep of entryPoints) {
    const domainId = domainMap[ep.domain_id];
    if (!domainId) continue;
    
    try {
      await post(`/domains/${domainId}/entry_points`, { value: ep.value });
    } catch (err) {}
  }
  
  // Crawl rules
  const crawlRules = crawlerObj.crawlRules || crawlerObj.crawl_rules || [];
  for (const cr of crawlRules) {
    const domainId = domainMap[cr.domain_id];
    if (!domainId) continue;
    
    try {
      await post(`/domains/${domainId}/crawl_rules`, {
        policy: cr.policy,
        rule: cr.rule,
        pattern: cr.pattern,
        order: cr.order
      });
    } catch (err) {}
  }
  
  // Sitemaps
  const sitemaps = crawlerObj.sitemaps || [];
  for (const sm of sitemaps) {
    const domainId = domainMap[sm.domain_id];
    if (!domainId) continue;
    
    try {
      await post(`/domains/${domainId}/sitemaps`, { url: sm.url || sm.value });
    } catch (err) {}
  }
  
  log('  Crawler: imported', quiet);
}

module.exports = importAppSearchEngine;