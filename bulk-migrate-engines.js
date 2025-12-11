// bulk-migrate-engines.js
//
// Lists all App Search engines from a source cluster,
// exports each to JSON, and imports them into a target cluster
// using the existing exporter/importer in this repo.

const { Client } = require('@elastic/enterprise-search');
const fs = require('fs/promises');
const path = require('path');
const { program } = require('commander');

const exportAppSearchEngine = require('./export-app-search-engine');
const importAppSearchEngine = require('./import-app-search-engine');

async function listAllEngines(client) {
  const all = [];
  let page = 1;

  while (true) {
    const resp = await client.app.listEngines({ page: { current: page } });

    if (!resp || !Array.isArray(resp.results)) break;

    all.push(...resp.results);

    const metaPage = resp.meta && resp.meta.page;
    if (!metaPage || page >= metaPage.total_pages) break;
    page += 1;
  }

  return all;
}

async function main() {
  program
    .name('entsporter-bulk')
    .description('Bulk export/import all App Search engines between clusters')
    .requiredOption('--from-endpoint <value>', 'Source App Search endpoint')
    .requiredOption('--from-key <value>', 'Source App Search private key')
    .requiredOption('--to-endpoint <value>', 'Target App Search endpoint')
    .requiredOption('--to-key <value>', 'Target App Search private key')
    .option('--output-dir <value>', 'Directory to store engine JSON files', './engines-export')
    .option('--target-prefix <value>', 'Prefix for target engine names', '')
    .option('--dry-run', 'Only list engines, do not export/import', false)
    .option('--force', 'Delete target engine if it already exists');

  program.argument('[engine-filter]', 'Optional substring filter for engine names', '');

  program.action(async (engineFilter, options) => {
    const {
      fromEndpoint,
      fromKey,
      toEndpoint,
      toKey,
      outputDir,
      targetPrefix,
      dryRun
    } = options;

    console.log(`Source endpoint: ${fromEndpoint}`);
    console.log(`Target endpoint: ${toEndpoint}`);
    console.log(`Output dir: ${outputDir}`);
    console.log(`Target name prefix: "${targetPrefix}"`);
    if (engineFilter) {
      console.log(`Engine name filter: "${engineFilter}" (substring match)`);
    }
    if (dryRun) {
      console.log('Dry-run mode: NO export/import will be performed.');
    }

    await fs.mkdir(outputDir, { recursive: true });

    const sourceClient = new Client({
      url: fromEndpoint,
      auth: { token: fromKey }
    });

    console.log('Listing engines from source cluster...');
    const engines = await listAllEngines(sourceClient);
    if (!engines.length) {
      console.log('No engines found on source cluster.');
      return;
    }

    const filteredEngines = engineFilter
      ? engines.filter(e => String(e.name).includes(engineFilter))
      : engines;

    console.log(`Found ${engines.length} engine(s), ${filteredEngines.length} match filter.`);

    if (!filteredEngines.length) {
      console.log('Nothing to do.');
      return;
    }

    if (dryRun) {
      console.log('Engines to process:');
      filteredEngines.forEach(e => {
        console.log(` - ${e.name} (type: ${e.type}, language: ${e.language || 'none'})`);
      });
      return;
    }

    // Process engines sequentially to keep things predictable
    for (const engine of filteredEngines) {
      const srcName = engine.name;
      const dstName = `${targetPrefix}${srcName}`;
      const jsonPath = path.join(outputDir, `${srcName}.json`);

      console.log('===============================================');
      console.log(`Processing engine: ${srcName}`);
      console.log(` -> Export file: ${jsonPath}`);
      console.log(` -> Target engine: ${dstName}`);
      console.log('===============================================');

      try {
        // 1) Export from source
        await exportAppSearchEngine(srcName, {
          appSearchEndpoint: fromEndpoint,
          appSearchPrivateKey: fromKey,
          outputJson: jsonPath
        });

        // 2) Import into target
        await importAppSearchEngine(dstName, {
          appSearchEndpoint: toEndpoint,
          appSearchPrivateKey: toKey,
          inputJson: jsonPath,
          force: options.force
        });

        console.log(`Completed migration for engine "${srcName}" -> "${dstName}"`);
      } catch (err) {
        console.error(`Failed to migrate engine "${srcName}":`, err && err.message ? err.message : err);
        // continue with next engine
      }
    }

    console.log('Bulk migration complete.');
  });

  await program.parseAsync(process.argv);
}

main().catch(err => {
  console.error('Fatal error in bulk migration:', err && err.message ? err.message : err);
  process.exit(1);
});
