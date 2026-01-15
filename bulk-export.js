// export-all-engines.js
const { Client } = require('@elastic/enterprise-search');
const fs = require('fs/promises');
const path = require('path');
const { program } = require('commander');

const exportAppSearchEngine = require('./export-app-search-engine');

async function listAllEngines(client) {
  console.log('Fetching engines from source cluster...');
  const allEngines = [];
  let page = 1;

  while (true) {
    const response = await client.app.listEngines({ page: { current: page } });
    if (!response?.results?.length) break;
    
    allEngines.push(...response.results);
    
    if (allEngines.length % 100 === 0) {
      console.log(`  Found ${allEngines.length} engines so far...`);
    }
    
    if (!response.meta?.page || page >= response.meta.page.total_pages) break;
    page++;
  }

  return allEngines;
}

async function main() {
  program
    .description('Export all App Search engines to JSON files')
    .requiredOption('--endpoint <url>', 'Source cluster URL')
    .requiredOption('--key <key>', 'Source API key')
    .option('--output-dir <path>', 'Output directory for JSON files', './engines-export')
    .argument('[filter]', 'Filter engines by name (substring)', '');

  program.action(async (filter, options) => {
    const startTime = Date.now();

    console.log('\n' + '='.repeat(70));
    console.log('Export All Engines');
    console.log('='.repeat(70));
    console.log(`Source:     ${options.endpoint}`);
    console.log(`Output:     ${options.outputDir}`);
    if (filter) console.log(`Filter:     "${filter}"`);
    console.log('='.repeat(70) + '\n');

    // Create output directory
    await fs.mkdir(options.outputDir, { recursive: true });

    // Connect to cluster
    const client = new Client({
      url: options.endpoint,
      auth: { token: options.key }
    });

    // List all engines
    const allEngines = await listAllEngines(client);
    console.log(`✅ Found ${allEngines.length} engines\n`);

    if (allEngines.length === 0) {
      console.log('No engines to export\n');
      return;
    }

    // Filter if needed
    const engines = filter
      ? allEngines.filter(e => e.name.includes(filter))
      : allEngines;

    if (filter) {
      console.log(`Filtered to ${engines.length} engines matching "${filter}"\n`);
    }

    if (engines.length === 0) {
      console.log(`No engines match filter "${filter}"\n`);
      return;
    }

    // Export each engine
    console.log(`Starting export of ${engines.length} engines...\n`);
    let succeeded = 0;
    let failed = 0;

    for (let i = 0; i < engines.length; i++) {
      const engine = engines[i];
      const progress = `[${i + 1}/${engines.length}]`;
      const jsonFile = path.join(options.outputDir, `${engine.name}.json`);

      try {
        console.log(`${progress} Exporting: ${engine.name}`);
        
        await exportAppSearchEngine(engine.name, {
          appSearchEndpoint: options.endpoint,
          appSearchPrivateKey: options.key,
          outputJson: jsonFile,
          quiet: true
        });
        
        succeeded++;
        console.log(`✅ ${progress} Saved to: ${jsonFile}`);
        
      } catch (error) {
        failed++;
        console.error(`❌ ${progress} Failed: ${engine.name}`);
        console.error(`   Error: ${error.message}`);
      }

      // Progress update every 25 engines
      if ((i + 1) % 25 === 0) {
        const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
        console.log(`\n📊 Progress: ${i + 1}/${engines.length} | ${elapsed} min elapsed\n`);
      }
    }

    // Summary
    const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    console.log('\n' + '='.repeat(70));
    console.log('Export Complete');
    console.log('='.repeat(70));
    console.log(`Time:      ${totalTime} minutes`);
    console.log(`Succeeded: ${succeeded}`);
    console.log(`Failed:    ${failed}`);
    console.log(`Location:  ${options.outputDir}`);
    console.log('='.repeat(70) + '\n');

    if (failed > 0) {
      process.exit(1);
    }
  });

  await program.parseAsync(process.argv);
}

main().catch(err => {
  console.error('\n❌ Fatal error:', err.message);
  process.exit(1);
});