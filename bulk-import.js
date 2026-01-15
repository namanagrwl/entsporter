// import-all-engines.js
const { Client } = require('@elastic/enterprise-search');
const fs = require('fs/promises');
const path = require('path');
const { program } = require('commander');

const importAppSearchEngine = require('./import-app-search-engine');

async function getJsonFiles(directory) {
  const files = await fs.readdir(directory);
  return files.filter(f => f.endsWith('.json'));
}

async function importEngineBatch(files, options, startIndex, totalFiles) {
  const results = [];
  
  for (let i = 0; i < files.length; i++) {
    const file = files[i];
    const globalIndex = startIndex + i + 1;
    const progress = `[${globalIndex}/${totalFiles}]`;
    const engineName = path.basename(file, '.json');
    const targetName = `${options.prefix}${engineName}`;
    const jsonPath = path.join(options.inputDir, file);

    console.log(`${progress} Importing: ${engineName} → ${targetName}`);

    try {
      await importAppSearchEngine(targetName, {
        appSearchEndpoint: options.endpoint,
        appSearchPrivateKey: options.key,
        inputJson: jsonPath,
        force: options.force,
        quiet: true
      });
      
      results.push({ 
        success: true, 
        engine: engineName,
        target: targetName 
      });

      // Cleanup if requested
      if (options.cleanup) {
        await fs.unlink(jsonPath).catch(() => {});
      }
      
    } catch (error) {
      results.push({ 
        success: false, 
        engine: engineName,
        target: targetName,
        error: error.message 
      });
    }
  }
  
  return results;
}

async function main() {
  program
    .description('Import all App Search engines from JSON files (with concurrency)')
    .requiredOption('--endpoint <url>', 'Target cluster URL')
    .requiredOption('--key <key>', 'Target API key')
    .option('--input-dir <path>', 'Input directory with JSON files', './engines-export')
    .option('--prefix <prefix>', 'Prefix to add to engine names', '')
    .option('--concurrency <number>', 'Number of engines to import in parallel', '5')
    .option('--force', 'Delete existing engines before import')
    .option('--cleanup', 'Delete JSON files after successful import')
    .option('--dry-run', 'Show what would be imported without doing it');

  program.action(async (options) => {
    const startTime = Date.now();
    const concurrency = parseInt(options.concurrency, 10);

    console.log('\n' + '='.repeat(70));
    console.log('Import All Engines');
    console.log('='.repeat(70));
    console.log(`Target:      ${options.endpoint}`);
    console.log(`Input:       ${options.inputDir}`);
    console.log(`Prefix:      "${options.prefix}"`);
    console.log(`Concurrency: ${concurrency} engines at once`);
    console.log(`Force:       ${options.force ? 'Yes' : 'No'}`);
    console.log(`Cleanup:     ${options.cleanup ? 'Yes' : 'No'}`);
    console.log(`Dry Run:     ${options.dryRun ? 'Yes' : 'No'}`);
    console.log('='.repeat(70) + '\n');

    // Get all JSON files
    console.log(`Reading JSON files from ${options.inputDir}...\n`);
    const jsonFiles = await getJsonFiles(options.inputDir);

    if (jsonFiles.length === 0) {
      console.log('❌ No JSON files found in directory\n');
      return;
    }

    console.log(`✅ Found ${jsonFiles.length} JSON files\n`);

    // Dry run
    if (options.dryRun) {
      console.log('─'.repeat(70));
      console.log('Engines to import:');
      console.log('─'.repeat(70));
      
      for (let i = 0; i < jsonFiles.length; i++) {
        const file = jsonFiles[i];
        const engineName = path.basename(file, '.json');
        const targetName = `${options.prefix}${engineName}`;
        console.log(`${i + 1}. ${engineName} → ${targetName}`);
      }
      
      console.log('─'.repeat(70));
      console.log(`Total: ${jsonFiles.length} engines`);
      console.log(`Estimated time: ~${(jsonFiles.length * 3 / concurrency).toFixed(0)} minutes (${concurrency} at once)`);
      console.log('─'.repeat(70) + '\n');
      console.log('💡 Run without --dry-run to start import\n');
      return;
    }

    // Import engines with concurrency
    console.log(`Starting import of ${jsonFiles.length} engines (${concurrency} at once)...\n`);
    
    const totalStats = {
      succeeded: 0,
      failed: 0,
      failedEngines: []
    };

    // Process in batches
    for (let i = 0; i < jsonFiles.length; i += concurrency) {
      const batch = jsonFiles.slice(i, i + concurrency);
      const batchNum = Math.floor(i / concurrency) + 1;
      const totalBatches = Math.ceil(jsonFiles.length / concurrency);

      console.log(`\n${'='.repeat(70)}`);
      console.log(`Batch ${batchNum}/${totalBatches}: Processing engines ${i + 1}-${Math.min(i + concurrency, jsonFiles.length)}`);
      console.log('='.repeat(70));

      // Process batch in parallel
      const batchPromises = batch.map((file, idx) => 
        importEngineBatch([file], options, i + idx, jsonFiles.length)
      );

      const batchResults = await Promise.all(batchPromises);

      // Collect results
      for (const results of batchResults) {
        for (const result of results) {
          if (result.success) {
            totalStats.succeeded++;
            console.log(`✅ Success: ${result.target}`);
          } else {
            totalStats.failed++;
            totalStats.failedEngines.push({
              engine: result.engine,
              error: result.error
            });
            console.log(`❌ Failed: ${result.engine}`);
            console.log(`   Error: ${result.error}`);
          }
        }
      }

      // Progress update
      const processed = Math.min(i + concurrency, jsonFiles.length);
      const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
      const remaining = jsonFiles.length - processed;
      const rate = processed / ((Date.now() - startTime) / 1000 / 60);
      const eta = remaining > 0 ? (remaining / rate).toFixed(0) : 0;

      console.log(`\n📊 Progress Update`);
      console.log(`   Completed: ${processed}/${jsonFiles.length} (${((processed/jsonFiles.length)*100).toFixed(1)}%)`);
      console.log(`   ✅ Succeeded: ${totalStats.succeeded}`);
      console.log(`   ❌ Failed: ${totalStats.failed}`);
      console.log(`   ⏱️  Elapsed: ${elapsed} min`);
      console.log(`   ⏱️  ETA: ~${eta} min remaining`);
      console.log(`   🚀 Rate: ${rate.toFixed(1)} engines/min`);
    }

    // Final summary
    const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    console.log('\n' + '='.repeat(70));
    console.log('Import Complete');
    console.log('='.repeat(70));
    console.log(`Time:        ${totalTime} minutes`);
    console.log(`Total:       ${jsonFiles.length} engines`);
    console.log(`Succeeded:   ${totalStats.succeeded}`);
    console.log(`Failed:      ${totalStats.failed}`);
    console.log(`Success rate: ${((totalStats.succeeded/jsonFiles.length)*100).toFixed(1)}%`);
    console.log('='.repeat(70));

    if (totalStats.failed > 0) {
      console.log(`\n❌ Failed engines (${totalStats.failed}):`);
      totalStats.failedEngines.slice(0, 20).forEach(({ engine, error }) => {
        console.log(`   - ${engine}: ${error}`);
      });
      if (totalStats.failedEngines.length > 20) {
        console.log(`   ... and ${totalStats.failedEngines.length - 20} more`);
      }
      console.log('\n💡 To retry failed engines, run the import script again with --force\n');
      process.exit(1);
    }

    console.log('\n');
  });

  await program.parseAsync(process.argv);
}

main().catch(err => {
  console.error('\n❌ Fatal error:', err.message);
  process.exit(1);
});