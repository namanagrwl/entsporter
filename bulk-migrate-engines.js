// bulk-migrate-engines.js
const { Client } = require('@elastic/enterprise-search');
const fs = require('fs/promises');
const path = require('path');
const { program } = require('commander');

const exportAppSearchEngine = require('./export-app-search-engine');
const importAppSearchEngine = require('./import-app-search-engine');

/* -------------------------
   STATE MANAGEMENT
   ------------------------- */
async function loadState(stateFile) {
  try {
    const data = await fs.readFile(stateFile, 'utf8');
    return JSON.parse(data);
  } catch (err) {
    return { completed: [], failed: [], skipped: [], startTime: Date.now() };
  }
}

async function saveState(stateFile, state) {
  await fs.writeFile(stateFile, JSON.stringify(state, null, 2));
}

/* -------------------------
   ENGINE LISTING
   ------------------------- */
async function listAllEngines(client) {
  console.log('   Fetching engines...');
  const all = [];
  let page = 1;

  while (true) {
    const resp = await client.app.listEngines({ page: { current: page } });
    if (!resp || !Array.isArray(resp.results)) break;

    all.push(...resp.results);
    
    if (page % 10 === 0) {
      console.log(`   Fetched ${all.length} engines so far...`);
    }

    const metaPage = resp.meta && resp.meta.page;
    if (!metaPage || page >= metaPage.total_pages) break;
    page += 1;
  }

  return all;
}

/* -------------------------
   PARALLEL PROCESSING WITH STATE
   ------------------------- */
async function processInParallel(engines, processFn, concurrency, stateFile, resumeMode, retryFailedOnly) {
  const state = resumeMode ? await loadState(stateFile) : { 
    completed: [], 
    failed: [], 
    skipped: [],
    startTime: Date.now() 
  };

  const completedSet = new Set(state.completed);
  let remaining = engines.filter(e => !completedSet.has(e.name));

  // Filter to only failed engines if retry-failed-only flag is set
  if (retryFailedOnly) {
    const failedEngineNames = new Set(state.failed.map(f => f.engine));
    const previousRemaining = remaining.length;
    remaining = remaining.filter(e => failedEngineNames.has(e.name));
    
    console.log(`\n🔄 Retry-failed-only mode enabled`);
    console.log(`   Previously failed: ${state.failed.length} engines`);
    console.log(`   Available to retry: ${remaining.length} engines`);
    console.log(`   Skipping: ${previousRemaining - remaining.length} new/unprocessed engines\n`);
    
    if (remaining.length === 0) {
      console.log('✅ No failed engines to retry!\n');
      return state;
    }
    
    // Clear failed list since we're retrying them
    state.failed = [];
  }

  console.log(`\n${'='.repeat(70)}`);
  console.log(`📊 Migration Status`);
  console.log(`${'='.repeat(70)}`);
  console.log(`Total engines:     ${engines.length}`);
  console.log(`Already completed: ${state.completed.length}`);
  console.log(`Remaining:         ${remaining.length}`);
  if (!retryFailedOnly) {
    console.log(`Failed (previous): ${state.failed.length}`);
  }
  console.log(`Concurrency:       ${concurrency} at once`);
  console.log(`${'='.repeat(70)}\n`);

  if (remaining.length === 0) {
    console.log('✅ All engines already migrated!\n');
    return state;
  }

  let processed = state.completed.length;
  const total = engines.length;
  const startTime = state.startTime;

  // Process in batches
  for (let i = 0; i < remaining.length; i += concurrency) {
    const batch = remaining.slice(i, i + concurrency);
    const batchNum = Math.floor(i / concurrency) + 1;
    const totalBatches = Math.ceil(remaining.length / concurrency);

    console.log(`\n${'='.repeat(70)}`);
    console.log(`Batch ${batchNum}/${totalBatches}: Engines ${i + 1}-${Math.min(i + concurrency, remaining.length)} of ${remaining.length} remaining`);
    console.log(`${'='.repeat(70)}`);

    // Process batch in parallel
    const batchPromises = batch.map((engine, batchIdx) => {
      const globalIdx = processed + i + batchIdx + 1;
      return processFn(engine, globalIdx, total)
        .then(result => ({ status: 'success', engine: engine.name, result }))
        .catch(error => ({ status: 'failed', engine: engine.name, error: error.message }));
    });

    const batchResults = await Promise.all(batchPromises);

    // Update state
    for (const result of batchResults) {
      processed++;
      
      if (result.status === 'success') {
        state.completed.push(result.engine);
        console.log(`✅ [${processed}/${total}] ${result.engine}`);
      } else {
        state.failed.push({ engine: result.engine, error: result.error });
        console.log(`❌ [${processed}/${total}] ${result.engine}`);
        console.log(`   Error: ${result.error}`);
      }
    }

    // Save state after each batch
    await saveState(stateFile, state);

    // Progress summary
    const elapsed = (Date.now() - startTime) / 1000;
    const remainingCount = total - processed;
    const rate = processed / elapsed;
    const etaSeconds = remainingCount > 0 ? remainingCount / rate : 0;
    const etaMinutes = (etaSeconds / 60).toFixed(1);
    
    console.log(`\n📊 Progress Update`);
    console.log(`   Completed: ${processed}/${total} (${((processed/total)*100).toFixed(1)}%)`);
    console.log(`   ✅ Succeeded: ${state.completed.length}`);
    console.log(`   ❌ Failed: ${state.failed.length}`);
    console.log(`   ⏱️  Elapsed: ${(elapsed/60).toFixed(1)} min`);
    console.log(`   ⏱️  ETA: ~${etaMinutes} min remaining`);
    console.log(`   🚀 Rate: ${(rate * 60).toFixed(1)} engines/min`);
  }

  return state;
}

/* -------------------------
   CHECK TARGET ENGINES
   ------------------------- */
async function checkTargetEngines(targetClient, targetPrefix) {
  console.log('   Checking target cluster for existing engines...');
  try {
    const targetEngines = await listAllEngines(targetClient);
    const existingNames = new Set(
      targetEngines
        .map(e => e.name)
        .filter(name => !targetPrefix || name.startsWith(targetPrefix))
    );
    console.log(`   Found ${existingNames.size} existing engines on target`);
    return existingNames;
  } catch (err) {
    console.warn(`   Could not list target engines: ${err.message}`);
    return new Set();
  }
}

/* -------------------------
   MAIN
   ------------------------- */
async function main() {
  program
    .name('entsporter-bulk')
    .description('Bulk migrate 500+ App Search engines')
    .requiredOption('--from-endpoint <value>', 'Source endpoint')
    .requiredOption('--from-key <value>', 'Source private key')
    .requiredOption('--to-endpoint <value>', 'Target endpoint')
    .requiredOption('--to-key <value>', 'Target private key')
    .option('--output-dir <value>', 'Directory for JSON files', './engines-export')
    .option('--target-prefix <value>', 'Prefix for target engine names', '')
    .option('--concurrency <number>', 'Engines to process in parallel', '5')
    .option('--state-file <value>', 'State file for resume', './migration-state.json')
    .option('--dry-run', 'List engines only', false)
    .option('--force', 'Overwrite existing engines')
    .option('--resume', 'Resume from previous run')
    .option('--retry-failed-only', 'Only retry previously failed engines (requires --resume)')
    .option('--skip-existing', 'Skip engines that exist on target')
    .option('--cleanup', 'Delete JSON after import', false);

  program.argument('[filter]', 'Substring filter for engine names', '');

  program.action(async (filter, options) => {
    const startTime = Date.now();
    const concurrency = parseInt(options.concurrency, 10);

    // Validate retry-failed-only requires resume
    if (options.retryFailedOnly && !options.resume) {
      console.error('\n❌ Error: --retry-failed-only requires --resume flag\n');
      process.exit(1);
    }

    console.log(`\n${'='.repeat(70)}`);
    console.log(`🚀 Large-Scale Bulk Engine Migration`);
    console.log(`${'='.repeat(70)}`);
    console.log(`Source:       ${options.fromEndpoint}`);
    console.log(`Target:       ${options.toEndpoint}`);
    console.log(`Output:       ${options.outputDir}`);
    console.log(`Prefix:       "${options.targetPrefix}"`);
    console.log(`Concurrency:  ${concurrency} engines at once`);
    console.log(`State file:   ${options.stateFile}`);
    if (filter) console.log(`Filter:       "${filter}"`);
    if (options.force) console.log(`Mode:         --force (overwrite)`);
    if (options.resume) console.log(`Mode:         --resume (continue previous)`);
    if (options.retryFailedOnly) console.log(`Mode:         --retry-failed-only (retry failures)`);
    if (options.skipExisting) console.log(`Mode:         --skip-existing`);
    if (options.cleanup) console.log(`Mode:         --cleanup`);
    if (options.dryRun) console.log(`Mode:         --dry-run`);
    console.log(`${'='.repeat(70)}`);

    // Create output directory
    await fs.mkdir(options.outputDir, { recursive: true });

    // Connect to clusters
    console.log(`\n🔍 Connecting to clusters...`);
    const sourceClient = new Client({
      url: options.fromEndpoint,
      auth: { token: options.fromKey }
    });

    const targetClient = new Client({
      url: options.toEndpoint,
      auth: { token: options.toKey }
    });

    // List source engines
    console.log(`\n📋 Listing source engines...`);
    const engines = await listAllEngines(sourceClient);
    
    if (!engines.length) {
      console.log('❌ No engines found on source cluster.\n');
      return;
    }

    console.log(`   ✅ Found ${engines.length} engines on source`);

    // Filter
    const filtered = filter
      ? engines.filter(e => String(e.name).includes(filter))
      : engines;

    console.log(`   ✅ ${filtered.length} engines match filter`);

    if (!filtered.length) {
      console.log('   Nothing to migrate.\n');
      return;
    }

    // Check target for existing engines if skip-existing
    let existingOnTarget = new Set();
    if (options.skipExisting) {
      console.log(`\n🔍 Checking target cluster...`);
      existingOnTarget = await checkTargetEngines(targetClient, options.targetPrefix);
    }

    // Dry run
    if (options.dryRun) {
      console.log(`\n${'─'.repeat(70)}`);
      console.log('Engines to migrate:');
      console.log(`${'─'.repeat(70)}`);
      
      let willMigrate = 0;
      let willSkip = 0;
      
      // Load state for dry run if resume mode
      let state = { completed: [], failed: [] };
      if (options.resume) {
        state = await loadState(options.stateFile);
      }
      
      const completedSet = new Set(state.completed);
      const failedEngineNames = new Set(state.failed.map(f => f.engine));
      
      filtered.forEach((e, idx) => {
        const dstName = `${options.targetPrefix}${e.name}`;
        const exists = existingOnTarget.has(dstName);
        const alreadyCompleted = completedSet.has(e.name);
        const previouslyFailed = failedEngineNames.has(e.name);
        
        let status = '';
        let shouldProcess = false;
        
        if (alreadyCompleted) {
          status = '[COMPLETED]';
          willSkip++;
        } else if (options.retryFailedOnly && !previouslyFailed) {
          status = '[SKIP - not failed]';
          willSkip++;
        } else if (options.skipExisting && exists && !options.force) {
          status = '[SKIP - exists]';
          willSkip++;
        } else {
          shouldProcess = true;
          willMigrate++;
          if (previouslyFailed) {
            status = '[RETRY]';
          } else if (exists) {
            status = '[OVERWRITE]';
          } else {
            status = '';
          }
        }
        
        if (shouldProcess || !options.resume) {
          console.log(`${idx + 1}. ${status} ${e.name} -> ${dstName}`);
        }
      });
      
      console.log(`${'─'.repeat(70)}`);
      console.log(`Will migrate: ${willMigrate}`);
      console.log(`Will skip: ${willSkip}`);
      console.log(`${'─'.repeat(70)}\n`);
      
      const estimatedTime = (willMigrate * 4 / concurrency).toFixed(0);
      console.log(`⏱️  Estimated time: ~${estimatedTime} minutes\n`);
      return;
    }

    // Process in parallel
    const state = await processInParallel(
      filtered,
      async (engine, idx, total) => {
        const srcName = engine.name;
        const dstName = `${options.targetPrefix}${srcName}`;
        const jsonPath = path.join(options.outputDir, `${srcName}.json`);

        // Skip if exists and skip-existing flag
        if (options.skipExisting && existingOnTarget.has(dstName) && !options.force) {
          console.log(`⏭️  [${idx}/${total}] Skipping ${srcName} (exists on target)`);
          throw new Error('SKIPPED');
        }

        console.log(`📦 [${idx}/${total}] ${srcName} -> ${dstName}`);

        // Export with quiet mode
        await exportAppSearchEngine(srcName, {
          appSearchEndpoint: options.fromEndpoint,
          appSearchPrivateKey: options.fromKey,
          outputJson: jsonPath,
          quiet: true
        });

        // Import with quiet mode
        await importAppSearchEngine(dstName, {
          appSearchEndpoint: options.toEndpoint,
          appSearchPrivateKey: options.toKey,
          inputJson: jsonPath,
          force: options.force,
          quiet: true
        });

        // Cleanup
        if (options.cleanup) {
          await fs.unlink(jsonPath).catch(() => {});
        }

        return { src: srcName, dst: dstName };
      },
      concurrency,
      options.stateFile,
      options.resume,
      options.retryFailedOnly
    );

    // Final summary
    const totalTime = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    
    console.log(`\n${'='.repeat(70)}`);
    console.log(`✅ Migration Complete!`);
    console.log(`${'='.repeat(70)}`);
    console.log(`Total time:    ${totalTime} minutes`);
    console.log(`Total engines: ${filtered.length}`);
    console.log(`Succeeded:     ${state.completed.length}`);
    console.log(`Failed:        ${state.failed.length}`);
    console.log(`Success rate:  ${((state.completed.length/filtered.length)*100).toFixed(1)}%`);
    console.log(`${'='.repeat(70)}`);

    if (state.failed.length > 0) {
      console.log(`\n❌ Failed engines (${state.failed.length}):`);
      state.failed.slice(0, 20).forEach(({ engine, error }) => {
        console.log(`   - ${engine}: ${error}`);
      });
      if (state.failed.length > 20) {
        console.log(`   ... and ${state.failed.length - 20} more`);
      }
      
      console.log(`\n💡 To retry failed engines:`);
      console.log(`   node bulk-migrate-engines.js --resume --retry-failed-only --concurrency 3 --force ...`);
    }

    console.log(`\n💾 State saved to: ${options.stateFile}`);
    console.log(`${'='.repeat(70)}\n`);

    if (state.failed.length > 0) {
      process.exit(1);
    }
  });

  await program.parseAsync(process.argv);
}

main().catch(err => {
  console.error('\n❌ Fatal error:', err.message);
  process.exit(1);
});