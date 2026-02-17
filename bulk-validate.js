// bulk-validate.js
// Offline validation: compares source export JSONs against target re-export JSONs
// No API calls — just file-to-file comparison with normalization
const fs = require('fs/promises');
const path = require('path');
const { program } = require('commander');

/* -----------------------
   Helpers
   ----------------------- */

async function getJsonFiles(directory) {
  const files = await fs.readdir(directory);
  return files.filter(f => f.endsWith('.json'));
}

/**
 * Deep-sort object keys for deterministic JSON.stringify comparison
 */
function sortKeys(obj) {
  if (obj === null || obj === undefined) return obj;
  if (Array.isArray(obj)) return obj.map(sortKeys);
  if (typeof obj !== 'object') return obj;

  const sorted = {};
  for (const key of Object.keys(obj).sort()) {
    sorted[key] = sortKeys(obj[key]);
  }
  return sorted;
}

/* -----------------------
   Normalization — strip volatile fields that legitimately differ between envs
   ----------------------- */

function normalizeSynonyms(synonyms) {
  if (!Array.isArray(synonyms) || synonyms.length === 0) return [];

  return synonyms
    .map(s => {
      // Keep only the synonym words, strip id
      const words = [...(s.synonyms || [])].sort();
      return { synonyms: words };
    })
    .sort((a, b) => {
      // Sort sets by their first word for deterministic ordering
      const aKey = a.synonyms.join(',');
      const bKey = b.synonyms.join(',');
      return aKey.localeCompare(bKey);
    });
}

function normalizeCurations(curations) {
  if (!Array.isArray(curations) || curations.length === 0) return [];

  return curations
    .map(c => ({
      queries: [...(c.queries || [])].sort(),
      promoted: [...(c.promoted || [])],
      hidden: [...(c.hidden || [])]
    }))
    .sort((a, b) => {
      const aKey = a.queries.join(',');
      const bKey = b.queries.join(',');
      return aKey.localeCompare(bKey);
    });
}

function normalizeSearchSettings(settings) {
  if (!settings) return null;

  const normalized = {};

  if (settings.search_fields) {
    normalized.search_fields = sortKeys(settings.search_fields);
  }
  if (settings.result_fields) {
    normalized.result_fields = sortKeys(settings.result_fields);
  }
  if (settings.boosts) {
    normalized.boosts = sortKeys(settings.boosts);
  }
  if (settings.precision !== undefined) {
    normalized.precision = settings.precision;
  }

  // Skip precision_enabled — it's auto-derived from precision

  return normalized;
}

function normalizeDomain(d) {
  // Strip volatile: id, document_count, created_at, last_visited_at,
  //   available_deduplication_fields, auth
  const normalized = {
    name: d.name || d.url || d.domain || ''
  };

  if (d.deduplication_enabled !== undefined) {
    normalized.deduplication_enabled = d.deduplication_enabled;
  }
  if (Array.isArray(d.deduplication_fields)) {
    normalized.deduplication_fields = [...d.deduplication_fields].sort();
  }

  // Normalize nested entry_points
  if (Array.isArray(d.entry_points)) {
    normalized.entry_points = d.entry_points
      .map(ep => ({ value: ep.value }))
      .sort((a, b) => (a.value || '').localeCompare(b.value || ''));
  }

  // Normalize nested crawl_rules
  if (Array.isArray(d.crawl_rules)) {
    normalized.crawl_rules = d.crawl_rules
      .map(cr => ({
        order: cr.order,
        policy: cr.policy,
        rule: cr.rule,
        pattern: cr.pattern
      }))
      .sort((a, b) => (a.order || 0) - (b.order || 0));
  }

  // Normalize default_crawl_rule
  if (d.default_crawl_rule) {
    normalized.default_crawl_rule = {
      policy: d.default_crawl_rule.policy,
      rule: d.default_crawl_rule.rule,
      pattern: d.default_crawl_rule.pattern
    };
  }

  // Normalize nested sitemaps
  if (Array.isArray(d.sitemaps)) {
    normalized.sitemaps = d.sitemaps
      .map(sm => ({ url: sm.url || sm.value }))
      .sort((a, b) => (a.url || '').localeCompare(b.url || ''));
  }

  return normalized;
}

function normalizeCrawler(crawler) {
  if (!crawler) return null;

  const domains = (crawler.domains || [])
    .map(normalizeDomain)
    .sort((a, b) => a.name.localeCompare(b.name));

  // Also normalize the flat arrays (entryPoints, crawlRules, sitemaps)
  // These duplicate the nested data but with domain_id references — strip those
  const entryPoints = (crawler.entryPoints || crawler.entry_points || [])
    .map(ep => ({ value: ep.value }))
    .sort((a, b) => (a.value || '').localeCompare(b.value || ''));

  const crawlRules = (crawler.crawlRules || crawler.crawl_rules || [])
    .map(cr => ({
      order: cr.order,
      policy: cr.policy,
      rule: cr.rule,
      pattern: cr.pattern
    }))
    .sort((a, b) => (a.order || 0) - (b.order || 0));

  const sitemaps = (crawler.sitemaps || [])
    .map(sm => ({ url: sm.url || sm.value }))
    .sort((a, b) => (a.url || '').localeCompare(b.url || ''));

  return { domains, entryPoints, crawlRules, sitemaps };
}

/**
 * Normalize an entire engine JSON for comparison.
 * Strips all volatile fields (IDs, timestamps, document_count, etc.)
 */
function normalizeEngine(engineJson) {
  return {
    read_only: {
      type: engineJson.read_only?.type || 'default',
      language: engineJson.read_only?.language || null
    },
    schema: sortKeys(engineJson.schema || {}),
    synonyms: normalizeSynonyms(engineJson.synonyms),
    curations: normalizeCurations(engineJson.curations),
    searchSettings: normalizeSearchSettings(engineJson.searchSettings),
    crawler: normalizeCrawler(engineJson.crawler)
  };
}

/* -----------------------
   Section comparators — return { status, mismatches[] }
   ----------------------- */

function compareMetadata(src, tgt) {
  const mismatches = [];

  if (src.read_only.language !== tgt.read_only.language) {
    mismatches.push(`metadata.language: source "${src.read_only.language}", target "${tgt.read_only.language}"`);
  }
  if (src.read_only.type !== tgt.read_only.type) {
    mismatches.push(`metadata.type: source "${src.read_only.type}", target "${tgt.read_only.type}"`);
  }

  return { status: mismatches.length === 0 ? 'pass' : 'fail', mismatches };
}

function compareSchema(src, tgt) {
  const mismatches = [];
  const srcSchema = src.schema;
  const tgtSchema = tgt.schema;

  const srcFields = Object.keys(srcSchema);
  const tgtFields = Object.keys(tgtSchema);

  if (srcFields.length !== tgtFields.length) {
    mismatches.push(`schema.fieldCount: source ${srcFields.length}, target ${tgtFields.length}`);
  }

  const tgtFieldSet = new Set(tgtFields);
  for (const field of srcFields) {
    if (!tgtFieldSet.has(field)) {
      mismatches.push(`schema.missing: field "${field}" not in target`);
    } else if (srcSchema[field] !== tgtSchema[field]) {
      mismatches.push(`schema.typeMismatch: "${field}" source "${srcSchema[field]}", target "${tgtSchema[field]}"`);
    }
  }

  const srcFieldSet = new Set(srcFields);
  for (const field of tgtFields) {
    if (!srcFieldSet.has(field)) {
      mismatches.push(`schema.extra: field "${field}" in target but not source`);
    }
  }

  return { status: mismatches.length === 0 ? 'pass' : 'fail', mismatches };
}

function compareSynonyms(src, tgt) {
  const mismatches = [];
  const srcSyn = src.synonyms;
  const tgtSyn = tgt.synonyms;

  if (srcSyn.length !== tgtSyn.length) {
    mismatches.push(`synonyms.count: source ${srcSyn.length}, target ${tgtSyn.length}`);
    return { status: 'fail', mismatches };
  }

  // Both are sorted — compare element by element
  for (let i = 0; i < srcSyn.length; i++) {
    const srcWords = JSON.stringify(srcSyn[i].synonyms);
    const tgtWords = JSON.stringify(tgtSyn[i].synonyms);
    if (srcWords !== tgtWords) {
      mismatches.push(`synonyms[${i}]: source ${srcWords}, target ${tgtWords}`);
    }
  }

  return { status: mismatches.length === 0 ? 'pass' : 'fail', mismatches };
}

function compareCurations(src, tgt) {
  const mismatches = [];
  const srcCur = src.curations;
  const tgtCur = tgt.curations;

  if (srcCur.length !== tgtCur.length) {
    mismatches.push(`curations.count: source ${srcCur.length}, target ${tgtCur.length}`);
    return { status: 'fail', mismatches };
  }

  for (let i = 0; i < srcCur.length; i++) {
    const s = srcCur[i];
    const t = tgtCur[i];

    if (JSON.stringify(s.queries) !== JSON.stringify(t.queries)) {
      mismatches.push(`curations[${i}].queries: source ${JSON.stringify(s.queries)}, target ${JSON.stringify(t.queries)}`);
    }
    if (JSON.stringify(s.promoted) !== JSON.stringify(t.promoted)) {
      mismatches.push(`curations[${i}].promoted: differs`);
    }
    if (JSON.stringify(s.hidden) !== JSON.stringify(t.hidden)) {
      mismatches.push(`curations[${i}].hidden: differs`);
    }
  }

  return { status: mismatches.length === 0 ? 'pass' : 'fail', mismatches };
}

function compareSearchSettings(src, tgt) {
  const mismatches = [];
  const srcS = src.searchSettings;
  const tgtS = tgt.searchSettings;

  if (!srcS && !tgtS) return { status: 'pass', mismatches };
  if (!srcS || !tgtS) {
    mismatches.push(`searchSettings: source ${srcS ? 'present' : 'absent'}, target ${tgtS ? 'present' : 'absent'}`);
    return { status: 'fail', mismatches };
  }

  // search_fields
  const srcSF = JSON.stringify(srcS.search_fields || {});
  const tgtSF = JSON.stringify(tgtS.search_fields || {});
  if (srcSF !== tgtSF) {
    // Drill into specifics
    const srcFields = srcS.search_fields || {};
    const tgtFields = tgtS.search_fields || {};
    const allKeys = new Set([...Object.keys(srcFields), ...Object.keys(tgtFields)]);

    for (const key of allKeys) {
      if (!srcFields[key]) {
        mismatches.push(`searchSettings.search_fields.extra: "${key}" in target only`);
      } else if (!tgtFields[key]) {
        mismatches.push(`searchSettings.search_fields.missing: "${key}" not in target`);
      } else if (srcFields[key]?.weight !== tgtFields[key]?.weight) {
        mismatches.push(`searchSettings.search_fields.weight: "${key}" source ${srcFields[key]?.weight}, target ${tgtFields[key]?.weight}`);
      }
    }
  }

  // result_fields
  const srcRF = JSON.stringify(srcS.result_fields || {});
  const tgtRF = JSON.stringify(tgtS.result_fields || {});
  if (srcRF !== tgtRF) {
    const srcFields = srcS.result_fields || {};
    const tgtFields = tgtS.result_fields || {};
    const allKeys = new Set([...Object.keys(srcFields), ...Object.keys(tgtFields)]);

    for (const key of allKeys) {
      if (!srcFields[key]) {
        mismatches.push(`searchSettings.result_fields.extra: "${key}" in target only`);
      } else if (!tgtFields[key]) {
        mismatches.push(`searchSettings.result_fields.missing: "${key}" not in target`);
      } else if (JSON.stringify(srcFields[key]) !== JSON.stringify(tgtFields[key])) {
        mismatches.push(`searchSettings.result_fields.config: "${key}" differs`);
      }
    }
  }

  // boosts
  const srcB = JSON.stringify(srcS.boosts || {});
  const tgtB = JSON.stringify(tgtS.boosts || {});
  if (srcB !== tgtB) {
    mismatches.push(`searchSettings.boosts: differs`);
  }

  // precision
  if (srcS.precision !== tgtS.precision) {
    mismatches.push(`searchSettings.precision: source ${srcS.precision}, target ${tgtS.precision}`);
  }

  return { status: mismatches.length === 0 ? 'pass' : 'fail', mismatches };
}

function compareCrawler(src, tgt) {
  const mismatches = [];
  const srcC = src.crawler;
  const tgtC = tgt.crawler;

  if (!srcC && !tgtC) return { status: 'skip', mismatches };
  if (!srcC || !tgtC) {
    mismatches.push(`crawler: source ${srcC ? 'present' : 'absent'}, target ${tgtC ? 'present' : 'absent'}`);
    return { status: 'fail', mismatches };
  }

  // Domains
  if (srcC.domains.length !== tgtC.domains.length) {
    mismatches.push(`crawler.domainCount: source ${srcC.domains.length}, target ${tgtC.domains.length}`);
  }

  const srcDomainStr = JSON.stringify(srcC.domains);
  const tgtDomainStr = JSON.stringify(tgtC.domains);
  if (srcDomainStr !== tgtDomainStr) {
    // Drill into domain-level diff
    const srcNames = new Set(srcC.domains.map(d => d.name));
    const tgtNames = new Set(tgtC.domains.map(d => d.name));

    for (const name of srcNames) {
      if (!tgtNames.has(name)) {
        mismatches.push(`crawler.domainMissing: "${name}" not in target`);
      }
    }
    for (const name of tgtNames) {
      if (!srcNames.has(name)) {
        mismatches.push(`crawler.domainExtra: "${name}" in target only`);
      }
    }

    // For matching domains, compare internals
    for (const srcDom of srcC.domains) {
      const tgtDom = tgtC.domains.find(d => d.name === srcDom.name);
      if (!tgtDom) continue;

      if (JSON.stringify(srcDom.entry_points) !== JSON.stringify(tgtDom.entry_points)) {
        mismatches.push(`crawler.domain["${srcDom.name}"].entry_points: differs`);
      }
      if (JSON.stringify(srcDom.crawl_rules) !== JSON.stringify(tgtDom.crawl_rules)) {
        mismatches.push(`crawler.domain["${srcDom.name}"].crawl_rules: differs`);
      }
      if (JSON.stringify(srcDom.sitemaps) !== JSON.stringify(tgtDom.sitemaps)) {
        mismatches.push(`crawler.domain["${srcDom.name}"].sitemaps: differs`);
      }
      if (JSON.stringify(srcDom.default_crawl_rule) !== JSON.stringify(tgtDom.default_crawl_rule)) {
        mismatches.push(`crawler.domain["${srcDom.name}"].default_crawl_rule: differs`);
      }
    }
  }

  // Flat arrays (entryPoints, crawlRules, sitemaps)
  if (JSON.stringify(srcC.entryPoints) !== JSON.stringify(tgtC.entryPoints)) {
    mismatches.push(`crawler.entryPoints: differs`);
  }
  if (JSON.stringify(srcC.crawlRules) !== JSON.stringify(tgtC.crawlRules)) {
    mismatches.push(`crawler.crawlRules: differs`);
  }
  if (JSON.stringify(srcC.sitemaps) !== JSON.stringify(tgtC.sitemaps)) {
    mismatches.push(`crawler.sitemaps: differs`);
  }

  return { status: mismatches.length === 0 ? 'pass' : 'fail', mismatches };
}

/* -----------------------
   Single engine comparison
   ----------------------- */

async function compareEngine(engineName, srcPath, tgtPath) {
  const mismatches = [];
  const checks = {};

  const srcJson = JSON.parse(await fs.readFile(srcPath, 'utf8'));
  const tgtJson = JSON.parse(await fs.readFile(tgtPath, 'utf8'));

  const src = normalizeEngine(srcJson);
  const tgt = normalizeEngine(tgtJson);

  const sections = [
    { name: 'metadata', fn: compareMetadata },
    { name: 'schema', fn: compareSchema },
    { name: 'synonyms', fn: compareSynonyms },
    { name: 'curations', fn: compareCurations },
    { name: 'searchSettings', fn: compareSearchSettings },
    { name: 'crawler', fn: compareCrawler }
  ];

  for (const section of sections) {
    const result = section.fn(src, tgt);
    checks[section.name] = result.status;
    mismatches.push(...result.mismatches);
  }

  return {
    passed: mismatches.length === 0,
    engine: engineName,
    mismatches,
    checks
  };
}

/* -----------------------
   Main
   ----------------------- */

async function main() {
  program
    .description('Validate imported engines by comparing source and target export directories (offline, no API calls)')
    .requiredOption('--source-dir <path>', 'Original export directory (from source env)')
    .requiredOption('--target-dir <path>', 'Re-export directory (from target env after import)')
    .option('--prefix <prefix>', 'Prefix that was added to engine names during import', '')
    .option('--filter <substring>', 'Only validate engines whose filename contains this substring', '')
    .option('--output-report <path>', 'Path for JSON validation report', './validation-report.json')
    .option('--dry-run', 'Show what would be validated without doing it');

  program.action(async (options) => {
    const startTime = Date.now();

    console.log('\n' + '='.repeat(70));
    console.log('Validate: Source Export vs Target Export');
    console.log('='.repeat(70));
    console.log(`Source dir:  ${options.sourceDir}`);
    console.log(`Target dir:  ${options.targetDir}`);
    console.log(`Prefix:      "${options.prefix}"`);
    if (options.filter) console.log(`Filter:      "${options.filter}"`);
    console.log(`Report:      ${options.outputReport}`);
    console.log('='.repeat(70) + '\n');

    // Read source JSON files
    let srcFiles = await getJsonFiles(options.sourceDir);
    const tgtFiles = await getJsonFiles(options.targetDir);
    const tgtFileSet = new Set(tgtFiles);

    if (srcFiles.length === 0) {
      console.log('❌ No JSON files found in source directory\n');
      return;
    }

    // Filter
    if (options.filter) {
      srcFiles = srcFiles.filter(f => f.includes(options.filter));
    }

    console.log(`Source: ${srcFiles.length} JSON files`);
    console.log(`Target: ${tgtFiles.length} JSON files\n`);

    // Dry run
    if (options.dryRun) {
      console.log('-'.repeat(70));
      console.log('Engines to validate:');
      console.log('-'.repeat(70));

      let matched = 0;
      let missing = 0;
      for (let i = 0; i < srcFiles.length; i++) {
        const srcName = path.basename(srcFiles[i], '.json');
        const tgtFileName = `${options.prefix}${srcName}.json`;
        const found = tgtFileSet.has(tgtFileName);
        console.log(`${i + 1}. ${srcName} → ${tgtFileName} ${found ? '✅' : '❌ MISSING'}`);
        if (found) matched++;
        else missing++;
      }

      console.log('-'.repeat(70));
      console.log(`Total:   ${srcFiles.length} engines`);
      console.log(`Matched: ${matched}`);
      console.log(`Missing: ${missing}`);
      console.log('-'.repeat(70) + '\n');
      console.log('💡 Run without --dry-run to start validation\n');
      return;
    }

    // Compare all engines
    console.log(`Starting validation of ${srcFiles.length} engines...\n`);

    const allResults = [];
    const totalStats = { passed: 0, failed: 0, missing: 0, failedEngines: [] };

    for (let i = 0; i < srcFiles.length; i++) {
      const srcFile = srcFiles[i];
      const engineName = path.basename(srcFile, '.json');
      const tgtFileName = `${options.prefix}${engineName}.json`;
      const progress = `[${i + 1}/${srcFiles.length}]`;

      // Check if target file exists
      if (!tgtFileSet.has(tgtFileName)) {
        totalStats.failed++;
        totalStats.missing++;
        const result = {
          passed: false,
          engine: engineName,
          mismatches: [`engine missing: "${tgtFileName}" not found in target directory`],
          checks: {}
        };
        totalStats.failedEngines.push(result);
        allResults.push(result);
        console.log(`${progress} ❌ MISSING: ${engineName} (no ${tgtFileName})`);
        continue;
      }

      try {
        const srcPath = path.join(options.sourceDir, srcFile);
        const tgtPath = path.join(options.targetDir, tgtFileName);
        const result = await compareEngine(engineName, srcPath, tgtPath);
        allResults.push(result);

        if (result.passed) {
          totalStats.passed++;
          console.log(`${progress} ✅ PASS: ${engineName}`);
        } else {
          totalStats.failed++;
          totalStats.failedEngines.push(result);
          console.log(`${progress} ❌ FAIL: ${engineName}`);
          result.mismatches.forEach(m => console.log(`    - ${m}`));
        }
      } catch (err) {
        totalStats.failed++;
        const result = {
          passed: false,
          engine: engineName,
          mismatches: [`error: ${err.message}`],
          checks: {}
        };
        totalStats.failedEngines.push(result);
        allResults.push(result);
        console.log(`${progress} ❌ ERROR: ${engineName}: ${err.message}`);
      }

      // Progress update every 50 engines
      if ((i + 1) % 50 === 0) {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`\n📊 Progress: ${i + 1}/${srcFiles.length} | ✅ ${totalStats.passed} | ❌ ${totalStats.failed} | ${elapsed}s elapsed\n`);
      }
    }

    // Final summary
    const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log('\n' + '='.repeat(70));
    console.log('Validation Complete');
    console.log('='.repeat(70));
    console.log(`Time:      ${totalTime}s`);
    console.log(`Total:     ${srcFiles.length} engines`);
    console.log(`Passed:    ${totalStats.passed}`);
    console.log(`Failed:    ${totalStats.failed}${totalStats.missing > 0 ? ` (${totalStats.missing} missing)` : ''}`);
    console.log(`Pass rate: ${((totalStats.passed / srcFiles.length) * 100).toFixed(1)}%`);
    console.log('='.repeat(70));

    if (totalStats.failed > 0) {
      console.log(`\n❌ Failed engines (${totalStats.failed}):`);
      totalStats.failedEngines.slice(0, 30).forEach(result => {
        console.log(`  - ${result.engine}:`);
        result.mismatches.forEach(m => console.log(`    ${m}`));
      });
      if (totalStats.failedEngines.length > 30) {
        console.log(`  ... and ${totalStats.failedEngines.length - 30} more (see report)`);
      }
    }

    // Write JSON report
    const report = {
      timestamp: new Date().toISOString(),
      sourceDir: options.sourceDir,
      targetDir: options.targetDir,
      prefix: options.prefix,
      summary: {
        total: srcFiles.length,
        passed: totalStats.passed,
        failed: totalStats.failed,
        missing: totalStats.missing,
        passRate: `${((totalStats.passed / srcFiles.length) * 100).toFixed(1)}%`,
        durationSeconds: parseFloat(totalTime)
      },
      results: allResults,
      failures: totalStats.failedEngines.map(r => ({
        engine: r.engine,
        mismatches: r.mismatches
      }))
    };

    await fs.writeFile(options.outputReport, JSON.stringify(report, null, 2));
    console.log(`\n📄 Report written to: ${options.outputReport}\n`);

    if (totalStats.failed > 0) {
      process.exit(1);
    }
  });

  await program.parseAsync(process.argv);
}

main().catch(err => {
  console.error('\n❌ Fatal error:', err.message);
  process.exit(1);
});
