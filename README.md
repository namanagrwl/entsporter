# Entsporter

An import / export and bulk migration tool for Elastic **Enterprise Search (App Search)** engine settings.

<!-- sources: http://www.theargonath.cc/pictures/ents/ents.html https://cdn.player.one/sites/player.one/files/2016/02/01/enterprise-star-trek.jpg -->
![Entsporter](/entsporter.png)

Entsporter allows you to **safely migrate, clone, and version-control App Search engine configurations** across environments and clusters.

---

## Supported Features

Entsporter can **export and import the following App Search configuration**:

### Engine Configuration
- Engine metadata (name, type, language)
- Schema 
- Curations
- Synonyms
- Search fields (weights)
- Result fields
- Boosts (value, functional, proximity)
- Precision & precision-enabled settings

### Crawler (via REST APIs)
- Domains  
- Entry Points (domain-scoped)  
- Crawl Rules (domain-scoped)  
- Sitemaps (domain-scoped)  

Crawler APIs are accessed via REST because they are not fully supported in the Enterprise Search JS SDK.

---

## Bulk Migration 

You can now:

- List **all App Search engines**
- Export **every engine automatically**
- Import them into a **different cluster**
- Apply **name prefixes** (e.g., `import-`)
- Run in **dry-run mode**
- Overwrite existing engines using `--force`

### Bulk Migration Options

| Option | Description | Default |
|--------|-------------|---------|
| `--from-endpoint` | Source cluster URL | Required |
| `--from-key` | Source private API key | Required |
| `--to-endpoint` | Target cluster URL | Required |
| `--to-key` | Target private API key | Required |
| `--concurrency <n>` | Number of engines to process in parallel | 5 |
| `--output-dir <path>` | Directory for temporary JSON files | `./engines-export` |
| `--target-prefix <prefix>` | Prefix to add to target engine names | (none) |
| `--state-file <path>` | State file for resume capability | `./migration-state.json` |
| `--force` | Overwrite existing engines on target | false |
| `--resume` | Resume from previous run | false |
| `--retry-failed-only` | Only retry previously failed engines | false |
| `--skip-existing` | Skip engines that already exist on target | false |
| `--cleanup` | Delete JSON files after successful import | false |
| `--dry-run` | List engines without migrating | false |
| `[filter]` | Substring filter for engine names | (none) |

This enables full environment-to-environment migrations such as:

- DEV → QA  
- QA → PROD  
- On-prem → Elastic Cloud  

---

## Compatibility

Fully tested with:
- **Elastic App Search 8.6 – 8.12 (Managed Cloud)**

Should also work with:
- **7.17+ (not officially tested)**

> ⚠️ Some App Search versions enforce a **hard 64-field schema limit**.  
> This tool assumes your deployment supports **incremental schema updates beyond 64 fields** (as validated in 8.x Cloud).

---

## Installation

```sh
git clone https://github.com/namanagrwl/entsporter.git
cd entsporter
npm install

```
---

## Usage

Export an App Search engine `parks` to a JSON file, `engine.json`.

```sh
npm run index.js export-app-search-engine parks -- \
  --app-search-endpoint "https://my-cloud-deployment.ent.us-central1.gcp.cloud.es.io" \
  --app-search-private-key "private-REDACTED" \
  --output-json "engine.json"
```

Import an exported engine's settings from a file `engine.json` into a new engine, `new-parks`.

```sh
npm run index.js import-app-search-engine new-parks -- \
  --app-search-endpoint "https://my-cloud-deployment.ent.us-central1.gcp.cloud.es.io" \
  --app-search-private-key "private-REDACTED" \
  --input-json "engine.json"
  --force
```

### Bulk Migration

Migrate multiple engines between clusters in parallel:

#### Dry Run (Always Do This First!)
```bash
node bulk-migrate-engines.js \
  --from-endpoint "https://source.elastic-cloud.com" \
  --from-key "source-private-key" \
  --to-endpoint "https://target.elastic-cloud.com" \
  --to-key "target-private-key" \
  --dry-run
```

#### Full Migration
```bash
node bulk-migrate-engines.js \
  --from-endpoint "https://source.elastic-cloud.com" \
  --from-key "source-private-key" \
  --to-endpoint "https://target.elastic-cloud.com" \
  --to-key "target-private-key" \
  --concurrency 5 \
  --force \
  --cleanup
```


#### Resume After Interruption
```bash
node bulk-migrate-engines.js \
  --from-endpoint "https://source.elastic-cloud.com" \
  --from-key "source-private-key" \
  --to-endpoint "https://target.elastic-cloud.com" \
  --to-key "target-private-key" \
  --concurrency 5 \
  --force \
  --resume
```

#### Retry Only Failed Engines
```bash
node bulk-migrate-engines.js \
  --from-endpoint "https://source.elastic-cloud.com" \
  --from-key "source-private-key" \
  --to-endpoint "https://target.elastic-cloud.com" \
  --to-key "target-private-key" \
  --concurrency 2 \
  --force \
  --resume \
  --retry-failed-only
```
---

## State File Format

The migration state is saved to `migration-state.json`:
```json
{
  "completed": ["engine-1", "engine-2", "engine-3"],
  "failed": [
    {
      "engine": "engine-4",
      "error": "Connection timeout"
    }
  ],
  "skipped": [],
  "startTime": 1702857600000
}
```

---
## Example exported engine settings JSON output

```json
{
  "read_only": {
    "name": "parks",
    "type": "default",
    "language": null
  },
  "schema": {
    "visitors": "number",
    "square_km": "number",
    "world_heritage_site": "text",
    "date_established": "date",
    "description": "text",
    "location": "geolocation",
    "acres": "text",
    "title": "text",
    "nps_link": "text",
    "states": "text"
  },
  "synonyms": [
    {
      "id": "syn-63d6e042a612f5da3c598f44",
      "synonyms": ["laptop", "computer", "pc", "ipad"]
    }
  ],
  "curations": [
    {
      "queries": ["mountain"],
      "promoted": ["park_saguaro"],
      "hidden": ["park_rocky-mountain"]
    }
  ],
  "searchSettings": {
    "search_fields": {
      "title": { "weight": 1 },
      "acres": { "weight": 9.6 }
    },
    "boosts": {
      "visitors": [
        { "type": "value", "factor": 2.7, "value": ["5"] }
      ]
    },
    "precision": 5,
    "precision_enabled": true
  },
  "crawler": {
    "domains": [],
    "entryPoints": [],
    "crawlRules": [],
    "sitemaps": []
  }
}
```

---

## Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                    Bulk Migration Script                     │
│  - Parallel processing (configurable concurrency)            │
│  - State management and resume capability                    │
│  - Progress tracking and statistics                          │
└───────────────────────┬─────────────────────────────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
        ▼                               ▼
┌───────────────┐               ┌───────────────┐
│ Export Script │               │ Import Script │
│ - Quiet mode  │               │ - Quiet mode  │
│ - Pagination  │               │ - Retry logic │
│ - REST API    │               │ - Force flag  │
└───────────────┘               └───────────────┘
```

