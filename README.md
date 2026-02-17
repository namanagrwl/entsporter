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

## Features

- 🚀 **Two-Step Process** - Export all engines, then import all engines
- ⚡ **Parallel Import** - Import multiple engines concurrently (configurable)
- 🔄 **Sequential Export** - Reliable one-at-a-time export
- 💪 **Force Overwrite** - Delete and recreate existing engines with `--force`
- 🎯 **Filter Support** - Export only engines matching a pattern
- 🔇 **Quiet Mode** - Clean logging for concurrent operations
- 📊 **Progress Tracking** - Real-time statistics and ETA
- 🛡️ **Reliable Deletion** - 30-second wait ensures names are fully released
- 📦 **Complete Export** - Schema, synonyms, curations, search settings, and crawler configs
- 🧹 **Cleanup Option** - Automatically delete JSON files after successful import


---

## Bulk Migration 

### Quick Start

### Step 1: Export All Engines from Source
```bash
node bulk-export.js \
  --endpoint "https://source-cluster.elastic-cloud.com" \
  --key "source-private-key" \
  --output-dir "./my-engines"
```

### Step 2: Import All Engines to Target
```bash
# Dry run first (recommended)
node bulk-import.js \
  --endpoint "https://target-cluster.elastic-cloud.com" \
  --key "target-private-key" \
  --input-dir "./my-engines" \
  --dry-run

# Actual import
node bulk-import.js \
  --endpoint "https://target-cluster.elastic-cloud.com" \
  --key "target-private-key" \
  --input-dir "./my-engines" \
  --concurrency 5 \
  --force
```

Done! ✅

---

## Complete Workflow Example

### Scenario: Migrate 535 engines from dev to prod
```bash
# Step 1: Export all engines from dev cluster
node bulk-export.js \
  --endpoint "https://dev-cluster.elastic-cloud.com" \
  --key "dev-private-key" \
  --output-dir "./dev-engines"

# Output: 535 JSON files created in ./dev-engines/

# Step 2: (Optional) Backup the JSON files
cp -r ./dev-engines ./dev-engines-backup

# Step 3: Dry run to verify what will be imported
node bulk-import.js \
  --endpoint "https://prod-cluster.elastic-cloud.com" \
  --key "prod-private-key" \
  --input-dir "./dev-engines" \
  --prefix "prod-******" \
  --dry-run

# Step 4: Import to prod cluster (in tmux/screen for long-running task)

node bulk-import.js \
  --endpoint "https://prod-cluster.elastic-cloud.com" \
  --key "prod-private-key" \
  --input-dir "./dev-engines" \
  --prefix "prod-*****" \
  --concurrency 5 \
  --force \
  --cleanup
  
# Step 5: Export all engines from prod cluster

node bulk-export.js \
  --endpoint "https://prod-cluster.elastic-cloud.com" \
  --key "prod-private-key" \
  --output-dir "./prod-engines"


# Step 6: Validation 

node bulk-validate.js \
  --source-dir ./dev-engines \
  --target-dir ./prod-engines \
  
```



### Bulk Migration Options

| Option | Description | Default |
|--------|-------------|---------|
| `--endpoint <url>` | Target cluster URL | Required |
| `--key <key>` | Target API private key | Required |
| `--input-dir <path>` | Directory with JSON files | `./engines-export` |
| `--concurrency <n>` | Engines to import in parallel | `5` |
| `--prefix <prefix>` | Prefix to add to engine names | (none) |
| `--force` | Delete existing engines before import | `false` |
| `--cleanup` | Delete JSON files after successful import | `false` |
| `--dry-run` | Preview without importing | `false` |

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
│                  Export All Engines Script                   │
│  - Lists all engines from source cluster                     │
│  - Exports one at a time (sequential)                        │
│  - Creates JSON file per engine                              │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
              ┌──────────────────┐
              │   JSON Files     │
              │   (Backup Safe)  │
              └──────────┬───────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  Import All Engines Script                   │
│  - Reads all JSON files from directory                       │
│  - Imports with configurable concurrency (default: 5)        │
│  - Progress tracking and statistics                          │
│  - Optional cleanup after success                            │
└─────────────────────────────────────────────────────────────┘
```

