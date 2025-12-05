# 🏆 Sales Medallion Pipeline - Accounts Data Engineering

End-to-end Medallion Architecture data pipeline for accounts analytics, deployed using Databricks Asset Bundles (DABs) with CI/CD via GitHub Actions.

## 📊 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────┐      ┌──────────┐      ┌──────────────────────────┐   │
│  │  BRONZE  │ ───► │  SILVER  │ ───► │          GOLD            │   │
│  │          │      │          │      │                          │   │
│  │ Raw Data │      │ Cleansed │      │ ┌──────────────────────┐ │   │
│  │ + Metadata│     │ + Derived│      │ │ gold_accounts        │ │   │
│  │          │      │  Columns │      │ │ gold_country_analytics│ │   │
│  └──────────┘      └──────────┘      │ │ gold_size_analytics  │ │   │
│                                       │ └──────────────────────┘ │   │
│                                       └──────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## 🗂️ Project Structure

```
sales-medallion-pipeline/
├── databricks.yml              # Bundle configuration
├── resources/
│   └── medallion_accounts_job.yml  # Job definition
├── src/
│   ├── bronze/
│   │   └── 01_ingest_accounts.py   # Raw data ingestion
│   ├── silver/
│   │   └── 01_transform_accounts.py # Data cleansing
│   └── gold/
│       ├── 01_gold_accounts.py      # Accounts dimension
│       ├── 02_gold_country_analytics.py  # Country aggregations
│       └── 03_gold_size_analytics.py     # Size aggregations
├── .github/
│   └── workflows/
│       └── ci.yml              # CI/CD pipeline
└── README.md
```

## 🥉 Bronze Layer
**Table:** `bronze_accounts`

Raw data ingestion with:
- Original source field names preserved
- Ingestion metadata (timestamp, source system)
- Rescued data column for schema evolution

## 🥈 Silver Layer
**Table:** `silver_accounts`

Data transformation including:
- Column standardization (snake_case)
- Data quality validation
- Null handling
- Derived columns:
  - `company_size`: Employee-based classification
  - `revenue_tier`: Revenue-based tier

## 🥇 Gold Layer

### `gold_accounts`
Curated accounts dimension for business analytics.

### `gold_country_analytics`
Country-level aggregations:
- Account counts, revenue totals, employee metrics
- Revenue and accounts rankings

### `gold_size_analytics`
Company size segment analytics:
- Distribution across size tiers
- Revenue concentration analysis

## 🚀 Deployment

### Prerequisites
1. Databricks workspace with Unity Catalog
2. GitHub repository with secrets configured:
   - `DATABRICKS_HOST`: Your workspace URL
   - `DATABRICKS_TOKEN`: Personal Access Token

### Multi-Environment Targets

| Target | Description | Trigger |
|--------|-------------|--------|
| `dev` | Development workspace | `develop` branch or manual |
| `staging` | Pre-production testing | `main` branch |
| `prod` | Production deployment | `main` branch (after staging) |

### Manual Deployment

```bash
# Validate the bundle
databricks bundle validate -t dev

# Deploy to development
databricks bundle deploy -t dev

# Run the pipeline
databricks bundle run medallion_accounts_pipeline -t dev
```

### CI/CD Workflow

The GitHub Actions workflow automatically:
1. ✅ Validates bundle on every push
2. 🚀 Deploys to `dev` on `develop` branch
3. 🎭 Deploys to `staging` on `main` branch
4. 🏭 Deploys to `prod` on `main` branch (after staging)

## 📋 Table Metadata

All tables include:
- Table comments describing purpose
- Column comments with descriptions
- Properties for lineage tracking:
  - `quality_tier`: bronze/silver/gold
  - `data_domain`: sales
  - `pipeline_name`: medallion-accounts

## 🔐 Security

- No hard-coded credentials
- Uses GitHub Secrets for CI/CD
- Workspace-scoped deployments
- User context for development

## 📚 References

- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Delta Lake](https://docs.databricks.com/delta/index.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
