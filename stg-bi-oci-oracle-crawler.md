# stg-bi-oci-oracle-crawler Flow

## Simple Operation Flow

```
Oracle Database (BIZ08K)         AWS Glue Crawler              Glue Data Catalog
┌─────────────────────────┐      ┌──────────────────┐         ┌─────────────────────┐
│ 10.231.120.227:1521     │─────▶│ stg-bi-oci-      │────────▶│ stg-bi-oracle_      │
│ Schema: BIZ08K/%        │      │ oracle-crawler   │         │ catalog (missing)   │
│ Tables: 4,142 discovered│      │                  │         │                     │
└─────────────────────────┘      └──────────────────┘         └─────────────────────┘
                                          │
                                          ▼
                                 ┌──────────────────┐
                                 │ Connection:      │
                                 │ stg-bi-oci2rs-   │
                                 │ connection       │
                                 │ (JDBC Oracle)    │
                                 └──────────────────┘
                                          │
                                          ▼
                                 ┌──────────────────┐
                                 │ Secrets Manager: │
                                 │ stg-bi-oci-      │
                                 │ cred-secret      │
                                 └──────────────────┘
```

## Detailed Flow Steps

1. **Connect** - Use JDBC connection to Oracle BIZ08K database
2. **Authenticate** - Retrieve credentials from AWS Secrets Manager
3. **Discover** - Scan all tables in BIZ08K schema (4,142 tables found)
4. **Catalog** - Create metadata entries in Glue Data Catalog
5. **Update** - Apply schema change policies (UPDATE_IN_DATABASE)

## Crawler Configuration

- **Target**: Oracle database BIZ08K/% (all schemas)
- **Connection**: stg-bi-oci2rs-connection
- **Role**: stg-bi-oci2rs-glue-role
- **Runtime**: ~72.7 minutes per run
- **Policy**: CRAWL_EVERYTHING on each run

## Current Status

- **State**: READY
- **Last Run**: September 2, 2025 (SUCCEEDED)
- **Tables Discovered**: 4,142
- **Issue**: Target database "stg-bi-oracle_catalog" missing from catalog

## Purpose

Provides metadata discovery for the OCI-to-Redshift pipeline by cataloging Oracle source database schema for staging environment data verification.
