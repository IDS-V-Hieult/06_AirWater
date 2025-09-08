# stg-bi-oci2rs-load-glue Job Flow

## Simple Operation Flow

```
Oracle DB (BIZ08K)           AWS Glue ETL Job              Redshift DWH
┌─────────────────┐         ┌──────────────────┐         ┌─────────────────┐
│ 10.231.120.227  │────────▶│ stg-bi-oci2rs-   │────────▶│ pro-bi-cluster  │
│ Port: 1521      │         │ load-glue        │         │ Port: 5439      │
│ Customer: AWI   │         │                  │         │ DB: stg-awi-bi  │
└─────────────────┘         └──────────────────┘         └─────────────────┘
                                      │
                                      ▼
                            ┌──────────────────┐
                            │ S3 Data Lake     │
                            │ stg-bi-oci2rs-   │
                            │ glue-s3/datalake │
                            └──────────────────┘
```

## Detailed Flow Steps

1. **Extract** - Connect to Oracle DB using `oci-rt-connection`
2. **Transform** - Process data in Glue ETL (2 G.1X workers)
3. **Load** - Store in S3 Data Lake and Redshift DWH
4. **Alert** - Send notifications via SNS on completion/failure

## Key Components

- **Source**: Oracle BIZ08K database
- **Processing**: AWS Glue ETL job (Python script)
- **Storage**: S3 bucket for data lake
- **Target**: Redshift cluster for BI analytics
- **Monitoring**: SNS alerts + CloudWatch logs

## Current Status
⚠️ **Issues**: Multiple recent failures due to connection and authentication problems
