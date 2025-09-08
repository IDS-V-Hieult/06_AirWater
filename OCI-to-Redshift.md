# OCI-to-Redshift Pipeline Flow

## Multi-Environment Pipeline Overview

```
Oracle DB (BIZ08K)                    AWS Glue Jobs                     Redshift Cluster
┌─────────────────┐                  ┌─────────────────┐               ┌─────────────────┐
│ 10.231.120.227  │                  │ Production:     │               │ pro-bi-cluster  │
│ Port: 1521      │─────────────────▶│ OCI-to-Redshift │──────────────▶│                 │
│ Customer: AWI   │                  └─────────────────┘               │ awi-bi-db       │
└─────────────────┘                           │                        └─────────────────┘
         │                                    │                                 │
         │                           ┌─────────────────┐                       │
         └──────────────────────────▶│ Staging:        │──────────────────────▶│
                                     │ stg-bi-oci2rs-  │                       │
                                     │ load-glue       │                       │
                                     └─────────────────┘               ┌─────────────────┐
                                              │                        │ stg-awi-bi-db   │
                                     ┌─────────────────┐               └─────────────────┘
                                     │ Testing:        │
                                     │ test-stg-bi-    │
                                     │ oci2rs-load-    │
                                     │ glue_ver2       │
                                     └─────────────────┘
```

## Environment Flow Details

### Production Environment
```
Oracle BIZ08K → OCI-to-Redshift → pro-bi-cluster/awi-bi-db
```
- **Job:** OCI-to-Redshift
- **Role:** OCI-to-Redshift-Glue-Role-ProBiCluster-ETL
- **Status:** ⚠️ Security group issues

### Staging Environment
```
Oracle BIZ08K → stg-bi-oci2rs-load-glue → pro-bi-cluster/stg-awi-bi-db
```
- **Job:** stg-bi-oci2rs-load-glue
- **Role:** stg-bi-oci2rs-glue-role
- **Status:** ⚠️ Authentication failures

### Testing Environment
```
Oracle BIZ08K → test-stg-bi-oci2rs-load-glue_ver2 → Development
```
- **Job:** test-stg-bi-oci2rs-load-glue_ver2
- **Mode:** Notebook (development)
- **Creator:** tanaka@ids.co.jp

## Shared Components

- **Oracle Connection:** oci-rt-connection
- **S3 Storage:** 
  - oci-to-redshit-probi (production)
  - stg-bi-oci2rs-glue-s3 (staging)
- **Target Cluster:** pro-bi-cluster.cm9fwkbizobx.ap-northeast-1.redshift.amazonaws.com

## Current Issues
- Production: Security group configuration problems
- Staging: Oracle authentication and connection failures
- Testing: Active development with enhanced library support
