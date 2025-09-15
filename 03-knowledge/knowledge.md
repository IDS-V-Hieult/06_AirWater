Spectrum Utilization is enabled with S3 external reference capability.

The pro-bi-cluster has the necessary configuration for Redshift Spectrum:

1. IAM Role: Redshift-role is attached to the cluster
2. S3 Access: The role has AmazonS3FullAccess policy attached
3. Glue Integration: AWSGlueConsoleFullAccess policy enables integration with AWS Glue Data Catalog

This setup allows the cluster to:
• Query external data stored in S3 using Spectrum
• Access S3 buckets for external table definitions
• Use AWS Glue Data Catalog for metadata management

So the answer to your question is Yes - the cluster is configured for "S3 external reference" through Redshift Spectrum.

> Spectrum Utilization --> what does it mean?

> Spectrum Utilization refers to whether Amazon Redshift Spectrum is enabled and being used by your Redshift cluster.

What it means:

• **Spectrum** = Redshift's feature to query data directly in S3 without loading it into the cluster
• **Utilization** = Whether this capability is actively configured and available

When set to "Yes (S3 external reference)":
• Your cluster can run SQL queries against data stored in S3
• Data stays in S3 (no need to load/copy into Redshift tables)
• Uses external tables that reference S3 locations
• Leverages separate Spectrum compute resources (billed separately)

Key benefits:
• Query petabytes of S3 data without moving it
• Separate compute scaling for S3 queries
• Cost-effective for infrequently accessed data
• Integrates with AWS Glue Data Catalog for metadata

In your pro-bi-cluster case, it's enabled because you have the required IAM role with S3 access permissions, allowing Spectrum to reference and
query external S3 data sources.


================================
"Pause/Resume scheduled operation" is configured.

The pro-bi-cluster has active scheduled pause/resume operations:

Daily Schedule:
• **Resume**: Every day at 15:00 UTC (3:00 PM)
• **Pause**: Every day at 15:45 UTC (3:45 PM)

Monthly Schedule:
• **Resume**: Last day of each month at 14:15 UTC
• **Pause**: Last day of each month at 15:45 UTC

All schedules are ACTIVE and managed by the redshift-scheduler-role IAM role. This automated pause/resume functionality helps optimize costs by
stopping the cluster during non-business hours.

> Daily Schedule:
• **Resume**: Every day at 15:00 UTC (3:00 PM)
• **Pause**: Every day at 15:45 UTC (3:45 PM)

Monthly Schedule:
• **Resume**: Last day of each month at 14:15 UTC
• **Pause**: Last day of each month at 15:45 UTC
--> how to see above setting in aws console?

> To view the scheduled pause/resume settings in the AWS Console:

1. Navigate to Amazon Redshift Console
   • Go to AWS Console → Services → Amazon Redshift

2. Select your cluster
   • Click on "Clusters" in the left navigation
   • Click on "pro-bi-cluster"

3. View scheduled actions
   • In the cluster details page, look for the "Actions" tab or section
   • Click on "Scheduled actions"

4. Alternative path
   • From the main Redshift dashboard, click "Scheduled actions" in the left navigation panel
   • Filter by cluster name "pro-bi-cluster"

You'll see a table showing:
• Action names (pro-bi-cluster-schedule-pause, pro-bi-cluster-schedule-resume, etc.)
• Schedule expressions (cron format)
• Target actions (Pause/Resume)
• Status (Active/Inactive)
• Next execution times

The console will display the same cron schedules we found via CLI, but in a more user-friendly format showing the actual times and frequencies.

===========
To see the Concurrency Scaling setting on AWS Console:

 - Parameter Groups:
1. Go to Amazon Redshift Console
2. Click Parameter groups in the left sidebar
3. Click default.redshift-1.0 (the parameter group used by your cluster)
4. Search for parameter: max_concurrency_scaling_clusters
5. The Value column will show 1


=================
WLM and QMR are Amazon Redshift query management features:

## **WLM (Workload Management)**
Purpose: Controls how queries are executed and prioritized in Redshift

Key features:
• **Query queues:** Organizes queries into different queues based on user groups or query characteristics
• **Concurrency:** Controls how many queries can run simultaneously in each queue
• **Memory allocation:** Assigns memory to each queue
• **Timeout settings:** Sets maximum execution time for queries
• **Priority:** Determines which queries get resources first

Types:
• **Automatic WLM:** Redshift manages queues automatically (recommended)
• **Manual WLM:** You configure specific queues and settings

## **QMR (Query Monitoring Rules)**
Purpose: Monitors and controls individual query behavior

Key features:
• **Performance thresholds:** Set limits on query execution time, CPU usage, disk I/O
• **Automatic actions:** Abort, log, or hop queries that exceed thresholds
• **Resource monitoring:** Track memory usage, rows processed, nested loop joins
• **Query patterns:** Monitor specific SQL patterns or operations

Common QMR rules:
• Abort queries running longer than X minutes
• Log queries using excessive CPU
• Hop long-running queries to a different queue

Relationship:
• WLM manages overall workload distribution
• QMR monitors individual query performance within WLM queues
• Both work together to optimize cluster performance and prevent resource contention

Your cluster uses Automatic WLM (as seen in the wlm_json_configuration parameter).

=============
WLM (Workload Management) Settings:

Configuration Type: Automatic WLM
• **Parameter:** wlm_json_configuration
• **Value:** [{"auto_wlm":true}]
• **Mode:** Fully automatic management by Redshift

Key Settings:
• **Queue Management:** Automatic (Redshift manages queues dynamically)
• **Concurrency:** Automatically determined based on workload
• **Memory Allocation:** Automatically optimized
• **Query Routing:** Intelligent routing based on query characteristics

QMR (Query Monitoring Rules):
• **Statement Timeout:** 0 (no timeout limit)
• **Query Group:** default (no custom query groups)
• **Custom QMR Rules:** None configured (using defaults)

Summary:
• **WLM:** Automatic WLM enabled - Redshift automatically manages all workload distribution
• **QMR:** Default settings - No custom query monitoring rules configured
• **Approach:** Hands-off, letting Redshift optimize performance automatically

This is the recommended modern approach where Redshift's machine learning algorithms automatically optimize query performance, concurrency, and resource
allocation without manual queue configuration.

> is it "Auto WLM, yes. Control runaway queries with QMR."?

> Based on the cluster configuration I checked:

Auto WLM: YES ✅
• Parameter: wlm_json_configuration = [{"auto_wlm":true}]
• Automatic workload management is enabled

Control runaway queries with QMR: NO ❌
• statement_timeout = 0 (no timeout limit)
• No custom QMR rules configured
• Using default settings only

Current Status:
• ✅ Auto WLM enabled
• ❌ No QMR controls for runaway queries

Answer: Partially - "Auto WLM, yes."

The cluster has Auto WLM but lacks QMR configuration to control runaway queries. To match the expected setting, you would need to configure statement timeouts or custom QMR rules to abort/control long-running queries.


================
Based on the cluster configuration:

Communications/Encryption Settings:

require_ssl: NO ❌
• Parameter: require_ssl = false (from previous parameter check)
• SSL connections are not required

Enhanced VPC Routing: NO ❌
• Parameter: EnhancedVpcRouting = false
• Enhanced VPC routing is disabled

Current Configuration:
• ❌ require_ssl = false (not 1/true)
• ❌ EnhancedVpcRouting = false (not enabled)

Answer to your question:
NO - The cluster is NOT configured with "require_ssl=1、Enhanced VPC Routing".

Current Settings:
• SSL is optional (not required)
• Enhanced VPC Routing is disabled
• Traffic uses standard internet routing instead of VPC routing

To match the expected configuration, both require_ssl would need to be set to true and EnhancedVpcRouting would need to be enabled.

> Current Configuration:
• ❌ require_ssl = false (not 1/true)
• ❌ EnhancedVpcRouting = false (not enabled)
--> how to see on aws console?

> To see the Communications/Encryption settings on AWS Console:

Method 1 - Cluster Properties:
1. Go to Amazon Redshift Console
2. Select Asia Pacific (Tokyo) region
3. Click Clusters → pro-bi-cluster
4. Click Properties tab
5. Scroll down to Network and security section
6. Look for:
   • **Enhanced VPC routing**: Shows "Disabled"
   • **Require SSL**: Shows "No" or "Disabled"

Method 2 - Parameter Groups:
1. Go to Amazon Redshift Console
2. Click Parameter groups in left sidebar
3. Click default.redshift-1.0
4. Search for parameter: require_ssl
5. The Value column will show false

Method 3 - Cluster Configuration Details:
1. Clusters → pro-bi-cluster
2. Click Configuration tab
3. Under Security section:
   • **SSL requirement**: No/Disabled
   • **Enhanced VPC routing**: Disabled

========================
The secret contains connection details pointing to pro-bi-cluster.cm9fwkbizobx.ap-northeast-1.redshift.amazonaws.com
--> how to see on aws console?

> To see the secret's connection details on AWS Console:

Method 1 - Secrets Manager Console:
1. Go to AWS Secrets Manager Console
2. Select Asia Pacific (Tokyo) region
3. Find and click prod-bi_awi-qs-secrets
4. Click Retrieve secret value button
5. You'll see the JSON with:

json
   {
     "host": "pro-bi-cluster.cm9fwkbizobx.ap-northeast-1.redshift.amazonaws.com",
     "port": 5439,
     "username": "ids_maint",
     "password": "...",
     "engine": "redshift",
     "dbClusterIdentifier": "pro-bi-cluster"
   }

==============================



=========================================
Bucket Policy Approach for Redshift Audit Logging:

Steps in AWS Console:
1. Go to S3 Console → bucket aws-redshift-logs-496620541602
2. Click "Permissions" tab
3. Scroll to "Bucket policy" section
4. Click "Edit" button
5. Paste this policy:

json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RedshiftAuditLogging",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::902366379725:root"
      },
      "Action": [
        "s3:GetBucketAcl",
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::aws-redshift-logs-496620541602",
        "arn:aws:s3:::aws-redshift-logs-496620541602/*"
      ]
    }
  ]
}


6. Click "Save changes"

What this does:
• **Principal:** 902366379725 = Redshift service account for ap-northeast-1
• **GetBucketAcl:** Allows Redshift to read bucket permissions
• **PutObject:** Allows Redshift to write audit log files
• **Resource:** Covers both bucket and all objects inside

Advantages:
• No need to modify ACLs
• No need to disable Block Public Access
• More granular permissions
• Easier to manage

After applying this policy, you should be able to enable Redshift audit logging without ACL errors.

