# -*- coding: utf-8 -*-
# Glue Spark Job: スキーマ/テーブル/カラム + 行数 を 1つのExcel(3シート) で S3 に出力
# Excelライブラリが無ければ CSV にフォールバック

import sys, os
from datetime import datetime, timezone, timedelta
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

# ===== Glue 起動 =====
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext); job.init(args["JOB_NAME"], args)

# ===== 接続情報 =====
ORACLE_URL  = "jdbc:oracle:thin:@//10.231.120.227:1521/BIZ08K"
JDBC_PROPS  = {"user": "AWG_DATARENKEI", "password": "buy0arTi0J", "driver": "oracle.jdbc.OracleDriver"}

# ===== 出力先（scripts直下。run_dt=JSTタイムスタンプ）=====
JST = timezone(timedelta(hours=9))
STAMP = datetime.now(JST).strftime("%Y%m%d_%H%M%S")
S3_BASE = f"s3://stg-bi-oci2rs-glue-s3/datalake/oracle_inventory_excel/run_dt={STAMP}"
S3_EXCEL_KEY = f"datalake/oracle_inventory_excel/run_dt={STAMP}/oracle_inventory.xlsx"
LOCAL_XLSX = f"/tmp/oracle_inventory_{STAMP}.xlsx"

# ===== SQL（システム系ユーザー除外）=====
EXCLUDE = "('SYS','SYSTEM','XDB','CTXSYS','MDSYS','ORDSYS','OUTLN','DBSNMP','APPQOSSYS','AUDSYS')"

Q_SCHEMAS = f"""
(SELECT owner,
        COUNT(*) AS total_objects,
        SUM(CASE WHEN object_type='TABLE' THEN 1 ELSE 0 END) AS tables,
        SUM(CASE WHEN object_type='VIEW'  THEN 1 ELSE 0 END) AS views
   FROM all_objects
  WHERE object_type IN ('TABLE','VIEW')
    AND owner NOT IN {EXCLUDE}
  GROUP BY owner
  ORDER BY owner) X
"""

Q_TABLES = f"""
(SELECT t.owner,
        t.table_name,
        t.num_rows,         -- 統計に基づく件数（NULLのこともある）
        t.last_analyzed,
        o.last_ddl_time
   FROM all_tables t
   LEFT JOIN all_objects o
          ON o.owner=t.owner AND o.object_name=t.table_name AND o.object_type='TABLE'
  WHERE t.owner NOT IN {EXCLUDE}
  ORDER BY t.owner, t.table_name) X
"""

Q_COLUMNS = f"""
(SELECT c.owner,
        c.table_name,
        o.object_type,                  -- TABLE or VIEW
        c.column_id,
        c.column_name,
        c.data_type,
        c.data_length,
        c.data_precision,
        c.data_scale,
        c.nullable
   FROM all_tab_columns c
   JOIN all_objects o
     ON o.owner=c.owner AND o.object_name=c.table_name AND o.object_type IN ('TABLE','VIEW')
  WHERE c.owner NOT IN {EXCLUDE}
  ORDER BY c.owner, c.table_name, c.column_id) X
"""

# ===== 取得 =====
df_schemas = spark.read.jdbc(url=ORACLE_URL, table=Q_SCHEMAS,  properties=JDBC_PROPS)
df_tables  = spark.read.jdbc(url=ORACLE_URL, table=Q_TABLES,   properties=JDBC_PROPS)
df_cols    = spark.read.jdbc(url=ORACLE_URL, table=Q_COLUMNS,  properties=JDBC_PROPS)

# ちょい表示（任意）
df_schemas.show(50, truncate=False)
df_tables.show(20, truncate=False)
df_cols.show(20, truncate=False)

# ===== Excel書き出し（pandas + xlsxwriter/openpyxl）。無ければCSVにフォールバック =====
def write_csv(df, subdir):
    (df.coalesce(1)
       .write.mode("overwrite")
       .option("header", True)
       .csv(f"{S3_BASE}/{subdir}/"))

def autosize_excel_columns(ws, df):
    # ざっくりオートサイズ（最大50文字まで）
    for idx, col in enumerate(df.columns):
        maxlen = max([len(str(col))] + [len(str(x)) if x is not None else 0 for x in df[col].values.tolist()])
        ws.set_column(idx, idx, min(maxlen + 2, 50))

try:
    import pandas as pd
    engine = None
    try:
        import xlsxwriter  # noqa
        engine = "xlsxwriter"
    except Exception:
        try:
            import openpyxl  # noqa
            engine = "openpyxl"
        except Exception:
            engine = None

    if engine is None:
        raise ImportError("No Excel writer engine available")

    # 注意: toPandas() はドライバに集約します（件数が極端に多いとメモリを食います）
    pdf_schemas = df_schemas.toPandas()
    pdf_tables  = df_tables.toPandas()
    pdf_cols    = df_cols.toPandas()

    with pd.ExcelWriter(LOCAL_XLSX, engine=engine) as writer:
        pdf_schemas.to_excel(writer, index=False, sheet_name="schemas")
        pdf_tables.to_excel(writer,  index=False, sheet_name="tables")
        pdf_cols.to_excel(writer,    index=False, sheet_name="columns")

        # 体裁ちょい整える（xlsxwriterのみ）
        if engine == "xlsxwriter":
            wb = writer.book
            for name, pdf in [("schemas", pdf_schemas), ("tables", pdf_tables), ("columns", pdf_cols)]:
                ws = writer.sheets[name]
                autosize_excel_columns(ws, pdf)
                # フリーズペイン
                ws.freeze_panes(1, 0)

    # S3へアップロード
    s3 = boto3.client("s3")
    bucket = "stg-bi-oci2rs-glue-s3"
    s3.upload_file(LOCAL_XLSX, bucket, S3_EXCEL_KEY)
    print(f"[DONE] Excel uploaded to s3://{bucket}/{S3_EXCEL_KEY}")

except Exception as e:
    # フォールバック: CSVで3分割出力
    print(f"[WARN] Excel出力に失敗（{e}）。CSV出力にフォールバックします。")
    write_csv(df_schemas, "schemas")
    write_csv(df_tables,  "tables")
    write_csv(df_cols,    "columns")
    print(f"[DONE] CSVs written under: {S3_BASE}/(schemas|tables|columns)/")

job.commit()
print(f"[JOB END] run_dt={STAMP}")
