# 03. コンピュート設計

## 採用AWSサービス（本章該当）

| サービス名         | 用途                                 |
|--------------------|--------------------------------------|
| AWS Glue           | ETL処理（OCI→S3→Redshift）           |
| AWS Lambda         | ユーザー管理処理、ログ出力処理等     |
| Amazon EventBridge | Lambdaスケジューリング・連携         |
| AWS Step Functions | ETL全体のオーケストレーション |

---

## 1. Step Functions 構成（オーケストレーション）

以下の構成でOCI上の売買情報のデータを直列でRedshiftに取り込む。

![AWS構成図](../assets/ETL構成図.png)

---

## 2. Glue構成（ETL）

OCI 上の売買情報データを Redshift に取り込む ETL は Glue ジョブで実施し、起動は Step Functions が担う。

### 主な特徴

- JDBC（OCI）→ S3 着地（Parquet/圧縮）→ Redshift COPY
- Secrets Managerで認証情報を管理
- 失敗時はリトライ／リトライ上限超過でジョブ失敗→Step Functions 側で Catch

> 詳細なETL構成については「ETL設計書（別文書）」に記載

---

## 3. Lambda関数構成

本システムには、以下のAWS Lambda関数を導入している。  
主にユーザー管理、ログ出力、QuickSight連携処理を担う。

### 関数一覧（2025年8月現在）

| 関数名                                  | 主な用途                         | ランタイム     |
|------------------------------------------|----------------------------------|----------------|
| `bi_awi-qsevent-log-function`           | QuickSightイベントログ出力        | Python 3.9     |
| `bi_awi-qsgroup-add-function`           | QuickSightグループ追加            | Python 3.9     |
| `call-stg-bi_awi-qsevent-log-function`  | 上記ログ関数の呼び出し            | Python 3.13    |
| `stg-bi_awi-qsevent-log-function`       | ログ出力ステージング（検証用）    | Python 3.9     |
| `user-suspension-function`              | ユーザー停止処理                  | Python 3.13    |
| `user-restriction-function`             | ユーザー制限処理（権限制御）      | Python 3.13    |
| `stg-user-suspension-function`          | 停止処理ステージング（検証用）    | Python 3.13    |
| `stg-user-restriction-function`         | 制限処理ステージング（検証用）    | Python 3.12    |

---

[前の章へ](02_network.md)｜[目次へ](README.md)｜[次の章へ](04_storage.md)