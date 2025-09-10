# 05. データベース設計（Redshift）

## 1. 概要
本システムは **Amazon Redshift（RA3）** を分析基盤として採用し、Glue による取り込みデータを格納、QuickSight で可視化する。  
ネットワークは Enhanced VPC Routing を前提とし、KMS による暗号化・監査ログ出力を行う。

---

## 2. クラスター構成

| 項目               | 本番環境                              | 検証環境                      |
|--------------------|---------------------------------------|-------------------------------|
| クラスター名       | analytics-cluster                     | analytics-stg-cluster         |
| ノードタイプ       | ra3.xlplus                            | ra3.large                     |
| ノード数           | 2（可用性・性能）                     | 1（コスト最適化）             |
| スペクトラム利用   | 有（S3外部参照）                      | 有                            |
| 停止/起動          | Pause/Resume をスケジュール運用       | 同左                          |
| 認証               | IAM 一時認証 + Secrets Manager        | IAM 一時認証 + Secrets Manager |
| 通信/暗号          | `require_ssl=1`、Enhanced VPC Routing | 同左                          |
| ログ/監査          | 監査ログを S3（KMS）へ出力            | 同左                          |
| WLM/QMR            | Auto WLM、有。QMRで暴走クエリを制御   | Auto WLM                      |
| Concurrency Scaling| 必要時に有効                          | 不要（既定は無効）            |
| スナップショット   | 自動 + 日次手動。7日保持/DRコピー     | 自動（短期保持）              |

> 停止/起動は夜間のETL処理の一環として実施。
> 処理開始のタイミングで起動し、完了したら停止を行う。

---

## 3. スキーマ設計（レイヤリング）

| スキーマ | 用途                         | 例 |
|---------|------------------------------|----|
| `stg`   | 取り込み直後の一時/整形前    | Glue → COPY/INSERT |
| `core`  | 正規化・履歴保持（事実/次元）| DWHコアモデル     |
| `dm`    | マート/BI最適化              | 集計・ワイドテーブル |
| `ext`   | Spectrum 外部スキーマ         | S3 Parquet 参照   |

> 環境はクラスターで分離するため、スキーマ名に `prod/stg` は付与しない。

---

## 4. テーブル設計ガイドライン

- **分布/ソート/エンコード**：基本は **AUTO**。大型結合キーのみ `DIST KEY` / `SORTKEY` を明示。  
- **圧縮**：`AZ64` / `ZSTD` を前提（AUTOに委譲）。  
- **キー設計**：`IDENTITY` は衝突・再採番に注意。自然キー or 複合キーを検討。  
- **ビュー**：**Late-binding View** を標準。スキーマ差し替えに強い。  
- **デフォルト権限**：`ALTER DEFAULT PRIVILEGES` で将来作成テーブルにも権限継承。  
- **メタ/統計**：`ANALYZE` は自動最適化に任せ、必要時のみ対象限定で実行。

---

## 5. アクセス制御（DBロール/RLS）

### 5.1 ロールと権限
| ロール名              | 付与権限（抜粋）                                       |
|----------------------|--------------------------------------------------------|
| `role_admin`         | 全スキーマの DDL/DML、メンテナンス                     |
| `role_etl_writer`    | `stg/core` の INSERT/UPDATE/DELETE、限定DDL            |
| `role_bi_reader`     | `dm` の SELECT（将来作成テーブル含む）                 |
| `role_audit`         | システムビュー/ログ参照                                |

> 人の直接ログインは原則禁止。IdC/SSO（IAM 一時認証）経由でロールを付与。

### 5.2 ユーザー/ロール例
| プリンシパル                          | 役割/接続元        | 付与ロール                 |
|--------------------------------------|--------------------|---------------------------|
| `prod-bi-cluster-admin`（管理者）    | 管理               | `role_admin`              |
| `IAM: Redshift-Glue-access-role`     | Glue（ETL）        | `role_etl_writer`         |
| `IAM: Redshift-qs-access-role`       | QuickSight（BI）   | `role_bi_reader`          |

> `rdsdb` はシステムユーザーのため操作不可。  
> 監査要件により、ログイン/クエリ/接続ログは S3（KMS）へ出力。

---

## 6. UNLOAD / Spectrum（外部テーブル）

- **形式**：**Parquet + Snappy** を標準（CSVは原則非推奨）。  
- **パーティション**：`dt=YYYY-MM-DD` 等で日付パーティション。**パーティション投影**を使用。  
- **カタログ**：Glue Data Catalog を基準とし、`CREATE EXTERNAL SCHEMA FROM data catalog`。

> UNLOAD 例：`UNLOAD ('SELECT ...') TO 's3://bucket/prefix/' WITH PARQUET ENCRYPTED KMS_KEY_ID '...'`  
> Spectrum は **S3アクセスポイント/VPC限定** とし、Lake Formation/Access Analyzer の統制下で運用。

---

## 7. 運用・監視

- **メトリクス**：WLMキュー待ち、ディスク/スロット、クエリ時間、Spectrumスキャンバイト。  
- **QMR**：長時間/大スキャン/大スキューを Abort/Log に分類。  
- **スナップショット**：自動 + 日次手動。**クロスリージョンコピー**でDR。  
- **障害復旧**：復旧Runbook（RPO/RTO）を整備。  
- **ログ**：ユーザー/クエリ/接続ログ → S3（KMS）。CloudWatch Alarms連携。

---

## 8. コスト最適化

- RA3 は計算/ストレージ分離。ノードサイズ/数を**定期見直し**。  
- **Concurrency Scaling** はBIピーク時のみ。クレジット消費を可視化。  
- **Spectrum**：Parquet＋パーティションでスキャン削減。不要列を**明示選択**。  
- QuickSight は **SPICE活用**でRedshift負荷を低減。

---

## 9. DB SLO（指標/目標/測定/環境/責任）

| ID   | SLO（要件）                       | 指標（SLI）                                  | 目標値                                | 測定/検証方法                                          | 環境 | Owner | 状態 |
|------|-----------------------------------|----------------------------------------------|---------------------------------------|---------------------------------------------------------|------|-------|------|
| RS-1 | BIダッシュボードの応答性          | P95 クエリ実行時間（dm系クエリ）             | ≤ 10 秒                               | STL/SLV 系ビューの定期集計、QuickSightの実測           | prod | BI/SRE| ToDo |
| RS-2 | ETLウィンドウの完了                | Glue→Redshift ジョブ成功率/完了時刻          | 成功率 ≥ 99%／定刻 +15 分以内         | Glue/Step Functions 実行ログ、CloudWatch Alarm          | prod | Data  | ToDo |
| RS-3 | 監査/ログの保全                    | Redshift ログ出力の成否                      | 100%                                  | S3到達監視＋欠損チェック、月次レポート                  | prod | SecOps| ToDo |
| RS-4 | DR の復旧性                        | スナップショット RPO/RTO                     | RPO ≤ 24h、RTO ≤ 4h                   | 手順演習（四半期）で計測、結果を記録                    | prod | SRE   | ToDo |
| RS-5 | Spectrum のコスト健全性            | 1 クエリ当たりスキャン量（P95）              | ≤ 1 GB                                | Redshift Spectrum スキャンメトリクスの週次レビュー      | prod | Data  | ToDo |
| RS-6 | クエリ健全性                       | QMR による Abort 率（暴走検知）              | 月次で改善傾向（前月比 -10% 目安）    | QMRログの可視化とレビュー                               | prod | SRE   | ToDo |

---

[前の章へ](04_storage.md)｜[目次へ](README.md)｜[次の章へ](06_security.md)
