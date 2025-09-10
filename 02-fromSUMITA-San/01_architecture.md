# 01. アーキテクチャ設計

## 1. 構成概要

本システムは、各種データソースから手動、あるいは自動でデータを取得・格納し、RedshiftおよびQuickSightで可視化・分析を行う構成となっている。  
ユーザー情報管理やログ監視はAWSサービスで完結し、運用自動化に対応。

## 2. 全体構成図

以下にシステム全体の構成図を示す：

![AWS構成図](../assets/AWS全体構成図.svg)

## 3. 採用サービス一覧と役割

| 記載章                                  | サービス名          | 用途                                   |
|------------------------------------------|---------------------|----------------------------------------|
| [02. ネットワーク設計](02_network.md)     | VPC                | ネットワーク基盤                       |
| [02. ネットワーク設計](02_network.md)     | Subnet             | パブリック／プライベート分離           |
| [02. ネットワーク設計](02_network.md)     | Security Group     | ポート・通信制御                       |
| [02. ネットワーク設計](02_network.md)     | DirectConnect      | OCIとの接続（FIC経由）                 |
| [03. コンピュート設計](03_compute.md)     | Glue               | ETLバッチ処理（OCI→S3→Redshift）       |
| [03. コンピュート設計](03_compute.md)     | Lambda             | ユーザー処理／連携バッチ               |
| [03. コンピュート設計](03_compute.md)     | EventBridge        | スケジュール制御                       |
| [04. ストレージ設計](04_storage.md)       | S3                 | 一時保存／ローデータ／ログ             |
| [05. データベース設計](05_database.md)     | Redshift           | 分析DWH                                |
| [05. データベース設計](05_database.md)     | DynamoDB           | 一時的ユーザー属性などの軽量キー・バリュー型データストア       |
| [06. セキュリティ設計](06_security.md)     | IAM                | ロール・権限設計（AzureAD連携含む）     |
| [06. セキュリティ設計](06_security.md)     | Secrets Manager    | DB接続認証情報管理                     |
| [06. セキュリティ設計](06_security.md)     | Security Group     | ポート・通信制御（再掲）               |
| [07. 運用設計](07_operations.md)          | CloudWatch         | ログ／メトリクス／アラーム             |
| [07. 運用設計](07_operations.md)          | CloudTrail         | 操作ログ監査                           |
| [07. 運用設計](07_operations.md)          | GuardDuty          | セキュリティ脅威検知                   |
| 別冊：ダッシュボード要件一覧               | QuickSight         | ダッシュボード・BI可視化               |

---

[前の章へ](00_overview.md)｜[目次へ](README.md)｜[次の章へ](02_network.md)
