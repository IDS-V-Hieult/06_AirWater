# 06. セキュリティ設計

## 1. 概要

本システムは、AWSが提供するセキュリティ機能を活用し、**データ保護**・**アクセス制御**・**不正操作検知**・**準拠監査**を継続的に実施する。主な対策は以下のとおり。

- IAMロールによる**最小権限**（信頼ポリシーで発行元を制限）
- Secrets Manager による認証情報の保護（**ランタイム取得**・ローテーション）
- KMS による暗号化（**CMK 原則・年1回ローテーション**）
- CloudTrail（全リージョン）・GuardDuty による監査と脅威検知
- **Security Hub（CIS/FSBP）** と **AWS Config** による準拠監視
- **S3 Block Public Access**（アカウント/バケット両方）
- **Access Analyzer** による外部公開検出

---

## 2. IAM 設計

### 2.1 IAM ロール構成（代表）

| ロール名                          | 使用サービス           | 権限概要                                             |
|----------------------------------|------------------------|-----------------------------------------------------|
| `Redshift-Glue-access-role`      | Glue                   | S3 入出力、Secrets 読取、Redshift 操作                |
| `Redshift-qs-access-role`        | QuickSight             | Redshift 参照（読み取り専用）                         |
| `Redshift-spectrum-access-role`  | Redshift               | S3 への COPY/UNLOAD（必要バケット/プレフィックス限定） |
| `aw-ad` ※名称は実環境準拠         | IAM Identity Center等  | 組織SSO連携（Azure AD）                              |

> **運用方針**  
> ・長期アクセスキーは原則禁止。キー最終使用が 90日超は自動失効。  
> ・権限は Permissions Boundary で上限拘束。
> ・四半期ごとにロール/ポリシーの棚卸しを実施。

### 2.2 IAM ポリシー方針

- S3/Secrets は **必要なバケット/プレフィックスのみに限定**（`aws:ResourceTag` 等で絞り込み）
- サービスロールは **信頼ポリシー** で起動元（Lambda/Glue/QuickSight 等）に限定
- **Access Analyzer** により外部主体へのアクセスを継続検出・是正
- Redshift は **AWS API 権限**（例：Data API, Describe*）と **DB 権限（GRANT）** を分離し、DB はスキーマ/ロール単位で最小化

### 2.3 オーケストレーション/ETL実行のセキュリティ

- **API Gateway**：Private 連携、リソースポリシーで`vpce-*`（インターフェースエンドポイント）または組織ID/固定CIDRに限定。アクセスログは JSON で CloudWatch Logsへ。
- **Step Functions**：実行ログ（CloudWatch Logs）を **暗号化（KMS）**、**X-Ray** 有効化。
- **Glue ジョブ**：**Security Configuration** を適用（CloudWatch/S3 出力暗号化、ジョブ一時領域の暗号化）。**VPC 内実行**、SG/NACL は必要最小限。ジョブロールは S3/KMS/Secrets/Redshift の**最小権限**のみ許可。

---

## 3. Secrets Manager（認証情報の保護）

- DB 接続情報（OCI/Redshift）を Secrets に保管し、**Lambda/Glue からランタイム取得**（環境変数へ平文展開しない）
- **CMK（KMS）で暗号化**、ローテーション（90日）を有効化
- **リソースポリシー** で参照元ロールを限定（クロスアカウント参照は明示許可）

| 名前                             | 用途                         |
|----------------------------------|------------------------------|
| `prod-bi_awi-redshift-secrets`   | Redshift 管理者接続情報      |
| `prod-bi_awi-oci-secrets`        | Glue JDBC 接続（本番）       |
| `stg-bi_awi-oci-secrets`         | Glue JDBC 接続（STG）        |

---

## 4. 暗号化（KMS）

| 対象             | 暗号化方式             | ポイント                                                                 |
|------------------|------------------------|--------------------------------------------------------------------------|
| S3 バケット      | **SSE-KMS（CMK）**     | ログ監査系のバケットはポリシーで `aws:SecureTransport` 必須、**未暗号化 PUT を拒否**    |
| Secrets Manager  | KMS（CMK）             | キーポリシーは最小化、年1ローテーション                                 |
| Redshift         | **クラスター暗号化**   | `require_ssl=1`、Enhanced VPC Routing、監査ログを S3（KMS）へ            |

---

## 5. ログ・操作監査（CloudTrail/監査保持）

- **組織/全リージョンの証跡**＋新規リージョン自動追随
- 監査ログは S3（バージョン管理・**Object Lock（WORM）**・KMS）へ保管
- **Log File Validation** は日次検証／合格率 100% を維持
- 保持方針：ホット 365 日 → Glacier Deep Archive **3 年**保持

| ログ対象        | 出力先 / 通知                              |
|-----------------|------------------------------------------|
| 管理者操作      | CloudWatch Alarms → SNS（高リスク API 即時）|
| Lambda 実行     | CloudWatch Logs                           |
| Glue 実行       | CloudWatch Logs                           |
| VPC Flow Logs   | CloudWatch Logs                           |

---

## 6. GuardDuty（脅威検知）

- **全リージョン有効**（新規リージョン自動有効化）
- **S3/EKS/RDS/Malware Protection** を有効化
- 検知（Severity ≥ 5.0）は **当番へ自動エスカレーション**／抑制ルールを整備

---

## 7. VPC 関連セキュリティ

- S3 へのアクセスは **VPC Endpoint（Gateway）** 経由（必要に応じて **S3 Access Point／VPC 限定**）
- Public サブネットは作成しない。  
  運用アクセスは **SSM Session Manager** を使用（踏み台サーバーは原則不使用）
- Secrets/STS/KMS などは **インターフェース型 VPC エンドポイント** を利用し、外向き通信を最小化

---

## 8. Redshift / QuickSight の追加ガード

- Redshift：監査ログ（ユーザー/クエリ/接続）を **S3（KMS）** へ出力、COPY/UNLOAD は `ENCRYPTED KMS_KEY_ID` を明示  
- QuickSight：ダッシュボード公開範囲の厳格化、**RLS** 適用、SPICE 暗号化、AzureAD経由でのみログイン許可

---

## 9. セキュリティ SLO（指標/目標/測定/環境/責任）

| ID  | SLO（要件）                  | 指標（SLI）                                      | 目標値                        | 測定/検証方法                                         |
|-----|------------------------------|-------------------------------------------------|------------------------------|-------------------------------------------------------|
| S-1 | 重大検知の一次対応            | GuardDuty Severity ≥ 5.0 の MTTA/MTTR            | MTTA ≤ 10 分、MTTR ≤ 2 時間  | CloudWatch 連携/SNS、オンコール記録の月次レビュー         |
| S-2 | 監査ログの完全性              | CloudTrail Log File Validation 成功率            | 100%                         | 日次ジョブで検証、失敗時アラート                          |
| S-3 | S3 の公開防止                | 外部公開バケット件数                              | 0                             | Security Hub/Access Analyzer レポート                  |
| S-4 | アカウントの強固化            | コンソール MFA 有効率／長期アクセスキー 90 日超件数 | MFA 100%／該当 0 件           | IAM Credentials Report、月次棚卸し                      |
| S-5 | 監査ログの長期保全            | 監査ログ保持期間                                  | 3年                         | S3 ライフサイクル/オブジェクトロック設定の年次レビュー      |

※運用設計に移す可能性あり。

---

[前の章へ](05_database.md)｜[目次へ](README.md)｜[次の章へ](07_operations.md)
