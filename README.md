# tradesystem-rpi

## ドキュメント
- [docs/00_Git初期設定.md](docs/00_Git初期設定.md)
- [docs/01_ネットワーク設定.md](docs/01_ネットワーク設定.md)
- [docs/02_Pythonパッケージ導入.md](docs/02_Pythonパッケージ導入.md)
- [docs/03_MariaDB.md](docs/03_MariaDB.md)
- [docs/04_Django.md](docs/04_Django.md)
- [docs/10_システム仕様書.md](docs/10_システム仕様書.md)

## プロジェクト規約
- データベースのDDLは `ddl/` に配置し、テーブルは事前に作成する。
- DDLは必ず `DROP TABLE IF EXISTS` の後に `CREATE TABLE` を記述する（`CREATE TABLE IF NOT EXISTS` は使用しない）。
- DDLのテーブル名/カラム名にはSQL予約語を使用しない。既存仕様上やむを得ず使用する場合は、DDL/DMLの両方でバッククォート（`` ` ``）により必ずエスケープする。
- データベースを扱うスクリプトでの `CREATE TABLE` は禁止とする。
- 新規テーブル追加などでDDLを追加/変更した場合は、`docs/03_MariaDB.md` のDDL適用手順にも同時に追記する。
- WEBシステムは `django/tradesystem_web` に作成する。
- スタイルシートは `django/tradesystem_web/css` に配置し、画面デザインを統一する。
- パッケージ導入が必要な場合は `docs/02_Pythonパッケージ導入.md` に記載する。
- ドキュメントの追加/更新/削除があった場合は、READMEの目次も更新する。
