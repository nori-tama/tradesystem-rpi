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
- データベースを扱うスクリプトでの `CREATE TABLE` は禁止とする。
- WEBシステムは `django/tradesystem_web` に作成する。
- スタイルシートは `django/tradesystem_web/css` に配置し、画面デザインを統一する。
- パッケージ導入が必要な場合は `docs/02_Pythonパッケージ導入.md` に記載する。
- ドキュメントの追加/更新/削除があった場合は、READMEの目次も更新する。
