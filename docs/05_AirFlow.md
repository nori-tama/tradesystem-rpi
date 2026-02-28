# Raspberry Pi 上での Apache Airflow 導入手順

このドキュメントは、Raspberry Pi（ARM）上に Apache Airflow を pip ベースで導入する手順をまとめたもの。軽量化のための注意点や systemd による自動起動例、トラブルシューティングも含む。

**前提 / 推奨（MySQL を利用する前提）**
- OS: Raspberry Pi OS（64-bit 推奨）または Debian 系（最新のセキュリティパッチ適用）
- Python: `python3`（推奨 3.10 / 3.11。Airflow のバージョンに依存）
- メモリ: 最低 1GB（実運用では 2GB 以上推奨）。Swap の設定を検討。
- DB: 本手順は `MySQL` / `MariaDB` を想定しています。開発/検証は SQLite でも可ですが、運用では MySQL を利用してください（SQLite は Scheduler の競合で非推奨）。

---

## 全体の流れ（短縮）
1. システム更新と必須パッケージ導入
2. Python 仮想環境作成（`venv`）
3. Airflow を constraints 指定で pip インストール
4. `AIRFLOW_HOME` 設定、DB 初期化、管理ユーザー作成
5. Web サーバー / Scheduler 起動（systemd での自動起動推奨）

---

## 手順（詳細・コマンド例）

1) システム更新と依存パッケージ

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y build-essential libssl-dev libffi-dev libpq-dev libxml2-dev libxslt1-dev python3-dev python3-venv git curl
# cryptography のビルドに rust が必要な場合:
sudo apt install -y cargo
```

2) 専用ユーザーと `AIRFLOW_HOME`（任意）

```bash
# 任意で専用ユーザーを作成する例（システムユーザー）
sudo adduser --system --group --home /home/airflow airflow
sudo mkdir -p /home/airflow/airflow
sudo chown -R $(whoami):$(whoami) /home/airflow/airflow

# シェルに環境変数を設定
export AIRFLOW_HOME=/home/$(whoami)/airflow
mkdir -p "$AIRFLOW_HOME"
```

3) 仮想環境作成と pip アップグレード

```bash
python3 -m venv ~/airflow-venv
source ~/airflow-venv/bin/activate
pip install --upgrade pip setuptools wheel
```

4) Airflow インストール（constraints を必ず使用）

Airflow は依存関係が多く、必ずリリースに対応した constraints ファイルを使ってください。

```bash
AIRFLOW_VERSION=2.8.3
PYTHON_VERSION=3.10
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# MySQL を使用するための extra を指定してインストール
pip install "apache-airflow[mysql]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# MySQL 用の Python ドライバ（mysqlclient）を利用する場合、事前にシステム依存パッケージを入れておく
sudo apt install -y default-libmysqlclient-dev build-essential
pip install mysqlclient
```

注: `AIRFLOW_VERSION` と `PYTHON_VERSION` は使う環境に合わせて変更してください。

---

4a) MySQL / MariaDB サーバのセットアップ（ローカルで DB を用意する場合）

以下は Raspberry Pi 上で `MariaDB` を使う例です（MySQL でもほぼ同様）。外部 DB を使う場合は該当サーバに合わせてください。

```bash
# MariaDB サーバ（または mysql-server）をインストール
sudo apt install -y mariadb-server

# セキュリティ設定（推奨）
sudo mysql_secure_installation

# MySQL に接続して Airflow 用 DB とユーザーを作成
sudo mysql -u root -p <<'SQL'
CREATE DATABASE airflow_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'airflow'@'localhost' IDENTIFIED BY 'change_me_password';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow'@'localhost';
FLUSH PRIVILEGES;
SQL
```

5) Airflow の DB 接続設定（MySQL）

Airflow は SQLAlchemy の接続文字列で DB 接続を指定します。環境変数で設定する例を示します。

```bash
export AIRFLOW__CORE__SQL_ALCHEMY_CONN='mysql://airflow:change_me_password@localhost:3306/airflow_db?charset=utf8mb4'

# 環境変数を永続化したい場合は ~/.profile や systemd ユニット内で指定します。

# DB 初期化
airflow db init
```

注: パスワードやホストは運用環境に合わせてください。外部 DB を使う場合は `localhost` を外部ホスト名に置き換えます。

5) 初期設定・DB 初期化・管理ユーザー作成

```bash
export AIRFLOW_HOME=/home/$(whoami)/airflow

# DB 初期化（SQLite/本番 DB に応じて設定ファイルで接続先を変更）
airflow db init

# 管理ユーザー作成（例）
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

6) 起動（開発・動作確認用）

```bash
# 別ターミナルでそれぞれ実行
airflow scheduler &
airflow webserver -p 8080 &
# ブラウザで http://<RaspberryPiのIP>:8080 にアクセス
```

---

## systemd による自動起動例
以下はユーザー単位（`systemctl --user`）の例です。`youruser` を利用中のユーザー名に置き換えてください。

`~/.config/systemd/user/airflow-webserver.service`:

```
[Unit]
Description=Airflow webserver

[Service]
Type=simple
Environment=AIRFLOW_HOME=/home/youruser/airflow
ExecStart=/home/youruser/airflow-venv/bin/airflow webserver -p 8080
Restart=always
RestartSec=5s

[Install]
WantedBy=default.target
```

`~/.config/systemd/user/airflow-scheduler.service`:

```
[Unit]
Description=Airflow scheduler

[Service]
Type=simple
Environment=AIRFLOW_HOME=/home/youruser/airflow
ExecStart=/home/youruser/airflow-venv/bin/airflow scheduler
Restart=always
RestartSec=5s

[Install]
WantedBy=default.target
```

有効化と起動:

```bash
systemctl --user daemon-reload
systemctl --user enable --now airflow-webserver
systemctl --user enable --now airflow-scheduler
```

注意: システムサービスとして `/etc/systemd/system/` に配置する場合は環境変数やパス、ユーザー権限に注意してください。systemd ユニット内で `Environment=` に `AIRFLOW__CORE__SQL_ALCHEMY_CONN=` を設定しておくと確実です。

---

## Docker / コンテナ運用（代替案）
- 公式の Airflow Docker イメージは多くが amd64 前提です。Raspberry Pi（ARM）で Docker を使う場合は ARM 向けイメージを自分でビルドするか、マルチアーキ（`docker buildx`）でビルドしてください。
- 小型運用であれば、`LocalExecutor` にして1台で完結させるのが簡単です。

---

## よくあるトラブルと対処
- ビルド失敗 (`cryptography` 等): `rustc`/`cargo` が必要になることがあります。`sudo apt install cargo` を試す。
- OpenSSL 関連エラー: `libssl-dev` のインストールを確認する。
- メモリ不足でプロセスが落ちる: Swap を一時的に増やす、不要プロセスを停止する。
- 依存解決エラー: Airflow と Python の組み合わせが constraints に対応しているか確認する。
- SQLite を本番で使用すると Scheduler の競合やロック問題が発生するため、運用では `Postgres` または `MySQL` に切り替える。

---

## 参考 / 次の作業候補
- systemd ユニットのテンプレートを `youruser` の実名に合わせて生成しますか？
- ARM 向け Docker イメージのビルド手順を作成しますか？

---

ファイル: docs/05_AirFlow.md
