# Raspberry Pi 上での Apache Airflow 導入手順

このドキュメントは、Raspberry Pi（ARM）上に Apache Airflow を pip ベースで導入する手順をまとめたもの。軽量化のための注意点や systemd による自動起動例、トラブルシューティングも含む。

**前提 / 推奨（MySQL を利用する前提）**
- 本手順は PostgreSQL を使用しない、MySQL 専用の手順です（Airflow 公式は MariaDB 非対応）。
- OS: Raspberry Pi OS（64-bit 推奨）または Debian 系（最新のセキュリティパッチ適用）
- Python: `python3`（本手順は 3.13 系を想定。Airflow 3.1.7 を使用）
- メモリ: 最低 1GB（実運用では 2GB 以上推奨）。Swap の設定を検討。
- DB: 本手順は `MySQL` を想定しています。開発/検証は SQLite でも可ですが、運用では MySQL を利用してください（SQLite は Scheduler の競合で非推奨）。

---

## 全体の流れ（短縮）
1. システム更新と必須パッケージ導入
2. Python パッケージの準備（apt 管理）
3. Airflow 本体を constraints 指定でインストール
4. `AIRFLOW_HOME` 設定、DB 接続設定（MySQL）、DB 初期化
5. 管理ユーザー作成
6. Web サーバー / Scheduler 起動（systemd での自動起動推奨）

---

## 手順（詳細・コマンド例）

1) システム更新と依存パッケージ

```bash
# MySQL 用の開発パッケージを含めた最小依存パッケージ
sudo apt install -y build-essential libssl-dev libffi-dev default-libmysqlclient-dev libxml2-dev libxslt1-dev python3-dev git curl
# cryptography のビルドに rust が必要な場合:
sudo apt install -y cargo
```

2) 専用ユーザーと `AIRFLOW_HOME` の作成

以下は systemd で `User=airflow` として動作させる前提の手順です。既に `/home/airflow` が存在して所有者が `pi` など別ユーザーになっている場合は所有権を修正してください。

#### 1) 専用の system ユーザーを作成（既に存在する場合はスキップ）
```bash
sudo adduser --system --group --home /home/airflow --shell /bin/false airflow
```

#### 2) Airflow 用ディレクトリを作成し、所有者を airflow にする
```bash
sudo mkdir -p /home/airflow/airflow
sudo chown -R airflow:airflow /home/airflow
```

#### 3) 所有者/ユーザーの確認
```bash
id airflow
ls -ld /home/airflow /home/airflow/airflow
```

#### 4) 環境変数（シェルで一時適用する場合）
```bash
export AIRFLOW_HOME=/home/airflow/airflow
```

注: 既に `pi` ユーザーでディレクトリを作成してしまった場合は `sudo chown -R airflow:airflow /home/airflow` で修正してください。systemd ユニットは `User=airflow` を想定していますので、所有者が一致しないと起動時にファイルアクセスエラーが発生します。

3) Python パッケージの準備（apt 管理）

プロジェクト規約により、Python の仮想環境（`venv`/`virtualenv`）は使用しません。
PEP 668 により、Debian 系ではシステム Python への `sudo pip` が制限されます。システム全体へ追加する Python ライブラリは `python3-<package>` 形式で導入してください。
本手順内の追加ライブラリ導入は、すべて `python3-<package>` 形式で実施します。

# Python 実行環境と関連ツールを apt で導入
```bash
sudo apt install -y python3-pip python3-setuptools python3-wheel python3-full
```

# 追加パッケージは python3-<package> 形式で導入
```bash
sudo apt install -y python3-numpy python3-pandas
```

# 必要なパッケージ名を探す例
```bash
apt-cache search '^python3-' | grep -i <keyword>
```

4) Airflow 本体インストール（constraints を必ず使用）

Airflow は依存関係が多く、必ずリリースに対応した constraints ファイルを使ってください。

```bash
AIRFLOW_VERSION=3.1.7
PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

```bash
# constraints URL の事前確認（404 を先に検知）
curl -fSL "${CONSTRAINT_URL}" >/dev/null
```

# Airflow 本体をシステム全体にインストール
# 注: Airflow 本体は apt の `python3-<package>` で提供されない場合があるため、ここは例外として pip を使用
#     (プロジェクト規約により仮想環境を使わないため、PEP 668 回避として --break-system-packages を付与)
```bash
sudo python3 -m pip install --break-system-packages --ignore-installed "apache-airflow[mysql]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

# MySQL 用の Python ドライバは apt の python3-<package> 形式で導入
```bash
sudo apt install -y default-libmysqlclient-dev build-essential python3-mysqldb
```

注: `AIRFLOW_VERSION` と `PYTHON_VERSION` は使う環境に合わせて変更してください。
注: Python 3.13 系で実行する場合、constraints URL は `constraints-3.13.txt` が存在する Airflow バージョンを指定してください（本手順の `3.1.7` は対応）。

```bash
# 導入確認（PATH 非依存）
sudo python3 -m airflow version
```

注: `Attempting uninstall ... error: uninstall-no-record-file` が出る環境では、`--ignore-installed` を外さないでください。

---

4a) MySQL の前提と Airflow 用 DB 作成例

本手順は、MySQL が既にインストールされ、`mysql_secure_installation` 等による基本的な初期セキュリティ設定（root パスワード設定、不要な匿名ユーザー削除、リモート root ログイン無効化など）が実施済みであることを前提とします。

以下は Airflow 用のデータベースとユーザーを作成する最小の例です。ローカル DB を利用する場合は `localhost` に、外部 DB を利用する場合はホスト名を変更してください。

```bash
sudo mysql -u root -p <<'SQL'
DROP DATABASE IF EXISTS airflow_db;
DROP USER IF EXISTS 'airflow'@'localhost';
CREATE DATABASE airflow_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'airflow'@'localhost' IDENTIFIED BY 'airflow';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow'@'localhost';
FLUSH PRIVILEGES;
SQL
```

注: 既存の `airflow_db` 内データは `DROP DATABASE` により削除されます。必要に応じて事前にバックアップしてください。

5) Airflow の DB 接続設定（MySQL）

Airflow は SQLAlchemy の接続文字列で DB 接続を指定します。環境変数で設定する例を示します。

```bash
export AIRFLOW__CORE__SQL_ALCHEMY_CONN='mysql://airflow:airflow@localhost:3306/airflow_db?charset=utf8mb4'

# 環境変数を永続化したい場合は ~/.profile や systemd ユニット内で指定します。

# DB 初期化/マイグレーション（Airflow 3 系）
sudo python3 -m airflow db migrate
```

注: パスワードやホストは運用環境に合わせてください。外部 DB を使う場合は `localhost` を外部ホスト名に置き換えます。

6) 初期設定・DB 初期化・管理ユーザー設定

```bash
export AIRFLOW_HOME=/home/airflow/airflow

# DB 初期化（MySQL を使用する場合は環境変数で接続先を指定済みの前提）
sudo python3 -m airflow db migrate

# Airflow 3 の既定（SimpleAuthManager）で管理ユーザーを定義
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS='admin:admin'
```

注: SimpleAuthManager では `airflow users create` は使えません。初回起動時にパスワードが自動生成され、API サーバーのログと
`$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated` に保存されます。

注: `sudo python3 -m airflow config get-value core auth_manager` で現在の auth manager を確認できます。

Python 3.13 環境では FAB auth manager を使わず、SimpleAuthManager 前提で運用してください。

固定パスワードにしたい場合（Python 3.13 のまま実施可）:

```bash
# 1) sudo に環境変数を引き継いで、auth manager を確認
export AIRFLOW_HOME=/home/airflow/airflow
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS='admin:admin'
sudo -E python3 -m airflow config get-value core auth_manager

# 2) 先に airflow を起動して password ファイルを生成
sudo -E python3 -m airflow api-server -p 8080

# 3) 別ターミナルで固定パスワードへ更新（生成先を自動探索）
sudo -E python3 - <<'PY'
import json
import os
from pathlib import Path

paths = []
home = os.environ.get('AIRFLOW_HOME')
if home:
	paths.append(Path(home) / 'simple_auth_manager_passwords.json.generated')
paths.extend([
	Path('/home/airflow/airflow/simple_auth_manager_passwords.json.generated'),
	Path('/root/airflow/simple_auth_manager_passwords.json.generated'),
])

target = next((p for p in paths if p.exists()), None)
if target is None:
	raise SystemExit(
		'password file not found. Keep api-server running once, then retry. '
		'Also check AIRFLOW_HOME/auth_manager settings.'
	)

raw = target.read_text(encoding='utf-8').strip()
data = json.loads(raw) if raw else {}
data['admin'] = 'admin'
target.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
print(f'updated: {target}')
PY

# 4) api-server を再起動して反映
sudo pkill -f "airflow api-server" || true
sudo -E python3 -m airflow api-server -p 8080
```

注: 上記の `admin` は `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS='admin:admin'` で定義したユーザー名です。
`AIRFLOW_HOME` を変更している場合は JSON ファイルのパスも変更してください。

一時的なローカル検証だけで認証を無効化する場合:

```bash
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS='True'
```

注: `SIMPLE_AUTH_MANAGER_ALL_ADMINS=True` は全員 admin 扱いになるため、本番運用では使用しないでください。

7) 起動（開発・動作確認用）

```bash
# 別ターミナルでそれぞれ実行
sudo python3 -m airflow scheduler &
sudo python3 -m airflow api-server -p 8080 &
# ブラウザで http://<RaspberryPiのIP>:8080 にアクセス
```

---

## systemd による自動起動例
以下はユーザー単位（`systemctl --user`）の例です。`youruser` を利用中のユーザー名に置き換えてください。

`~/.config/systemd/user/airflow-api-server.service`:

```
[Unit]
Description=Airflow API server

[Service]
Type=simple
Environment=AIRFLOW_HOME=/home/youruser/airflow
Environment=AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin
ExecStart=/usr/bin/python3 -m airflow api-server -p 8080
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
Environment=AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin
ExecStart=/usr/bin/python3 -m airflow scheduler
Restart=always
RestartSec=5s

[Install]
WantedBy=default.target
```

有効化と起動:

```bash
systemctl --user daemon-reload
systemctl --user enable --now airflow-api-server
systemctl --user enable --now airflow-scheduler
```

注意: システムサービスとして `/etc/systemd/system/` に配置する場合は環境変数やパス、ユーザー権限に注意してください。systemd ユニット内で `Environment=` に `AIRFLOW__CORE__SQL_ALCHEMY_CONN=` を設定しておくと確実です。

---

## Docker / コンテナ運用（代替案）
- 公式の Airflow Docker イメージは多くが amd64 前提です。Raspberry Pi（ARM）で Docker を使う場合は ARM 向けイメージを自分でビルドするか、マルチアーキ（`docker buildx`）でビルドしてください。
- 小型運用であれば、`LocalExecutor` にして1台で完結させるのが簡単です。

---

## よくあるトラブルと対処
- `airflow: command not found`: `airflow` コマンドを直接呼ばず、`sudo python3 -m airflow version` / `sudo python3 -m airflow db migrate` で実行する。
- `airflow command error: invalid choice: 'users'`: Airflow 3 の既定（SimpleAuthManager）では `users` CLI は使用不可。`AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` を設定し、必要なら `simple_auth_manager_passwords.json.generated` を直接更新してパスワードを固定する。
- `FileNotFoundError: ... simple_auth_manager_passwords.json.generated`: まだファイル未生成か、`sudo` 実行時に `AIRFLOW_HOME` が変わって別パスを見ている。`sudo -E` で `AIRFLOW_HOME` を引き継ぎ、`api-server` を一度起動してから再実行する。
- `No module named 'connexion'` / `No module named 'cachetools'`: FAB auth manager 依存の読み込み失敗。Python 3.13 運用では FAB を使わず、SimpleAuthManager を使用する。
- `uninstall-no-record-file`: apt 管理パッケージと pip が衝突しているため、Airflow 導入コマンドに `--ignore-installed` を付ける。
- ビルド失敗 (`cryptography` 等): `rustc`/`cargo` が必要になることがあります。`sudo apt install cargo` を試す。
- OpenSSL 関連エラー: `libssl-dev` のインストールを確認する。
- メモリ不足でプロセスが落ちる: Swap を一時的に増やす、不要プロセスを停止する。
- 依存解決エラー: Airflow と Python の組み合わせが constraints に対応しているか確認する。
- SQLite を本番で使用すると Scheduler の競合やロック問題が発生するため、運用では `Postgres` または `MySQL` に切り替える。

---

## systemd による自動起動例（システムサービス）
以下はシステム全体で管理する `/etc/systemd/system/` に配置するサービスユニットの例です。専用のシステムユーザー `airflow` を作成してそのユーザーで動かすことを想定しています。

`/etc/systemd/system/airflow-api-server.service`:

```
[Unit]
Description=Airflow API server
After=network.target

[Service]
Type=simple
User=airflow
Group=airflow
Environment=AIRFLOW_HOME=/home/airflow/airflow
Environment=AIRFLOW__CORE__SQL_ALCHEMY_CONN=mysql://airflow:cairflow@localhost:3306/airflow_db?charset=utf8mb4
Environment=AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin
ExecStart=/usr/bin/python3 -m airflow api-server -p 8080
Restart=always
RestartSec=5s
WorkingDirectory=/home/airflow/airflow

[Install]
WantedBy=multi-user.target
```

`/etc/systemd/system/airflow-scheduler.service`:

```
[Unit]
Description=Airflow scheduler
After=network.target

[Service]
Type=simple
User=airflow
Group=airflow
Environment=AIRFLOW_HOME=/home/airflow/airflow
Environment=AIRFLOW__CORE__SQL_ALCHEMY_CONN=mysql://airflow:change_me_password@localhost:3306/airflow_db?charset=utf8mb4
Environment=AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin
ExecStart=/usr/bin/python3 -m airflow scheduler
Restart=always
RestartSec=5s
WorkingDirectory=/home/airflow/airflow

[Install]
WantedBy=multi-user.target
```

有効化と起動:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now airflow-api-server.service
sudo systemctl enable --now airflow-scheduler.service
```

注意: `Environment=` の接続文字列は運用環境に合わせて必ず変更してください。パスワードを平文でユニットに書くのが不可な場合は、`/etc/environment` や systemd の `EnvironmentFile=` を使って管理してください。
