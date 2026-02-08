# MySQLインストール手順（Linux）

## 1. 前提
- 権限: sudo利用可

## 2. インストール
```bash
sudo apt install -y mariadb-server
```

## 3. 起動/自動起動
```bash
sudo systemctl enable mariadb
sudo systemctl start mariadb
```

## 4. 初期設定
### 4.1 ルートユーザーのパスワード設定
```bash
sudo mysql
```

MySQL互換コンソールで実行:
```sql
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password';
FLUSH PRIVILEGES;
```

### 4.2 接続確認
```bash
mysql -u root -p
```

## 5. DB作成
```bash
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS tradesystem DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;"
```

## 6. DDL適用（本プロジェクト）
```bash
mysql -u root -p tradesystem < ddl/tse_listings.sql
```

## 7. 動作確認
```bash
mysql -u root -p tradesystem -e "SHOW TABLES;"
```

## 8. トラブルシュート
- ログ確認: `sudo journalctl -u mariadb --no-pager -n 200`
- 設定ファイル: `/etc/mysql/mariadb.conf.d/50-server.cnf`
