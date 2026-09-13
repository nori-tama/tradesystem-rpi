# OpenVPN インストールと VPN サーバー接続手順

対象OS: Linux（Debian / Ubuntu / Raspberry Pi OS）

## 1. OpenVPN をインストール

```bash
sudo apt install -y openvpn
```

## 2. サーバ側で `client.crt` / `client.key` を作成

前提:
- OpenVPN サーバーに `easy-rsa` が導入済み、または導入できること。
- 既存運用の `ca.crt`（認証局証明書）がすでに存在すること。

`ca.crt` とは:
- VPN クライアントが「このサーバー証明書は正当か」を検証するための認証局（CA）公開証明書。
- クライアントへ配布してよい公開情報（秘密鍵ではない）。
- 対になる `ca.key`（CA 秘密鍵）はサーバー外へ持ち出さない。

例（クライアント名: `client1`）:

```bash
cd /etc/openvpn/easy-rsa
./easyrsa gen-req client-RPI-CTRL-01 nopass
```
```bash
./easyrsa sign-req client client-RPI-CTRL-01
```
```bash
ls -l /etc/openvpn/easy-rsa/pki/*/client-RPI-CTRL-01.*
```

注意:
- 既存CAを使う場合、`init-pki` や `build-ca` で新しいCAを作らない（既存の信頼チェーンを壊さないため）。
- `~/easy-rsa` は既存PKI（`pki/ca.crt` と `pki/private/ca.key` がある場所）を使う。

生成される主なファイル:
- `~/easy-rsa/pki/issued/client1.crt`
- `~/easy-rsa/pki/private/client1.key`
- `~/easy-rsa/pki/ca.crt`

クライアントへ安全に配布するファイル:
- `client1.crt`
- `client1.key`
- `ca.crt`

補足:
- `nopass` を外すと鍵にパスフレーズを設定できる（セキュリティ重視時に推奨）。
- 失効が必要な場合は `./easyrsa revoke client1` と CRL 再生成を実施する。

## 3. 接続プロファイルを準備

VPN 管理者から受け取った `client.ovpn` を任意の場所に配置する。

例:

```bash
sudo cp /tmp/client-RPI-CTRL-01.ovpn /etc/openvpn/client/client-RPI-CTRL-01.conf

```

補足:
- `ca.crt` / `client.crt` / `client.key` などが別ファイルで渡される場合は、`client.ovpn` 内の参照パスと同じ場所に置く。
- `client1.crt` / `client1.key` を使う場合は、`client.ovpn` の `cert` / `key` 行も同じファイル名に合わせる。
- 証明書・鍵ファイルは権限を絞る。

```bash
chmod 600 ~/openvpn/*
```

## 4. 手動接続で動作確認

```bash
sudo openvpn --config /etc/openvpn/client/client-RPI-CTRL-01.ovpn
```

成功の目安:
- ログ末尾に `Initialization Sequence Completed` が表示される。

終了方法:
- `Ctrl + C`

## 5. 接続状態を確認

別ターミナルで確認する。

```bash
ip a
```

- `tun0` などの VPN インターフェースが作成されていれば接続中。

必要に応じてグローバルIP確認:

```bash
curl ifconfig.me
```

## 6. systemd で常時接続（任意）

`openvpn-client@<name>` サービスを使うため、設定名を `client.conf` として配置する。

```bash
sudo cp ~/openvpn/client-RPI-CTRL-01.ovpn /etc/openvpn/client/client-RPI-CTRL-01.conf
sudo systemctl enable --now openvpn-client@client-RPI-CTRL-01
```

状態確認:

```bash
sudo systemctl status openvpn-client@client
journalctl -u openvpn-client@client -e
```

停止 / 再開:

```bash
sudo systemctl stop openvpn-client@client
sudo systemctl start openvpn-client@client
```

## 7. よくあるトラブル

- 証明書や鍵ファイルのパス不一致（`client.ovpn` の `ca` / `cert` / `key` 設定を確認）
- サーバー側ポート閉塞（例: UDP 1194）
- クライアントの時刻ずれによる証明書検証失敗

時刻確認:

```bash
timedatectl status
```
