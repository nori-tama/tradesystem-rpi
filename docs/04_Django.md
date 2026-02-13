# Django導入手順

## 1. Django導入
```bash
sudo apt install -y python3-django
```

## 2. プロジェクト作成
```bash
mkdir -p ~/tradesystem-rpi/django
django-admin startproject tradesystem_web ~/tradesystem-rpi/django
```

## 3. 起動
```bash
cd ~/tradesystem-rpi/django
python3 manage.py runserver 192.168.0.10:8000
```

## 4. OSサービス化（systemd）

### 4.1 サービス定義ファイル作成
```bash
sudo tee /etc/systemd/system/tradesystem-django.service > /dev/null <<'EOF'
[Unit]
Description=TradeSystem Django Server
After=network.target

[Service]
Type=simple
User=pi
Group=pi
WorkingDirectory=/home/pi/tradesystem-rpi/django
ExecStart=/usr/bin/python3 manage.py runserver 0.0.0.0:8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
```

### 4.2 サービス有効化・起動
```bash
sudo systemctl daemon-reload
sudo systemctl enable tradesystem-django.service
sudo systemctl start tradesystem-django.service
```

### 4.3 状態確認
```bash
sudo systemctl status tradesystem-django.service
sudo journalctl -u tradesystem-django.service -f
```

### 4.4 停止・無効化
```bash
sudo systemctl stop tradesystem-django.service
sudo systemctl disable tradesystem-django.service
```

