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
python3 manage.py runserver 0.0.0.0:8000
```
