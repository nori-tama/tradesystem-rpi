# Django導入手順

## 1. Django導入
```bash
sudo apt install -y python3-django
```

## 2. プロジェクト作成
```bash
mkdir -p ~/tradesystem-rpi/webdjango
django-admin startproject tradesystem_web ~/tradesystem-rpi/django
```
```bash
cd ~/tradesystem-rpi/django
python3 manage.py startapp tradesystem_web
```

## 3. DB接続設定
- 設定ファイル: `tradesystem_web/settings.py`

```python
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "tradesystem",
        "USER": "root",
        "PASSWORD": "your_password",
        "HOST": "localhost",
        "PORT": "3306",
    }
}
```
