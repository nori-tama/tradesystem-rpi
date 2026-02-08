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

アプリは作成せず、プロジェクト直下で管理する。

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

## 4. 起動
```bash
cd ~/tradesystem-rpi/django
python3 manage.py runserver 0.0.0.0:8000
```
