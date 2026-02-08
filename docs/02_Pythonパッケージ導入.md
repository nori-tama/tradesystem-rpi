# Pythonパッケージ導入手順

## 1. 前提
- 権限: sudo利用可

## 2. パッケージ導入（apt）
外部管理環境（PEP 668）のため、システムパッケージで導入する。
```bash
sudo apt install -y python3-pandas python3-requests python3-pymysql python3-xlrd
```

## 3. 動作確認
```bash
python3 - <<'PY'
import pandas, requests, pymysql, xlrd
print("ok")
PY
```
