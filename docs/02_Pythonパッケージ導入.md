# Pythonパッケージ導入手順

## 1. 前提
- 権限: sudo利用可

## 2. pipの更新
### 2.1 pipが無い場合の導入
```bash
sudo apt install -y python3-pip
```

### 2.2 pipの更新
```bash
python3 -m pip install --upgrade pip
```

## 3. パッケージ導入
### 3.1 JPX上場銘柄取得スクリプト
```bash
python3 -m pip install pandas requests pymysql xlrd
```
