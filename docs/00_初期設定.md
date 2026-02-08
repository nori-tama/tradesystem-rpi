#### Git インストール
```bash
sudo apt install -y git
```

#### README.md を作成しリポジトリ名を追記する
```bash
echo "# tradesystem-rpi" >> README.md
```

#### 現在のディレクトリを Git 管理対象として初期化する（.git ディレクトリ作成）
```bash
git init
```

#### README.md をステージングエリアに追加する（コミット対象にする）
```bash
git add README.md
```

#### グローバル設定
```bash
git config --global user.name "nori-tama"
git config --global user.email "example@gmail.com"
```

#### 最初のコミットを作成しローカル履歴に保存する
```bash
git commit -m "first commit"
```

#### 現在のブランチ名を main に変更する
```bash
git branch -M main
```

#### GitHub のリモートリポジトリを origin として登録する
```bash
git remote add origin https://github.com/nori-tama/tradesystem-rpi.git
```

#### main ブランチを GitHub に初回アップロードし、以後の push を簡略化する設定を行う
```bash
git push -u origin main
```
