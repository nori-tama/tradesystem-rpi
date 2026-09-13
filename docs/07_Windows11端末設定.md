# Windows 11端末設定手順

## 1. 目的と使用するスクリプト

`docs/スクリプト` の次の2本を順番に実行し、Windows 11からRaspberry PiのプロジェクトをVS Code Remote - SSHで開く環境を構築する。

| 順序 | スクリプト | 処理内容 |
| --- | --- | --- |
| 1 | [Install_VSCode_Codex_Japanese_OpenSSH.ps1](スクリプト/Install_VSCode_Codex_Japanese_OpenSSH.ps1) | VS Codeのクリーン再構築、日本語拡張・Codex拡張の導入、日本語表示設定、OpenSSH Clientの導入、SSH鍵の確認・作成 |
| 2 | [Setup_RaspberryPi_VSCode_RemoteSSH.ps1](スクリプト/Setup_RaspberryPi_VSCode_RemoteSSH.ps1) | Remote - SSH拡張の導入、SSH接続設定、公開鍵登録、公開鍵認証テスト、リモートフォルダーを開く |

スクリプト内の説明やエラーには `_CLEAN`、`_FIXED2` 付きの旧ファイル名が残っているが、実行するファイル名は上表のとおり。

## 2. 実行前の準備

### Windows側

- Windows Updateと必要な再起動を完了する。
- インターネットとRaspberry PiのLANに接続する。
- Windows PowerShell 5.1以上を使用する。1本目はAMD64またはARM64のプロセスを前提としているため、32ビット版PowerShellは使用しない。
- **VS Codeで編集中のファイルを保存し、終了する。** 1本目はVS Codeを強制終了し、既存設定・拡張機能をバックアップ後に削除して再インストールする。
- 普段VS Codeを利用するWindowsユーザーで実行する。管理者として起動する際も同じユーザーを使用する。別の管理者アカウントで実行すると、そのアカウントのユーザーフォルダーに設定・鍵が作成される。

1本目のバックアップ先は `C:\TEMP\vscode-clean-setup\backup_yyyyMMdd_HHmmss`。
既存の `%APPDATA%\Code`、`%LOCALAPPDATA%\Code`、`%USERPROFILE%\.vscode` が存在すればコピーされる。復旧時に参照できるよう、セットアップ完了後も保存しておく。

既存の `.ssh` は削除されない。`id_ed25519` と公開鍵があれば再利用し、秘密鍵だけがある場合は公開鍵を復元する。両方ない場合は**パスフレーズなし**の鍵を作成する。秘密鍵は共有・Git登録しない。

### Raspberry Pi側

| 項目 | 本手順の設定値 |
| --- | --- |
| ホスト名 | `RPI-CTRL-01` |
| IPアドレス | `192.168.0.10` |
| SSHユーザー | `pi` |
| Windows側のSSH接続名 | `raspberrypi-trade` |
| プロジェクト | `/home/pi/tradesystem-rpi` |

接続先は [ネットワーク設定](01_ネットワーク設定.md) に合わせた例。異なる場合は実行引数を変更する。
SSHサービスが稼働し、初回の公開鍵登録に使うパスワード認証、または既存のSSH認証でログインできることを確認する。

SSHサーバーが未導入の場合は、Raspberry Piのコンソールで実行する。

```bash
sudo apt install -y openssh-server
sudo systemctl enable --now ssh
```

プロジェクトがまだない場合は、Raspberry Pi側で取得する。既存フォルダーがある場合はcloneを省略する。

```bash
sudo apt install -y git
cd /home/pi
git clone https://github.com/nori-tama/tradesystem-rpi.git
```

非公開リポジトリの取得にはGitHub認証が必要。Gitの設定は [Git初期設定](00_Git初期設定.md) を参照する。

## 3. スクリプトをWindows端末へ配置する

リポジトリから上記2本をダウンロードまたはコピーし、Windowsの次の場所に保存する。GitがWindowsに入っていない場合も、ブラウザーでリポジトリのZIPを取得して展開できる。

```text
C:\TEMP\tradesystem-setup\
  Install_VSCode_Codex_Japanese_OpenSSH.ps1
  Setup_RaspberryPi_VSCode_RemoteSSH.ps1
```

WebページのHTMLではなく `.ps1` ファイル本体を保存する。日本語を含むため、元の文字コード（UTF-8 BOM付き）を維持する。
`/home/pi/tradesystem-rpi/docs/スクリプト` はRaspberry Pi側のパスであり、以下はWindowsへコピーしたファイルを実行する。

## 4. VS Code・日本語表示・OpenSSHを構築する

スタートメニューから **Windows PowerShellを管理者として実行** する。VS Code内のターミナルは使用しない。

```powershell
Set-Location "C:\TEMP\tradesystem-setup"
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
Unblock-File -LiteralPath ".\Install_VSCode_Codex_Japanese_OpenSSH.ps1"
& ".\Install_VSCode_Codex_Japanese_OpenSSH.ps1"
```

実行ポリシーの変更はこのPowerShellプロセスだけに適用する。組織のポリシーで禁止されている場合は管理者に確認する。

スクリプトはVS Codeを公式配布先から取得し、14段階の処理後に「セットアップ完了」を表示してVS Codeを通常起動する。日本語表示を確認する。
WinGetとMicrosoft Storeは使用しない。Windows側のGitはこのスクリプトでは導入されない。

完了後は管理者PowerShellを閉じる。OpenSSHの導入で再起動が必要な場合は、次の手順の前に再起動する。

**接続設定だけをやり直す場合、1本目を再実行する必要はない。** 再実行するとVS Codeの設定・拡張機能が再度初期化される。

## 5. Raspberry PiへのRemote - SSH接続を設定する

同じWindowsユーザーで、通常権限のWindows PowerShellを新しく開く。

```powershell
Test-NetConnection 192.168.0.10 -Port 22
```

`TcpTestSucceeded : True` を確認してから実行する。

```powershell
Set-Location "C:\TEMP\tradesystem-setup"
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
Unblock-File -LiteralPath ".\Setup_RaspberryPi_VSCode_RemoteSSH.ps1"
& ".\Setup_RaspberryPi_VSCode_RemoteSSH.ps1" `
    -PiHost "192.168.0.10" `
    -PiUser "pi" `
    -HostAlias "raspberrypi-trade" `
    -RemoteProject "/home/pi/tradesystem-rpi"
```

PowerShellの行末のバッククォートの後ろに空白を入れない。

| 引数 | 指定内容 |
| --- | --- |
| `-PiHost` | 接続先IPアドレスまたはホスト名（必須） |
| `-PiUser` | SSHログインユーザー（必須） |
| `-HostAlias` | Windows側の接続名。省略時は `raspberrypi-trade` |
| `-RemoteProject` | Raspberry Pi上の既存プロジェクトの絶対パス（必須）。通常のパスを指定し、シングルクォートを含めない |
| `-NoOpenVSCode` | 指定すると設定・認証テストまで行い、VS Codeを起動しない |

初回のホスト鍵確認では、Raspberry Piのコンソールなどで確認したフィンガープリントと照合してから承認する。ED25519ホスト鍵の確認コマンドは次のとおり。

```bash
sudo ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

公開鍵が未登録の場合は、接続確認と公開鍵登録でRaspberry Piユーザーのパスワードを求められることがある。
スクリプトは `%USERPROFILE%\.ssh\config` の専用マーカーで囲まれた接続設定を追加・更新し、`id_ed25519.pub` を接続先の `~/.ssh/authorized_keys` へ完全一致の重複を避けて登録する。

出力に以下が表示されることを確認する。

```text
PUBLIC_KEY_AUTH_OK
PROJECT_DIR_OK
```

`PROJECT_DIR_NOT_FOUND` はプロジェクトが存在しないことを示す。現在のスクリプトはこの表示でも処理を続けるため、「セットアップ完了」だけで判断せず、パスとフォルダーの存在を確認する。

既存の鍵にパスフレーズがある場合、認証テストは `BatchMode=yes` のため入力を求めず失敗することがある。既存のssh-agentへ鍵を読み込むなど、非対話で鍵を利用できる状態にしてから再実行する。

## 6. 接続と作業フォルダーの確認

自動起動したVS Codeで、接続先OSを聞かれた場合は `Linux` を選択する。
リモート接続表示を確認し、ターミナルで実行する。

```bash
hostname
whoami
pwd
git status
```

`RPI-CTRL-01`、`pi`、`/home/pi/tradesystem-rpi` が実環境の設定と一致し、Gitの状態が表示されれば完了。
PythonやDjangoはこのリモートターミナルで実行する。Pythonパッケージは [Pythonパッケージ導入手順](02_Pythonパッケージ導入.md) に従う。

以後は通常権限のWindows PowerShellから接続できる。

```powershell
ssh raspberrypi-trade
```

VS Codeでは `Ctrl+Shift+P` → `Remote-SSH: Connect to Host...` → `raspberrypi-trade` を選択する。
`code` がPATHに登録されている場合は、次のコマンドでも開ける。

```powershell
code --remote ssh-remote+raspberrypi-trade /home/pi/tradesystem-rpi
```

## 7. トラブル対応

| 症状 | 対応 |
| --- | --- |
| 管理者PowerShellを要求される | 1本目を、普段利用するユーザーの管理者PowerShellから実行する |
| スクリプトの実行が禁止される | 実行中のPowerShellでProcessスコープの実行ポリシーを確認する。組織ポリシーは管理者へ確認する |
| VS CodeやSSH鍵が見つからない | 1本目が完了したか、2本目も同じWindowsユーザーで実行しているかを確認する |
| `id_ed25519.pub` だけが存在する | 対応する秘密鍵をバックアップから戻すなど、鍵の状態を整理する。1本目はこの状態で停止する |
| 22番ポートに接続できない | Raspberry Piの電源、IPアドレス、LAN、SSHサービス、ファイアウォールを確認する |
| SSH実効設定が指定値と一致しない | `.ssh/config` に同じ接続名の手動設定や先行する `Host *` 設定がないか確認する |
| 公開鍵認証に失敗する | ユーザー名、鍵の組み合わせ、ssh-agent、接続先の `authorized_keys` と権限を確認する |
| `PROJECT_DIR_NOT_FOUND` | `-RemoteProject` のパスを修正するか、Raspberry Pi側でリポジトリを取得する |
| SSHは成功するがVS Code接続に失敗する | VS Codeの「出力」→「Remote - SSH」でログを確認し、接続先の空き容量やVS Code Serverのダウンロード経路を調べる |

Windows実機での実行確認は別途必要。
