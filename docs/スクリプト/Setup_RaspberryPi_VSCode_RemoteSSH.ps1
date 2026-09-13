#requires -Version 5.1
<#
.SYNOPSIS
    Windows 11 の VS Code から Raspberry Pi へ Remote SSH 接続する設定を作成します。

.DESCRIPTION
    Install_VSCode_Codex_Japanese_OpenSSH_CLEAN.ps1 とは独立して実行できます。

    実施内容:
      - VS Code / OpenSSH / ED25519鍵の存在確認
      - VS Code Remote - SSH 拡張機能の導入
      - ~/.ssh/config に Raspberry Pi 接続設定を冪等追加/更新
      - Raspberry Pi の ~/.ssh/authorized_keys に公開鍵を重複なしで登録
      - 公開鍵転送は Base64 を使用し、1行の公開鍵を絶対に改行分割しない
      - パスワードなし公開鍵認証テスト
      - -RemoteProject で指定した Raspberry Pi 上のプロジェクトを VS Code Remote SSH で開く

    FIXED:
      旧版では PowerShell 5.1 から公開鍵を標準入力で SSH に渡したため、
      authorized_keys の1行が複数行に分割される場合がありました。
      本版では公開鍵とリモートシェル処理を Base64 化して渡すことで修正しています。

.EXAMPLE
    & "C:\Users\norit\Downloads\Setup_RaspberryPi_VSCode_RemoteSSH_FIXED2.ps1" `
        -PiHost "192.168.0.10" `
        -PiUser "pi" `
        -RemoteProject "/home/pi/tradesystem-rpi"

.EXAMPLE
    & "C:\Users\norit\Downloads\Setup_RaspberryPi_VSCode_RemoteSSH_FIXED2.ps1" `
        -PiHost "192.168.0.10" `
        -PiUser "pi" `
        -RemoteProject "/home/pi/tradesystem-rpi" `
        -HostAlias "raspberrypi-trade" `
        -RemoteProject "/DATA/TradeSystem"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$PiHost,

    [Parameter(Mandatory = $true)]
    [string]$PiUser,

    [Parameter(Mandatory = $false)]
    [string]$HostAlias = "raspberrypi-trade",

    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$RemoteProject,

    [Parameter(Mandatory = $false)]
    [switch]$NoOpenVSCode
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

function Write-Step {
    param(
        [string]$Number,
        [string]$Text
    )

    Write-Host ""
    Write-Host "================================================================"
    Write-Host "[$Number] $Text"
    Write-Host "================================================================"
}

function Get-CodeCommand {
    $Candidates = @(
        "$env:LOCALAPPDATA\Programs\Microsoft VS Code\bin\code.cmd",
        "$env:ProgramFiles\Microsoft VS Code\bin\code.cmd"
    )

    foreach ($Candidate in $Candidates) {
        if (Test-Path $Candidate) {
            return $Candidate
        }
    }

    return $null
}

function Get-SSHExe {
    $Candidates = @(
        "$env:WINDIR\System32\OpenSSH\ssh.exe"
    )

    $Command = Get-Command ssh.exe -ErrorAction SilentlyContinue
    if ($Command) {
        $Candidates += $Command.Source
    }

    foreach ($Candidate in $Candidates) {
        if ($Candidate -and (Test-Path $Candidate)) {
            return $Candidate
        }
    }

    return $null
}

function Convert-ToBase64Utf8 {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Text
    )

    $Bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
    return [Convert]::ToBase64String($Bytes)
}

Write-Host ""
Write-Host "################################################################"
Write-Host "# VS Code -> Raspberry Pi Remote SSH セットアップ FIXED2"
Write-Host "################################################################"
Write-Host ""
Write-Host "HostAlias     : $HostAlias"
Write-Host "Raspberry Pi  : $PiUser@$PiHost"
Write-Host "RemoteProject : $RemoteProject"


# ------------------------------------------------------------
# 1. 必須コンポーネント確認
# ------------------------------------------------------------

Write-Step "1/8" "VS Code / OpenSSH / SSH鍵確認"

$CodeCommand = Get-CodeCommand
if (-not $CodeCommand) {
    throw "VS Code の code.cmd が見つかりません。先に Install_VSCode_Codex_Japanese_OpenSSH_CLEAN.ps1 を実行してください。"
}

$SSHExe = Get-SSHExe
if (-not $SSHExe) {
    throw "OpenSSH Client の ssh.exe が見つかりません。"
}

$SSHDir = Join-Path $env:USERPROFILE ".ssh"
$SSHPrivateKey = Join-Path $SSHDir "id_ed25519"
$SSHPublicKey = Join-Path $SSHDir "id_ed25519.pub"
$SSHConfig = Join-Path $SSHDir "config"

if (-not (Test-Path $SSHPrivateKey)) {
    throw "SSH秘密鍵がありません: $SSHPrivateKey"
}

if (-not (Test-Path $SSHPublicKey)) {
    throw "SSH公開鍵がありません: $SSHPublicKey"
}

Write-Host "[OK] VS Code : $CodeCommand"
Write-Host "[OK] OpenSSH : $SSHExe"
Write-Host "[OK] 秘密鍵  : $SSHPrivateKey"
Write-Host "[OK] 公開鍵  : $SSHPublicKey"


# ------------------------------------------------------------
# 2. Remote - SSH 拡張機能
# ------------------------------------------------------------

Write-Step "2/8" "VS Code Remote - SSH 拡張機能確認"

$Extensions = @(
    & $CodeCommand --list-extensions
)

$RemoteSSHInstalled = $Extensions |
    Where-Object { $_ -ieq "ms-vscode-remote.remote-ssh" }

if ($RemoteSSHInstalled) {
    Write-Host "[SKIP] ms-vscode-remote.remote-ssh はインストール済みです。"
}
else {
    Write-Host "[RUN ] Remote - SSH をインストールします。"

    & $CodeCommand `
        --install-extension "ms-vscode-remote.remote-ssh"

    if ($LASTEXITCODE -ne 0) {
        throw "Remote - SSH 拡張機能のインストールに失敗しました。"
    }

    Write-Host "[DONE] Remote - SSH をインストールしました。"
}


# ------------------------------------------------------------
# 3. ~/.ssh/config 作成・更新
# ------------------------------------------------------------

Write-Step "3/8" "SSH config 作成・更新"

New-Item `
    -ItemType Directory `
    -Force `
    -Path $SSHDir |
    Out-Null

$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)

$BeginMarker = "# BEGIN CHATGPT $HostAlias"
$EndMarker = "# END CHATGPT $HostAlias"

$HostBlock = @"
$BeginMarker
Host $HostAlias
    HostName $PiHost
    User $PiUser
    IdentityFile ~/.ssh/id_ed25519
    IdentitiesOnly yes
    ServerAliveInterval 60
    ServerAliveCountMax 3
$EndMarker
"@

if (Test-Path $SSHConfig) {
    $ConfigText = [System.IO.File]::ReadAllText($SSHConfig)

    $EscapedBegin = [regex]::Escape($BeginMarker)
    $EscapedEnd = [regex]::Escape($EndMarker)
    $Pattern = "(?ms)^$EscapedBegin\r?\n.*?^$EscapedEnd\r?\n?"

    if ([regex]::IsMatch($ConfigText, $Pattern)) {
        $ConfigText = [regex]::Replace(
            $ConfigText,
            $Pattern,
            ($HostBlock.TrimEnd() + "`r`n")
        )

        Write-Host "[DONE] 既存の $HostAlias 設定を更新しました。"
    }
    else {
        if (-not $ConfigText.EndsWith("`n")) {
            $ConfigText += "`r`n"
        }

        $ConfigText += "`r`n" + $HostBlock.TrimEnd() + "`r`n"
        Write-Host "[DONE] $HostAlias 設定を追加しました。"
    }
}
else {
    $ConfigText = $HostBlock.TrimEnd() + "`r`n"
    Write-Host "[DONE] SSH config を新規作成しました。"
}

[System.IO.File]::WriteAllText(
    $SSHConfig,
    $ConfigText,
    $Utf8NoBom
)

Write-Host ""
Write-Host "SSH config:"
Write-Host "---------------------------------------------------------------"
$HostBlock -split "`r?`n" | ForEach-Object {
    Write-Host $_
}
Write-Host "---------------------------------------------------------------"


# ------------------------------------------------------------
# 4. SSH実効設定確認
# ------------------------------------------------------------

Write-Step "4/8" "SSH実効設定確認"

$EffectiveConfig = @(
    & $SSHExe -G $HostAlias 2>$null
)

if ($LASTEXITCODE -ne 0) {
    throw "ssh -G による実効設定確認に失敗しました。"
}

$EffectiveHostName = (
    $EffectiveConfig |
        Where-Object { $_ -match '^hostname\s+' } |
        Select-Object -First 1
) -replace '^hostname\s+', ''

$EffectiveUser = (
    $EffectiveConfig |
        Where-Object { $_ -match '^user\s+' } |
        Select-Object -First 1
) -replace '^user\s+', ''

Write-Host "hostname : $EffectiveHostName"
Write-Host "user     : $EffectiveUser"

if ($EffectiveHostName -ne $PiHost) {
    throw "SSH実効設定のHostNameが指定値と一致しません。指定=$PiHost 実効=$EffectiveHostName"
}

if ($EffectiveUser -ne $PiUser) {
    throw "SSH実効設定のUserが指定値と一致しません。指定=$PiUser 実効=$EffectiveUser"
}

Write-Host "[DONE] SSH実効設定は指定値と一致しています。"


# ------------------------------------------------------------
# 5. 初回SSH接続確認
# ------------------------------------------------------------

Write-Step "5/8" "Raspberry Pi SSH接続確認"

Write-Host "初回接続ではSSHホスト鍵の確認が表示される場合があります。"
Write-Host "公開鍵未登録の場合は Raspberry Pi ユーザーのパスワードを入力してください。"
Write-Host ""

& $SSHExe `
    $HostAlias `
    "printf 'SSH_CONNECTION_OK\n'"

if ($LASTEXITCODE -ne 0) {
    throw "Raspberry Pi へのSSH接続に失敗しました。ExitCode=$LASTEXITCODE"
}

Write-Host "[DONE] SSH接続成功"


# ------------------------------------------------------------
# 6. 公開鍵を authorized_keys へ重複なし登録
#    FIX: PowerShell標準入力を使用しない
# ------------------------------------------------------------

Write-Step "6/8" "SSH公開鍵登録（Base64安全転送）"

$PublicKeyText = [System.IO.File]::ReadAllText($SSHPublicKey).Trim()

if ([string]::IsNullOrWhiteSpace($PublicKeyText)) {
    throw "SSH公開鍵が空です。"
}

# OpenSSH公開鍵は必ず
#   key-type base64-data optional-comment
# の1行形式であることを確認する。
if ($PublicKeyText -notmatch '^(ssh-ed25519|ssh-rsa|ecdsa-sha2-[^\s]+)\s+\S+(\s+.*)?$') {
    throw "SSH公開鍵の形式が不正です。公開鍵は1行である必要があります: $SSHPublicKey"
}

$PublicKeyBase64 = Convert-ToBase64Utf8 -Text $PublicKeyText

# リモート側スクリプト内では公開鍵をBase64から復元する。
# これにより公開鍵中の空白がPowerShell/ssh経由で改行に変換される問題を回避する。
$RemoteScriptTemplate = @'
set -eu

umask 077
mkdir -p "$HOME/.ssh"
touch "$HOME/.ssh/authorized_keys"
chmod 700 "$HOME/.ssh"
chmod 600 "$HOME/.ssh/authorized_keys"

key="$(printf '%s' '__PUBLIC_KEY_BASE64__' | base64 -d)"

if printf '%s' "$key" | grep -q '[[:space:]]'; then
    :
else
    printf 'ERROR_INVALID_PUBLIC_KEY\n' >&2
    exit 20
fi

if grep -qxF "$key" "$HOME/.ssh/authorized_keys"; then
    printf 'AUTHORIZED_KEY_ALREADY_EXISTS\n'
else
    printf '%s\n' "$key" >> "$HOME/.ssh/authorized_keys"
    printf 'AUTHORIZED_KEY_ADDED\n'
fi

# 登録後に完全一致を再確認する。
if ! grep -qxF "$key" "$HOME/.ssh/authorized_keys"; then
    printf 'ERROR_AUTHORIZED_KEY_VERIFY_FAILED\n' >&2
    exit 21
fi
'@

$RemoteScript = $RemoteScriptTemplate.Replace(
    "__PUBLIC_KEY_BASE64__",
    $PublicKeyBase64
)

# リモートスクリプト自体もBase64化する。
# ssh.exe へはASCIIだけの1行コマンドを渡す。
$RemoteScriptBase64 = Convert-ToBase64Utf8 -Text $RemoteScript

$RemoteCommand = "printf '%s' '$RemoteScriptBase64' | base64 -d | sh"

& $SSHExe `
    $HostAlias `
    $RemoteCommand

if ($LASTEXITCODE -ne 0) {
    throw "SSH公開鍵の登録に失敗しました。ExitCode=$LASTEXITCODE"
}

Write-Host "[DONE] authorized_keys へ1行形式で登録しました。"


# ------------------------------------------------------------
# 7. パスワードなし公開鍵認証テスト
# ------------------------------------------------------------

Write-Step "7/8" "パスワードなし公開鍵認証テスト"

$RemoteTestCommand = "printf 'PUBLIC_KEY_AUTH_OK\n'; test -d '$RemoteProject' && printf 'PROJECT_DIR_OK\n' || printf 'PROJECT_DIR_NOT_FOUND\n'"

& $SSHExe `
    -o BatchMode=yes `
    -o PasswordAuthentication=no `
    -o PreferredAuthentications=publickey `
    $HostAlias `
    $RemoteTestCommand

if ($LASTEXITCODE -ne 0) {
    throw "公開鍵認証に失敗しました。Raspberry Pi側のsshd設定・authorized_keys・権限を確認してください。"
}

Write-Host "[DONE] 公開鍵認証成功"


# ------------------------------------------------------------
# 8. VS Code Remote SSH 起動
# ------------------------------------------------------------

Write-Step "8/8" "VS Code Remote SSH"

if ($NoOpenVSCode) {
    Write-Host "[SKIP] -NoOpenVSCode が指定されたためVS Codeは起動しません。"
}
else {
    Write-Host "[RUN ] VS Codeで $RemoteProject を開きます。"

    & $CodeCommand `
        --remote "ssh-remote+$HostAlias" `
        $RemoteProject

    if ($LASTEXITCODE -ne 0) {
        throw "VS Code Remote SSH の起動に失敗しました。"
    }

    Write-Host "[DONE] VS Code Remote SSH を起動しました。"
}

Write-Host ""
Write-Host "################################################################"
Write-Host "# Raspberry Pi Remote SSH セットアップ完了"
Write-Host "################################################################"
Write-Host ""
Write-Host "接続名       : $HostAlias"
Write-Host "接続先       : $PiUser@$PiHost"
Write-Host "プロジェクト : $RemoteProject"
Write-Host ""
Write-Host "以後のSSH接続:"
Write-Host "ssh $HostAlias"
Write-Host ""
Write-Host "以後のVS Code接続:"
Write-Host "code --remote ssh-remote+$HostAlias $RemoteProject"
Write-Host ""
