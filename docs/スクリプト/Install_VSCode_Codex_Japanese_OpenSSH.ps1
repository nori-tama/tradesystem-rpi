#requires -Version 5.1
<#
.SYNOPSIS
    Windows 11 開発環境をクリーン再構築するセットアップスクリプト

.DESCRIPTION
    VS Code の既存状態をバックアップしてからクリーン再構築します。

    構築内容:
      - Visual Studio Code User Setup
      - Japanese Language Pack
      - OpenAI Codex
      - 恒久表示言語 ja
      - OpenSSH Client
      - ED25519 SSH鍵

    方針:
      - winget 不使用
      - Microsoft Store 不使用
      - VS Codeの既存ユーザーデータ/拡張機能をバックアップ後に初期化
      - VS Code公式の argv.json に locale=ja を設定
      - 最後は --locale を付けず通常起動
      - SSH鍵が既にある場合は削除しない

.NOTES
    OpenSSH Client の追加には管理者権限が必要です。
    VS Code の Settings Sync にサインインすると、Display Language が同期される場合があります。
#>

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

function Test-Administrator {
    $Identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $Principal = New-Object Security.Principal.WindowsPrincipal($Identity)

    return $Principal.IsInRole(
        [Security.Principal.WindowsBuiltInRole]::Administrator
    )
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

function Get-CodeExe {
    $Candidates = @(
        "$env:LOCALAPPDATA\Programs\Microsoft VS Code\Code.exe",
        "$env:ProgramFiles\Microsoft VS Code\Code.exe"
    )

    foreach ($Candidate in $Candidates) {
        if (Test-Path $Candidate) {
            return $Candidate
        }
    }

    return $null
}

Write-Host ""
Write-Host "################################################################"
Write-Host "# Windows 11 VS Code/Codex 日本語環境 クリーン再構築"
Write-Host "################################################################"
Write-Host ""

if (-not (Test-Administrator)) {
    throw "このスクリプトは管理者PowerShellで実行してください。"
}


# ------------------------------------------------------------
# 1. パス定義
# ------------------------------------------------------------

Write-Step "1/14" "環境・パス確認"

$Architecture = $env:PROCESSOR_ARCHITECTURE

switch ($Architecture) {
    "AMD64" {
        $VSCodePlatform = "win32-x64-user"
    }
    "ARM64" {
        $VSCodePlatform = "win32-arm64-user"
    }
    default {
        throw "未対応のWindowsアーキテクチャです: $Architecture"
    }
}

$TimeStamp = Get-Date -Format "yyyyMMdd_HHmmss"

$WorkDir = "C:\TEMP\vscode-clean-setup"
$BackupRoot = Join-Path $WorkDir "backup_$TimeStamp"
$InstallerPath = Join-Path $WorkDir "VSCodeUserSetup.exe"

$VSCodeInstallDir = "$env:LOCALAPPDATA\Programs\Microsoft VS Code"
$VSCodeAppData = Join-Path $env:APPDATA "Code"
$VSCodeLocalData = Join-Path $env:LOCALAPPDATA "Code"
$VSCodeDotDir = Join-Path $env:USERPROFILE ".vscode"

$SSHDir = Join-Path $env:USERPROFILE ".ssh"
$SSHPrivateKey = Join-Path $SSHDir "id_ed25519"
$SSHPublicKey = Join-Path $SSHDir "id_ed25519.pub"

New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null
New-Item -ItemType Directory -Force -Path $BackupRoot | Out-Null

Write-Host "Architecture     : $Architecture"
Write-Host "VS Code Platform : $VSCodePlatform"
Write-Host "作業ディレクトリ: $WorkDir"
Write-Host "バックアップ先  : $BackupRoot"


# ------------------------------------------------------------
# 2. VS Code完全終了
# ------------------------------------------------------------

Write-Step "2/14" "VS Code完全終了"

$RunningCode = Get-Process Code -ErrorAction SilentlyContinue

if ($RunningCode) {
    $RunningCode |
        Stop-Process -Force -ErrorAction Stop

    Start-Sleep -Seconds 2

    Write-Host "[DONE] VS Codeを終了しました。"
}
else {
    Write-Host "[SKIP] VS Codeは起動していません。"
}


# ------------------------------------------------------------
# 3. 既存VS Code設定バックアップ
# ------------------------------------------------------------

Write-Step "3/14" "既存VS Code状態バックアップ"

$BackupTargets = @(
    @{
        Source = $VSCodeAppData
        Name   = "AppData_Roaming_Code"
    },
    @{
        Source = $VSCodeLocalData
        Name   = "AppData_Local_Code"
    },
    @{
        Source = $VSCodeDotDir
        Name   = "UserProfile_dot_vscode"
    }
)

foreach ($Target in $BackupTargets) {
    if (Test-Path $Target.Source) {
        $Destination = Join-Path $BackupRoot $Target.Name

        Copy-Item `
            -Path $Target.Source `
            -Destination $Destination `
            -Recurse `
            -Force

        Write-Host "[DONE] バックアップ:"
        Write-Host "       $($Target.Source)"
        Write-Host "    -> $Destination"
    }
    else {
        Write-Host "[SKIP] 存在しません: $($Target.Source)"
    }
}


# ------------------------------------------------------------
# 4. VS Codeアンインストール
# ------------------------------------------------------------

Write-Step "4/14" "既存VS Codeアンインストール"

$UninstallCandidates = @(
    "$VSCodeInstallDir\unins000.exe",
    "$env:ProgramFiles\Microsoft VS Code\unins000.exe"
)

$Uninstaller = $null

foreach ($Candidate in $UninstallCandidates) {
    if (Test-Path $Candidate) {
        $Uninstaller = $Candidate
        break
    }
}

if ($Uninstaller) {
    Write-Host "[RUN ] VS Codeをアンインストールします。"
    Write-Host "       $Uninstaller"

    $UninstallProcess = Start-Process `
        -FilePath $Uninstaller `
        -ArgumentList "/VERYSILENT", "/NORESTART" `
        -Wait `
        -PassThru

    if ($UninstallProcess.ExitCode -ne 0) {
        throw "VS Codeのアンインストールに失敗しました。ExitCode=$($UninstallProcess.ExitCode)"
    }

    Write-Host "[DONE] VS Codeをアンインストールしました。"
}
else {
    Write-Host "[SKIP] VS Codeアンインストーラーはありません。"
}

Start-Sleep -Seconds 2


# ------------------------------------------------------------
# 5. VS Codeユーザー状態をクリーン化
# ------------------------------------------------------------

Write-Step "5/14" "VS Codeユーザー状態クリーン化"

$CleanTargets = @(
    $VSCodeAppData,
    $VSCodeLocalData,
    $VSCodeDotDir,
    $VSCodeInstallDir
)

foreach ($Target in $CleanTargets) {
    if (Test-Path $Target) {
        Remove-Item `
            -Path $Target `
            -Recurse `
            -Force

        Write-Host "[DONE] 削除: $Target"
    }
    else {
        Write-Host "[SKIP] 存在しません: $Target"
    }
}

# .ssh は対象外。SSH鍵は保持する。
Write-Host ""
Write-Host "[INFO] $SSHDir は削除しません。"


# ------------------------------------------------------------
# 6. VS Code最新版取得
# ------------------------------------------------------------

Write-Step "6/14" "VS Code最新版ダウンロード"

$VSCodeDownloadUrl = "https://update.code.visualstudio.com/latest/$VSCodePlatform/stable"

if (Test-Path $InstallerPath) {
    Remove-Item -Path $InstallerPath -Force
}

Invoke-WebRequest `
    -Uri $VSCodeDownloadUrl `
    -OutFile $InstallerPath `
    -UseBasicParsing

if (-not (Test-Path $InstallerPath)) {
    throw "VS Codeインストーラーを取得できませんでした。"
}

if ((Get-Item $InstallerPath).Length -lt 1MB) {
    throw "VS Codeインストーラーのサイズが異常です。"
}

Write-Host "[DONE] $InstallerPath"


# ------------------------------------------------------------
# 7. VS Codeクリーンインストール
# ------------------------------------------------------------

Write-Step "7/14" "VS Codeクリーンインストール"

$InstallProcess = Start-Process `
    -FilePath $InstallerPath `
    -ArgumentList @(
        "/VERYSILENT",
        "/NORESTART",
        "/MERGETASKS=!runcode"
    ) `
    -Wait `
    -PassThru

if ($InstallProcess.ExitCode -ne 0) {
    throw "VS Codeのインストールに失敗しました。ExitCode=$($InstallProcess.ExitCode)"
}

$CodeCommand = Get-CodeCommand
$CodeExe = Get-CodeExe

if (-not $CodeCommand) {
    throw "VS Codeインストール後に code.cmd を検出できません。"
}

if (-not $CodeExe) {
    throw "VS Codeインストール後に Code.exe を検出できません。"
}

Write-Host "[DONE] VS Code:"
& $CodeCommand --version | ForEach-Object {
    Write-Host "       $_"
}


# ------------------------------------------------------------
# 8. Japanese Language Packのみ先に導入
# ------------------------------------------------------------

Write-Step "8/14" "Japanese Language Packインストール"

& $CodeCommand `
    --install-extension "MS-CEINTL.vscode-language-pack-ja" `
    --force

if ($LASTEXITCODE -ne 0) {
    throw "Japanese Language Packのインストールに失敗しました。"
}

$ExtensionsAfterJapanese = @(
    & $CodeCommand --list-extensions
)

if (-not ($ExtensionsAfterJapanese |
        Where-Object { $_ -ieq "ms-ceintl.vscode-language-pack-ja" })) {
    throw "Japanese Language Packを確認できません。"
}

Write-Host "[DONE] ms-ceintl.vscode-language-pack-ja"


# ------------------------------------------------------------
# 9. argv.jsonを公式方式で新規作成
# ------------------------------------------------------------

Write-Step "9/14" "VS Code表示言語を日本語へ固定"

New-Item `
    -ItemType Directory `
    -Force `
    -Path $VSCodeDotDir |
    Out-Null

$ArgvJsonPath = Join-Path $VSCodeDotDir "argv.json"

$ArgvContent = @'
{
    "locale": "ja"
}
'@

# Windows PowerShell 5.1 のUTF8(BOM付き)を避ける。
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)

[System.IO.File]::WriteAllText(
    $ArgvJsonPath,
    $ArgvContent,
    $Utf8NoBom
)

$ArgvReadBack = [System.IO.File]::ReadAllText($ArgvJsonPath)

if ($ArgvReadBack -notmatch '"locale"\s*:\s*"ja"') {
    throw "argv.jsonのlocale=ja確認に失敗しました。"
}

$ArgvBytes = [System.IO.File]::ReadAllBytes($ArgvJsonPath)

$HasBom = (
    $ArgvBytes.Length -ge 3 -and
    $ArgvBytes[0] -eq 0xEF -and
    $ArgvBytes[1] -eq 0xBB -and
    $ArgvBytes[2] -eq 0xBF
)

if ($HasBom) {
    throw "argv.json がUTF-8 BOM付きになっています。"
}

Write-Host "[DONE] $ArgvJsonPath"
Get-Content $ArgvJsonPath | ForEach-Object {
    Write-Host "       $_"
}


# ------------------------------------------------------------
# 10. 初回日本語ブートストラップ
# ------------------------------------------------------------

Write-Step "10/14" "日本語Language Pack初期化"

# クリーン状態で一度だけ locale=ja を明示して起動する。
# この起動はLanguage Packの初回初期化用。
$BootstrapProcess = Start-Process `
    -FilePath $CodeExe `
    -ArgumentList "--locale=ja", "--new-window" `
    -PassThru

Start-Sleep -Seconds 10

$CodeProcesses = Get-Process Code -ErrorAction SilentlyContinue

if (-not $CodeProcesses) {
    throw "VS Codeの日本語初期化起動を確認できませんでした。"
}

Write-Host "[DONE] 日本語初期化起動を確認しました。"

$CodeProcesses |
    Stop-Process -Force -ErrorAction SilentlyContinue

Start-Sleep -Seconds 3


# ------------------------------------------------------------
# 11. OpenAI Codex
# ------------------------------------------------------------

Write-Step "11/14" "OpenAI Codexインストール"

& $CodeCommand `
    --install-extension "openai.chatgpt" `
    --force

if ($LASTEXITCODE -ne 0) {
    throw "OpenAI Codex拡張機能のインストールに失敗しました。"
}

$ExtensionsFinal = @(
    & $CodeCommand --list-extensions
)

if (-not ($ExtensionsFinal |
        Where-Object { $_ -ieq "openai.chatgpt" })) {
    throw "OpenAI Codex拡張を確認できません。"
}

if (-not ($ExtensionsFinal |
        Where-Object { $_ -ieq "ms-ceintl.vscode-language-pack-ja" })) {
    throw "Japanese Language Packを確認できません。"
}

Write-Host "[DONE] openai.chatgpt"


# ------------------------------------------------------------
# 12. OpenSSH Client
# ------------------------------------------------------------

Write-Step "12/14" "OpenSSH Client確認・インストール"

$OpenSSHCapability = Get-WindowsCapability -Online |
    Where-Object { $_.Name -like "OpenSSH.Client*" } |
    Select-Object -First 1

if (-not $OpenSSHCapability) {
    throw "OpenSSH.Client Capabilityを取得できません。"
}

Write-Host "Name  : $($OpenSSHCapability.Name)"
Write-Host "State : $($OpenSSHCapability.State)"

if ($OpenSSHCapability.State -ne "Installed") {
    Write-Host "[RUN ] OpenSSH Clientをインストールします。"

    $OpenSSHResult = Add-WindowsCapability `
        -Online `
        -Name $OpenSSHCapability.Name

    $OpenSSHCapability = Get-WindowsCapability -Online |
        Where-Object { $_.Name -like "OpenSSH.Client*" } |
        Select-Object -First 1

    if ($OpenSSHCapability.State -ne "Installed") {
        throw "OpenSSH Clientのインストールを確認できません。"
    }

    Write-Host "[DONE] OpenSSH Clientをインストールしました。"
}
else {
    Write-Host "[SKIP] OpenSSH Clientはインストール済みです。"
}

$SSHExe = "$env:WINDIR\System32\OpenSSH\ssh.exe"
$SSHKeygenExe = "$env:WINDIR\System32\OpenSSH\ssh-keygen.exe"

if (-not (Test-Path $SSHExe)) {
    throw "ssh.exeが見つかりません。"
}

if (-not (Test-Path $SSHKeygenExe)) {
    throw "ssh-keygen.exeが見つかりません。"
}

$OpenSSHVersion = & cmd.exe /c "`"$SSHExe`" -V 2>&1"

if ($LASTEXITCODE -ne 0) {
    throw "OpenSSHバージョン確認に失敗しました。"
}

Write-Host "OpenSSH:"
$OpenSSHVersion | ForEach-Object {
    Write-Host "       $_"
}


# ------------------------------------------------------------
# 13. SSH鍵
# ------------------------------------------------------------

Write-Step "13/14" "SSH鍵確認・作成"

New-Item `
    -ItemType Directory `
    -Force `
    -Path $SSHDir |
    Out-Null

$PrivateExists = Test-Path $SSHPrivateKey
$PublicExists = Test-Path $SSHPublicKey

if ($PrivateExists -and $PublicExists) {
    Write-Host "[SKIP] SSH鍵は既に存在します。"
}
elseif ($PrivateExists -and -not $PublicExists) {
    Write-Host "[RUN ] 既存秘密鍵から公開鍵を復元します。"

    $PublicKey = & $SSHKeygenExe `
        -y `
        -f $SSHPrivateKey

    if ($LASTEXITCODE -ne 0) {
        throw "SSH公開鍵の復元に失敗しました。"
    }

    [System.IO.File]::WriteAllText(
        $SSHPublicKey,
        (($PublicKey -join "`n").Trim() + "`n"),
        $Utf8NoBom
    )

    Write-Host "[DONE] 公開鍵を復元しました。"
}
elseif (-not $PrivateExists -and $PublicExists) {
    throw "id_ed25519.pubのみ存在します。秘密鍵を自動上書きしません。"
}
else {
    Write-Host "[RUN ] ED25519 SSH鍵をパスフレーズなしで作成します。"

    $SSHKeygenArguments = @(
        "-t",
        "ed25519",
        "-C",
        "windows-codex-raspberrypi",
        "-f",
        "`"$SSHPrivateKey`"",
        "-N",
        '""'
    )

    $SSHKeyProcess = Start-Process `
        -FilePath $SSHKeygenExe `
        -ArgumentList $SSHKeygenArguments `
        -Wait `
        -PassThru `
        -NoNewWindow

    if ($SSHKeyProcess.ExitCode -ne 0) {
        throw "SSH鍵作成に失敗しました。ExitCode=$($SSHKeyProcess.ExitCode)"
    }

    if (
        -not (Test-Path $SSHPrivateKey) -or
        -not (Test-Path $SSHPublicKey)
    ) {
        throw "SSH鍵ファイルを確認できません。"
    }

    Write-Host "[DONE] SSH鍵を作成しました。"
}

Write-Host "秘密鍵: $SSHPrivateKey"
Write-Host "公開鍵: $SSHPublicKey"


# ------------------------------------------------------------
# 14. 最終確認 + 引数なし通常起動
# ------------------------------------------------------------

Write-Step "14/14" "最終確認・通常起動"

$FinalExtensions = @(
    & $CodeCommand --list-extensions
)

$FinalJapanese = $FinalExtensions |
    Where-Object { $_ -ieq "ms-ceintl.vscode-language-pack-ja" }

$FinalCodex = $FinalExtensions |
    Where-Object { $_ -ieq "openai.chatgpt" }

if (-not $FinalJapanese) {
    throw "最終確認: Japanese Language Packがありません。"
}

if (-not $FinalCodex) {
    throw "最終確認: OpenAI Codexがありません。"
}

$FinalArgv = [System.IO.File]::ReadAllText($ArgvJsonPath)

if ($FinalArgv -notmatch '"locale"\s*:\s*"ja"') {
    throw "最終確認: argv.jsonのlocaleがjaではありません。"
}

if (
    -not (Test-Path $SSHPrivateKey) -or
    -not (Test-Path $SSHPublicKey)
) {
    throw "最終確認: SSH鍵がありません。"
}

Get-Process Code -ErrorAction SilentlyContinue |
    Stop-Process -Force -ErrorAction SilentlyContinue

Start-Sleep -Seconds 2

# ここでは --locale を絶対に指定しない。
Start-Process `
    -FilePath $CodeExe

Start-Sleep -Seconds 5

if (-not (Get-Process Code -ErrorAction SilentlyContinue)) {
    throw "VS Codeの通常起動を確認できませんでした。"
}

Write-Host ""
Write-Host "################################################################"
Write-Host "# セットアップ完了"
Write-Host "################################################################"
Write-Host ""
Write-Host "VS Code              : OK"
Write-Host "Japanese Language Pack: OK"
Write-Host "Display locale       : ja"
Write-Host "OpenAI Codex         : OK"
Write-Host "OpenSSH Client       : OK"
Write-Host "SSH秘密鍵            : OK"
Write-Host "SSH公開鍵            : OK"
Write-Host ""
Write-Host "VS Codeは --locale を付けず通常起動しました。"
Write-Host ""
Write-Host "バックアップ:"
Write-Host "$BackupRoot"
Write-Host ""
Write-Host "注意:"
Write-Host "Settings Syncへサインインした後に英語へ戻る場合、"
Write-Host "Display Languageの同期状態が英語を復元している可能性があります。"
Write-Host ""
