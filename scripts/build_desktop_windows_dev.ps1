# Build an isolated WebCodex Desktop Dev NSIS installer for Windows (no Git).
#
# Requires explicit source identity inputs and sets WEBCODEX_DESKTOP_DEV_EDITION=1
# for Desktop, CLI, Server, and Runner so Dev installs do not share production
# config, services, or updater data. Does not install, run, or stop any app.
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$SourceSha,
    [Parameter(Mandatory = $true)][Int64]$BuiltAt
)

$ErrorActionPreference = "Stop"

function Fail([string]$Message) {
    throw "Windows Desktop Dev build failed: $Message"
}

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        Fail "required command not found: $Name"
    }
}

function Ensure-NpmCi([string]$Prefix) {
    $nodeModules = Join-Path $Prefix "node_modules"
    if (-not (Test-Path -LiteralPath $nodeModules -PathType Container)) {
        Write-Host "Installing npm dependencies in $Prefix"
        & npm ci --prefix $Prefix
        if ($LASTEXITCODE -ne 0) {
            Fail "npm ci failed in $Prefix"
        }
    }
}

# Pre-rename Dev builds left WebCodex.exe in the isolated Tauri target; identity now emits
# WebCodexDev.exe and Cargo does not remove the old filename. Delete only that fixed path.
function Remove-LegacyIsolatedDevWebCodexExe([string]$RepoRoot, [string]$IsolatedTauriTarget) {
    $expectedIsolated = [System.IO.Path]::GetFullPath((Join-Path $RepoRoot "target\desktop-dev-tauri"))
    $resolvedTarget = [System.IO.Path]::GetFullPath($IsolatedTauriTarget)
    if ($resolvedTarget -ne $expectedIsolated) {
        Fail "refusing legacy exe cleanup: Tauri target is not the isolated Dev path ($resolvedTarget)"
    }

    $releaseDir = [System.IO.Path]::GetFullPath((Join-Path $resolvedTarget "release"))
    $legacyExe = [System.IO.Path]::GetFullPath((Join-Path $releaseDir "WebCodex.exe"))
    if ((Split-Path -Leaf $legacyExe) -ne "WebCodex.exe") {
        Fail "refusing legacy exe cleanup: unexpected leaf name at $legacyExe"
    }
    if ((Split-Path -Parent $legacyExe) -ne $releaseDir) {
        Fail "refusing legacy exe cleanup: WebCodex.exe must be directly under release ($legacyExe)"
    }

    if (Test-Path -LiteralPath $legacyExe -PathType Leaf) {
        Write-Host "Removing stale pre-rename Dev artifact: $legacyExe"
        Remove-Item -LiteralPath $legacyExe -Force
    }
}

$Root = Split-Path -Parent $PSScriptRoot
$RuntimeTarget = Join-Path $Root "target\desktop-dev-runtime"
$TauriTarget = Join-Path $Root "target\desktop-dev-tauri"
$OutputDir = Join-Path $Root "target\desktop-dev-dist"
$StageDir = Join-Path $Root ("target\desktop-dev-stage." + [guid]::NewGuid().ToString("N"))
$DevIdentityConfig = Join-Path $Root "apps\desktop\src-tauri\tauri.dev.identity.conf.json"
$PreviousLocation = Get-Location
$PreviousBuiltAt = $env:WEBCODEX_BUILT_AT
$PreviousGitCommit = $env:WEBCODEX_GIT_COMMIT
$PreviousGitDirty = $env:WEBCODEX_GIT_DIRTY
$PreviousDevEdition = $env:WEBCODEX_DESKTOP_DEV_EDITION
$PreviousCargoTarget = $env:CARGO_TARGET_DIR
$PreviousToolchain = $env:RUSTUP_TOOLCHAIN

try {
    foreach ($command in @("node", "npm", "cargo", "rustup")) {
        Require-Command $command
    }

    if ($SourceSha -notmatch '^[0-9A-Fa-f]{40}$') {
        Fail "SourceSha must be one exact 40-hex commit id"
    }
    if ($BuiltAt -le 0) {
        Fail "BuiltAt must be a positive Unix timestamp"
    }
    if (-not (Test-Path -LiteralPath $DevIdentityConfig -PathType Leaf)) {
        Fail "missing Dev Tauri identity overlay: $DevIdentityConfig"
    }

    Set-Location $Root

    $nodeVersion = [string](& node --version)
    if ($LASTEXITCODE -ne 0 -or $nodeVersion -notmatch '^v22(?:\.|$)') {
        Fail "Node.js 22 is required; got '$($nodeVersion.Trim())'"
    }

    $Version = ([string](& node -p "require('./npm/webcodex/package.json').version")).Trim()
    if ($LASTEXITCODE -ne 0 -or -not $Version) {
        Fail "could not read package version from npm/webcodex/package.json"
    }

    $nativeArch = if ($env:PROCESSOR_ARCHITEW6432) {
        $env:PROCESSOR_ARCHITEW6432
    } else {
        $env:PROCESSOR_ARCHITECTURE
    }
    $Platform = switch ($nativeArch.ToUpperInvariant()) {
        "AMD64" { "win32-x64" }
        "ARM64" { "win32-arm64" }
        default { Fail "unsupported Windows architecture: $nativeArch" }
    }

    $SourceSha = $SourceSha.ToLowerInvariant()
    $ShortSource = $SourceSha.Substring(0, 12)
    $GitDirty = $true

    Write-Host "Building WebCodex Desktop Dev installer"
    Write-Host "  source:   $SourceSha"
    Write-Host "  version:  $Version"
    Write-Host "  platform: $Platform"
    Write-Host "  dirty:    $GitDirty (required for this script)"
    Write-Host "  edition:  dev (WEBCODEX_DESKTOP_DEV_EDITION=1)"

    Ensure-NpmCi (Join-Path $Root "frontend")
    Ensure-NpmCi (Join-Path $Root "apps\desktop")

    $env:WEBCODEX_GIT_COMMIT = $SourceSha
    $env:WEBCODEX_GIT_DIRTY = "true"
    $env:WEBCODEX_BUILT_AT = "$BuiltAt"
    $env:WEBCODEX_DESKTOP_DEV_EDITION = "1"
    $env:RUSTUP_TOOLCHAIN = "1.92.0"

    $env:CARGO_TARGET_DIR = $RuntimeTarget
    & rustup run 1.92.0 cargo build --locked --profile dogfood -p webcodex -p webcodex-cli -p webcodex-runner
    if ($LASTEXITCODE -ne 0) {
        Fail "WebCodex Dev runtime build failed"
    }

    & .\scripts\prepare_desktop_bundle.ps1 `
        -BinDir (Join-Path $RuntimeTarget "dogfood") `
        -Version $Version `
        -SourceSha $SourceSha `
        -BuiltAt $BuiltAt `
        -OutputDir $StageDir `
        -GitDirty $GitDirty `
        -DevEdition $true

    $BundleConfig = Join-Path $StageDir "tauri.bundle.conf.json"
    if (-not (Test-Path -LiteralPath $BundleConfig -PathType Leaf)) {
        Fail "Desktop staging did not produce the Tauri bundle overlay"
    }

    New-Item -ItemType Directory -Force -Path $TauriTarget, $OutputDir | Out-Null
    $BundleDir = Join-Path $TauriTarget "release\bundle\nsis"
    if (Test-Path -LiteralPath $BundleDir) {
        Remove-Item -LiteralPath $BundleDir -Recurse -Force
    }

    Remove-LegacyIsolatedDevWebCodexExe -RepoRoot $Root -IsolatedTauriTarget $TauriTarget

    $env:CARGO_TARGET_DIR = $TauriTarget
    Push-Location (Join-Path $Root "apps\desktop")
    try {
        & node node_modules/@tauri-apps/cli/tauri.js build `
            --bundles nsis `
            --config $BundleConfig `
            --config $DevIdentityConfig `
            --ci `
            --no-sign `
            -- `
            --locked
        if ($LASTEXITCODE -ne 0) {
            Fail "Tauri NSIS build failed"
        }
    } finally {
        Pop-Location
    }

    $DevDesktopExe = Join-Path $TauriTarget "release\WebCodexDev.exe"
    if (-not (Test-Path -LiteralPath $DevDesktopExe -PathType Leaf)) {
        Fail "Dev Desktop binary must be WebCodexDev.exe (distinct from production WebCodex.exe); missing: $DevDesktopExe"
    }
    if (Test-Path -LiteralPath (Join-Path $TauriTarget "release\WebCodex.exe") -PathType Leaf) {
        Fail "Dev Tauri build must not emit production WebCodex.exe; check tauri.dev.identity.conf.json mainBinaryName"
    }

    $Installers = @(Get-ChildItem -LiteralPath $BundleDir -Filter "*-setup.exe" -File)
    if ($Installers.Count -ne 1) {
        Fail "expected exactly one NSIS installer, found $($Installers.Count) in $BundleDir"
    }

    $OutputInstaller = Join-Path $OutputDir "webcodex-desktop-dev-$ShortSource-v$Version-$Platform-setup.exe"
    Copy-Item -LiteralPath $Installers[0].FullName -Destination $OutputInstaller -Force

    $Hash = (Get-FileHash -LiteralPath $OutputInstaller -Algorithm SHA256).Hash.ToLowerInvariant()
    $ChecksumPath = "$OutputInstaller.sha256"
    [System.IO.File]::WriteAllText(
        $ChecksumPath,
        "$Hash  $(Split-Path -Leaf $OutputInstaller)`n",
        [System.Text.Encoding]::ASCII
    )

    Write-Host ""
    Write-Host "Dev installer: $OutputInstaller"
    Write-Host "SHA256:        $Hash"
    Write-Host "Checksum file: $ChecksumPath"
} finally {
    Set-Location $PreviousLocation
    if ($null -eq $PreviousBuiltAt) {
        Remove-Item Env:WEBCODEX_BUILT_AT -ErrorAction SilentlyContinue
    } else {
        $env:WEBCODEX_BUILT_AT = $PreviousBuiltAt
    }
    if ($null -eq $PreviousGitCommit) {
        Remove-Item Env:WEBCODEX_GIT_COMMIT -ErrorAction SilentlyContinue
    } else {
        $env:WEBCODEX_GIT_COMMIT = $PreviousGitCommit
    }
    if ($null -eq $PreviousGitDirty) {
        Remove-Item Env:WEBCODEX_GIT_DIRTY -ErrorAction SilentlyContinue
    } else {
        $env:WEBCODEX_GIT_DIRTY = $PreviousGitDirty
    }
    if ($null -eq $PreviousDevEdition) {
        Remove-Item Env:WEBCODEX_DESKTOP_DEV_EDITION -ErrorAction SilentlyContinue
    } else {
        $env:WEBCODEX_DESKTOP_DEV_EDITION = $PreviousDevEdition
    }
    if ($null -eq $PreviousCargoTarget) {
        Remove-Item Env:CARGO_TARGET_DIR -ErrorAction SilentlyContinue
    } else {
        $env:CARGO_TARGET_DIR = $PreviousCargoTarget
    }
    if ($null -eq $PreviousToolchain) {
        Remove-Item Env:RUSTUP_TOOLCHAIN -ErrorAction SilentlyContinue
    } else {
        $env:RUSTUP_TOOLCHAIN = $PreviousToolchain
    }
}
