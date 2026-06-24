# pi-server Windows installer
# Usage:
#   powershell -c "irm https://raw.githubusercontent.com/mikesoylu/pi-server/main/setup.ps1 | iex"
#
# Options:
#   -Version <tag>   : install a specific release tag (default: latest main)
#   -Dest <path>     : installation directory (default: ~\AppData\Local\pi-server\bin)

param(
    [string]$Version = "",
    [string]$Dest = ""
)

$ErrorActionPreference = "Stop"

# --- defaults ---
$repo = "mikesoylu/pi-server"
if (-not $Dest) {
    $Dest = Join-Path $env:LOCALAPPDATA "pi-server" "bin"
}
if (-not (Test-Path $Dest)) {
    New-Item -ItemType Directory -Path $Dest -Force | Out-Null
}

# --- detect architecture ---
$arch = switch ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture) {
    "X64"   { "amd64" }
    "Arm64" { "arm64" }
    default { throw "Unsupported architecture: $_" }
}
$target = "windows-$arch"
Write-Host "Detected target: $target"

# --- resolve version ---
if (-not $Version) {
    Write-Host "Fetching latest main release..."
    $releases = curl.exe -s "https://api.github.com/repos/$repo/releases" | ConvertFrom-Json
    $latest = $releases | Where-Object { $_.prerelease } | Select-Object -First 1
    if (-not $latest) {
        throw "No prerelease found. Try pinning a version with -Version."
    }
    $Version = $latest.tag_name
}
Write-Host "Installing version: $Version"

# --- download archive ---
$archiveName = "$target.zip"
$downloadUrl = "https://github.com/$repo/releases/download/$Version/$archiveName"
$zipPath = Join-Path $env:TEMP "pi-server-$Version.zip"

Write-Host "Downloading: $downloadUrl"
curl.exe -sL -o $zipPath $downloadUrl
if (-not (Test-Path $zipPath)) {
    throw "Download failed: $downloadUrl"
}

# --- extract ---
Write-Host "Extracting to: $Dest"
try {
    # Use .NET ZIP extraction (available on all supported Windows versions)
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    [System.IO.Compression.ZipFile]::ExtractToDirectory($zipPath, $Dest, $true)
} catch {
    # Fallback to Expand-Archive
    Expand-Archive -Path $zipPath -DestinationPath $Dest -Force
}

# --- add to PATH ---
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($userPath -notlike "*$Dest*") {
    $newPath = "$Dest;$userPath"
    [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
    Write-Host "Added $Dest to user PATH (log out/in or restart terminal to apply)."
}

Remove-Item $zipPath -Force -ErrorAction SilentlyContinue

Write-Host "pi-server $Version installed successfully!"
Write-Host ""
Write-Host "  Binary: $Dest\pi-server.exe"
Write-Host ""
Write-Host "Run: pi-server --hostname 127.0.0.1 --port 4096"
