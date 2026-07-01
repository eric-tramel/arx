$ErrorActionPreference = "Stop"

$Owner = "eric-tramel"
$Repo = "arx"
$Version = if ($env:ARX_VERSION) { $env:ARX_VERSION } else { "latest" }
$InstallDir = if ($env:ARX_INSTALL_DIR) { $env:ARX_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA "Programs\arx\bin" }

$IsWindowsHost = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows)
if (-not $IsWindowsHost) {
    throw "install.ps1 supports Windows. Unix users should run install.sh."
}

$Arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString().ToLowerInvariant()
switch ($Arch) {
    "x64" { $Target = "x86_64-pc-windows-msvc" }
    default { throw "unsupported Windows architecture: $Arch. Supported: x64." }
}

$Archive = "arx-$Target.zip"
if ($Version -eq "latest") {
    $Url = "https://github.com/$Owner/$Repo/releases/latest/download/$Archive"
} else {
    $Url = "https://github.com/$Owner/$Repo/releases/download/$Version/$Archive"
}

$TempDir = Join-Path ([System.IO.Path]::GetTempPath()) ("arx-install-" + [System.Guid]::NewGuid())
New-Item -ItemType Directory -Force -Path $TempDir | Out-Null
try {
    $ArchivePath = Join-Path $TempDir $Archive
    Write-Host "Downloading $Url"
    Invoke-WebRequest -Uri $Url -OutFile $ArchivePath

    Expand-Archive -Path $ArchivePath -DestinationPath $TempDir -Force
    New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null

    $BinaryRoot = Join-Path $TempDir "arx-$Target"
    Copy-Item (Join-Path $BinaryRoot "arx.exe") (Join-Path $InstallDir "arx.exe") -Force
    Copy-Item (Join-Path $BinaryRoot "arx-mcp.exe") (Join-Path $InstallDir "arx-mcp.exe") -Force
    Copy-Item (Join-Path $BinaryRoot "arxd.exe") (Join-Path $InstallDir "arxd.exe") -Force

    Write-Host "Installed arx, arxd, and arx-mcp to $InstallDir"
    $PathEntries = [Environment]::GetEnvironmentVariable("PATH", "User") -split ";"
    if ($PathEntries -notcontains $InstallDir) {
        Write-Host "Add $InstallDir to your user PATH if it is not already available."
    }
} finally {
    Remove-Item -Recurse -Force $TempDir -ErrorAction SilentlyContinue
}
