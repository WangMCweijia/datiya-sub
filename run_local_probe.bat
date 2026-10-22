@echo off
rem Local real-proxy probe for datiya-sub (C tier).
rem Downloads mihomo on first run, then probes every candidate through the kernel
rem and pushes the resulting whitelist.json back to GitHub.
rem Usage:  run_local_probe.bat            (one round, push to GitHub)
rem         run_local_probe.bat --loop 1800   (resident, every 30 minutes)
setlocal enabledelayedexpansion
chcp 65001 >nul
cd /d "%~dp0"

set "PY=python"
where python >nul 2>nul || set "PY=py -3"

if not exist "tools\mihomo.exe" (
  echo [1/3] downloading mihomo core ...
  if not exist "tools" mkdir "tools"
  curl -fsSL -o "tools\mihomo.zip" "https://ghfast.top/https://github.com/MetaCubeX/mihomo/releases/download/v1.19.31/mihomo-windows-amd64-v1.19.31.zip"
  if errorlevel 1 (
    echo [x] download failed. Download it manually into tools\mihomo.exe and rerun.
    exit /b 1
  )
  powershell -NoProfile -Command "Expand-Archive -Force 'tools\mihomo.zip' 'tools'"
  if exist "tools\mihomo-windows-amd64.exe" move /y "tools\mihomo-windows-amd64.exe" "tools\mihomo.exe" >nul
  del /q "tools\mihomo.zip" 2>nul
) else (
  echo [1/3] mihomo core found.
)

echo [2/3] checking python deps ...
%PY% -c "import yaml, requests" 2>nul || %PY% -m pip install -q pyyaml requests

echo [3/3] probing candidates through the kernel ...
%PY% local_probe.py --push %*
set "RC=%ERRORLEVEL%"
echo done. exit=%RC%
exit /b %RC%
