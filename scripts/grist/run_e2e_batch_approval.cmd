@echo off
setlocal
set "SCRIPT_DIR=%~dp0"
set "REPO_ROOT=%SCRIPT_DIR%..\.."
set "PYTHONPATH=%REPO_ROOT%"
set "PULSE_RUNTIME_MODE=TEST"

if exist "%REPO_ROOT%\venv\Scripts\python.exe" (
    "%REPO_ROOT%\venv\Scripts\python.exe" "%REPO_ROOT%\scripts\grist\run_e2e_batch_approval.py" %*
) else (
    python "%REPO_ROOT%\scripts\grist\run_e2e_batch_approval.py" %*
)
