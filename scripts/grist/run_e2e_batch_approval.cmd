@echo off
setlocal
set PYTHONPATH=.
set PULSE_RUNTIME_MODE=TEST
python scripts\grist\run_e2e_batch_approval.py %*
