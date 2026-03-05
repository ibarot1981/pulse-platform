@echo off
setlocal
set PYTHONPATH=.
set PULSE_RUNTIME_MODE=TEST
if "%~1"=="" (
  set PULSE_TEST_DOC_ID=tSFZW3ybtD46ug2q76iMML
) else (
  set PULSE_TEST_DOC_ID=%~1
)
python scripts\grist\render_test_outbox_preview.py
echo Open: artifacts\test_preview\outbox_preview.html
