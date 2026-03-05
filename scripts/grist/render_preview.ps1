param(
    [string]$TestDocId = "tSFZW3ybtD46ug2q76iMML"
)

$ErrorActionPreference = "Stop"

$env:PYTHONPATH = "."
$env:PULSE_RUNTIME_MODE = "TEST"
$env:PULSE_TEST_DOC_ID = $TestDocId

python scripts/grist/render_test_outbox_preview.py

Write-Host "Open: artifacts/test_preview/outbox_preview.html"
