Write-Host "=== START: Google Sheets Smoke Test ==="
cd $PSScriptRoot
.\.venv\Scripts\Activate.ps1
python .\smoke_test_sheets.py
Write-Host "=== END: Google Sheets Smoke Test ==="
