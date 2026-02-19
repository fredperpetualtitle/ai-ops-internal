Write-Host "=== START: Outlook 7-Day Scrape (Debug) ==="
cd $PSScriptRoot
cd ..
.\.venv\Scripts\Activate.ps1
python -m outlook_kpi_scraper.run --days 7 --mailbox "Chip Ridge" --folder "Inbox" --max 250 --debug
Write-Host "=== END: Outlook 7-Day Scrape (Debug) ==="