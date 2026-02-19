Write-Host "=== START: Outlook 1-Day Scrape ==="
cd $PSScriptRoot
cd ..
.\.venv\Scripts\Activate.ps1
python -m outlook_kpi_scraper.run --days 1 --mailbox "Chip Ridge" --folder "Inbox" --max 25 --debug
Write-Host "=== END: Outlook 1-Day Scrape ==="
