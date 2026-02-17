"""
Logger for run logs to Google Sheets.
"""
import datetime

def log_run(connector, run_id, status, sheets_read, rows_read, output_written_to, error_message):
    timestamp = datetime.datetime.utcnow().isoformat()
    row = [run_id, timestamp, status, ','.join(sheets_read), rows_read, output_written_to, error_message or '']
    connector.write_row('Run_Log', row)
