"""
Main runner for daily brief generation.
"""
import os
import uuid
from sheets_connector import SheetsConnector
from agent_exec_brief import generate_brief
from logger import log_run

def main():
    run_id = str(uuid.uuid4())
    connector = SheetsConnector()
    error_message = ''
    status = 'success'
    sheets_read = []
    rows_read = 0
    output_written_to = ''
    try:
        kpi_rows, kpi_err = connector.read_tab('KPI_Snapshot')
        sheets_read.append('KPI_Snapshot')
        if kpi_err:
            error_message += f"KPI_Snapshot error: {kpi_err}\n"
            status = 'fail'
            kpi_rows = []
        task_rows, task_err = connector.read_tab('Tasks')
        sheets_read.append('Tasks')
        if task_err:
            error_message += f"Tasks error: {task_err}\n"
            task_rows = []
        rows_read = len(kpi_rows) + (len(task_rows) if task_rows else 0)
        brief = generate_brief(kpi_rows, task_rows)
        output_row = [brief['date'], ', '.join(brief['priorities']), ', '.join([str(k) for k in brief['kpi_highlights']]), ', '.join(brief['risks'])]
        ok, write_err = connector.write_row('Daily_Briefs', output_row)
        output_written_to = 'Daily_Briefs'
        if not ok:
            error_message += f"Output write error: {write_err}\n"
            status = 'fail'
    except Exception as e:
        error_message += str(e)
        status = 'fail'
    log_run(connector, run_id, status, sheets_read, rows_read, output_written_to, error_message)

if __name__ == '__main__':
    main()
