"""
Minimal agent for daily executive brief.
"""
import datetime

def generate_brief(kpi_rows, task_rows=None):
    today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    kpi_highlights = []
    for row in kpi_rows[:5]:
        kpi_highlights.append(f"{row}")
    priorities = ["No task data"] if not task_rows else [t['Task'] for t in task_rows[:3]]
    brief = {
        'date': today,
        'priorities': priorities,
        'kpi_highlights': kpi_highlights,
        'risks': [r for r in kpi_rows if 'Risk Flag' in r]
    }
    return brief
