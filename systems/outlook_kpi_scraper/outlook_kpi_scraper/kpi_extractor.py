import re

def parse_money(val):
    val = val.replace(',', '').replace('$', '').strip()
    if val.lower().endswith('k'):
        return float(val[:-1]) * 1000
    if val.lower().endswith('m'):
        return float(val[:-1]) * 1000000
    try:
        return float(val)
    except Exception:
        return None

def parse_percent(val):
    val = val.replace('%', '').strip()
    try:
        return float(val) / 100
    except Exception:
        return None

def extract_kpis(msg, entity):
    body = msg.get('body', '')
    patterns = {
        'revenue': r'Revenue[:\-]?\s*\$?([\d,\.kKmM]+)',
        'cash': r'Cash[:\-]?\s*\$?([\d,\.kKmM]+)',
        'pipeline_value': r'Pipeline[:=\-]?\s*\$?([\d,\.kKmM]+)',
        'closings_count': r'Closings[:=\-]?\s*(\d+)',
        'orders_count': r'Orders[:=\-]?\s*(\d+)',
        'occupancy': r'Occupancy[:=\-]?\s*(\d+%?)',
    }
    kpi = {'entity': entity}
    for key, pat in patterns.items():
        m = re.search(pat, body, re.IGNORECASE)
        if m:
            val = m.group(1)
            if 'count' in key:
                kpi[key] = int(val)
            elif key == 'occupancy':
                kpi[key] = parse_percent(val)
            else:
                kpi[key] = parse_money(val)
        else:
            kpi[key] = None
    kpi['date'] = msg.get('received_dt')[:10]
    kpi['alerts'] = ''
    kpi['notes'] = ''
    return kpi
