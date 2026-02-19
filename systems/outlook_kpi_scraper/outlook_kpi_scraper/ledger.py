import sqlite3
import os

class Ledger:
    def __init__(self):
        db_path = os.path.join(os.path.dirname(__file__), '../data/ledger.db')
        self.conn = sqlite3.connect(db_path)
        self._init_tables()

    def _init_tables(self):
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS processed_messages (
            entry_id TEXT PRIMARY KEY,
            received_dt TEXT,
            mailbox TEXT,
            folder TEXT,
            subject TEXT,
            sender TEXT
        )''')
        c.execute('''CREATE TABLE IF NOT EXISTS extracted_rows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_id TEXT,
            date TEXT,
            entity TEXT,
            revenue REAL,
            cash REAL,
            pipeline_value REAL,
            closings_count INTEGER,
            orders_count INTEGER,
            occupancy REAL,
            alerts TEXT,
            notes TEXT
        )''')
        self.conn.commit()

    def is_processed(self, entry_id):
        c = self.conn.cursor()
        c.execute('SELECT 1 FROM processed_messages WHERE entry_id=?', (entry_id,))
        return c.fetchone() is not None

    def mark_processed(self, entry_id, msg):
        c = self.conn.cursor()
        c.execute('INSERT OR IGNORE INTO processed_messages (entry_id, received_dt, mailbox, folder, subject, sender) VALUES (?, ?, ?, ?, ?, ?)',
                  (entry_id, msg.get('date'), msg.get('mailbox'), msg.get('folder'), msg.get('subject'), msg.get('sender_name')))
        self.conn.commit()
