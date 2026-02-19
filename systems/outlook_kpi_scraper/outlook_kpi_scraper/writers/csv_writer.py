import os
import csv

class CSVWriter:
    def __init__(self):
        out_dir = os.path.join(os.path.dirname(__file__), '../../data/output')
        os.makedirs(out_dir, exist_ok=True)
        self.csv_path = os.path.join(out_dir, 'latest_rows.csv')

    def append_row(self, row):
        header = ['date','entity','revenue','cash','pipeline_value','closings_count','orders_count','occupancy','alerts','notes']
        write_header = not os.path.exists(self.csv_path)
        with open(self.csv_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=header)
            if write_header:
                writer.writeheader()
            writer.writerow({col: row.get(col) for col in header})
        return True
