"""Entry point for the AI-Ops project."""
from ai_ops.src.core.logger import get_logger
from ai_ops.src.config.settings import settings
from ai_ops.src.services.data_loader import DataLoader
from pathlib import Path
from typing import Any
from ai_ops.src.services.sheet_normalizer import SheetNormalizer
from ai_ops.src.agents.executive_brief_agent import ExecutiveBriefAgent




def main() -> None:
    log = get_logger()
    log.info("Starting AI-Ops skeleton application")
    log.debug(f"Loaded settings: {settings}")

    loader = DataLoader()
    workbook_path = Path("data/input/master_operating_sheet.xlsx")

    log.info("Loading workbook: %s", str(workbook_path))
    try:
        sheets = loader.load_workbook(workbook_path, allow_fallback=False)

        # sheets: dict[str, pd.DataFrame] or dict[str, SimpleDataFrame] if fallback used
        log.info("Loaded workbook %s with %d sheets", str(workbook_path), len(sheets))
        print(f"Loaded workbook {workbook_path} with {len(sheets)} sheets")

        for name, df in sheets.items():
            # Determine shape and columns for both pandas and SimpleDataFrame
            if hasattr(df, "shape"):
                shape = df.shape
            elif hasattr(df, "_rows"):
                shape = (len(df._rows), len(df.columns))
            else:
                shape = (0, 0)

            rows, cols = shape[0], shape[1]
            cols_list = list(getattr(df, "columns", getattr(df, "columns", [])))

            log.info("Sheet '%s' shape=%s", name, shape)
            print(f"\nSheet: {name}")
            print(f"Rows: {rows}")
            print(f"Cols: {cols}")
            print(f"Columns: {cols_list}")

            # print first 3 rows
            print("First 3 rows:")
            try:
                head = df.head(3)
                # pandas DataFrame -> use to_string
                if hasattr(head, "to_string"):
                    print(head.to_string(index=False))
                else:
                    # SimpleDataFrame
                    print(head.to_string(index=False))
            except Exception:
                print("(preview unavailable)")

        # Normalize sheets and print derived counts
        try:
            normalizer = SheetNormalizer()
            nw = normalizer.normalize(sheets)

            print("\nNormalization summary:")
            print(f"as_of_date: {nw.as_of_date}")

            deals = nw.deals
            tasks = nw.tasks

            # Deals summary
            total_deals = len(deals) if deals is not None else 0
            dd_due_soon = int(deals['dd_due_soon'].sum()) if 'dd_due_soon' in deals.columns else 0
            dd_overdue = int(deals['dd_overdue'].sum()) if 'dd_overdue' in deals.columns else 0
            stalled_ge_14 = int((deals['days_stalled'] >= 14).sum()) if 'days_stalled' in deals.columns else 0
            print(f"\nDeals: total={total_deals}, dd_due_soon={dd_due_soon}, dd_overdue={dd_overdue}, stalled>=14={stalled_ge_14}")

            # Tasks summary
            total_tasks = len(tasks) if tasks is not None else 0
            overdue_tasks = int(tasks['is_overdue'].sum()) if 'is_overdue' in tasks.columns else 0
            blocked_tasks = int(tasks['is_blocked'].sum()) if 'is_blocked' in tasks.columns else 0
            print(f"Tasks: total={total_tasks}, overdue={overdue_tasks}, blocked={blocked_tasks}")

            # Overdue tasks grouped by owner
            if not tasks.empty and 'is_overdue' in tasks.columns and tasks['is_overdue'].any():
                owner_cols = [c for c in ['owner', 'assigned_to', 'assignee', 'owner_name'] if c in tasks.columns]
                owner_col = owner_cols[0] if owner_cols else None
                print("\nOverdue tasks by owner:")
                if owner_col:
                    grouped = tasks[tasks['is_overdue']].groupby(owner_col).size()
                    for owner, cnt in grouped.items():
                        print(f"- {owner}: {cnt}")
                else:
                    # fallback: list indexes
                    for idx, row in tasks[tasks['is_overdue']].iterrows():
                        print(f"- Task row: {idx}")

            # Deals requiring attention
            if not deals.empty and ('dd_overdue' in deals.columns or 'dd_due_soon' in deals.columns):
                attention = deals[(deals.get('dd_overdue', False)) | (deals.get('dd_due_soon', False))]
                print("\nDeals requiring attention:")
                name_cols = [c for c in ['deal_name', 'opportunity', 'account', 'name', 'client'] if c in deals.columns]
                name_col = name_cols[0] if name_cols else None
                for idx, row in attention.iterrows():
                    label = row.get(name_col) if name_col else idx
                    status = 'OVERDUE' if row.get('dd_overdue') else ('DUE_SOON' if row.get('dd_due_soon') else '')
                    days = row.get('days_to_dd')
                    print(f"- {label} | {status} | days_to_dd={days}")

            # Executive brief (deterministic)
            try:
                agent = ExecutiveBriefAgent()
                brief = agent.build(nw)

                print("\nEXECUTIVE BRIEF (DETERMINISTIC)")
                print("\n1) KPI MOVEMENT")
                if brief.kpi_movement:
                    for k, v in brief.kpi_movement.items():
                        print(f"- {k}: {v}")
                else:
                    print("(no KPI movement data)")

                print("\n2) CASH ALERTS")
                if brief.cash_alerts:
                    for a in brief.cash_alerts:
                        print(f"- {a}")
                else:
                    print("(no cash alerts)")

                print("\n3) DEALS REQUIRING ATTENTION")
                if brief.deals_requiring_attention:
                    for d in brief.deals_requiring_attention:
                        print(f"- {d}")
                else:
                    print("(no deals requiring attention)")

                print("\n4) OVERDUE/BLOCKED TASKS BY OWNER")
                print("- Overdue:")
                if brief.overdue_tasks_by_owner:
                    for owner, tasks_list in brief.overdue_tasks_by_owner.items():
                        print(f"  - {owner}: {tasks_list}")
                else:
                    print("  (none)")
                print("- Blocked:")
                if brief.blocked_tasks_by_owner:
                    for owner, tasks_list in brief.blocked_tasks_by_owner.items():
                        print(f"  - {owner}: {tasks_list}")
                else:
                    print("  (none)")

                print("\n5) TODAY'S TOP 5 PRIORITIES")
                if brief.top_priorities:
                    for p in brief.top_priorities:
                        print(f"- {p}")
                else:
                    print("(no priorities)")

            except Exception as e:
                print(f"Executive brief error: {e}")

        except Exception as e:
            print(f"Normalization error: {e}")

    except FileNotFoundError as e:
        log.error(str(e))
        print(str(e))
    except RuntimeError as e:
        # Informative error (e.g., missing pandas/openpyxl)
        log.error(str(e))
        print(str(e))
    except Exception:
        log.exception("Unexpected error while loading workbook")


if __name__ == "__main__":
    main()
