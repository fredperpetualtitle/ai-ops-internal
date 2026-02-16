"""Entry point for the AI-Ops project."""
from ai_ops.src.core.logger import get_logger
from ai_ops.src.config.settings import settings
from ai_ops.src.services.data_loader import DataLoader
from pathlib import Path
from typing import Any
from ai_ops.src.services.sheet_normalizer import SheetNormalizer
from ai_ops.src.agents.executive_brief_agent import ExecutiveBriefAgent
import json
from datetime import date, datetime




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

                # Persist executive brief (JSON + Markdown)
                try:
                    out_dir = Path("data/output")
                    out_dir.mkdir(parents=True, exist_ok=True)

                    def _to_serializable(o):
                        if isinstance(o, (date, datetime)):
                            return o.isoformat()
                        try:
                            import numpy as _np
                            if isinstance(o, (_np.integer, _np.floating)):
                                return float(o)
                        except Exception:
                            pass
                        return o

                    brief_obj = {
                        "as_of_date": nw.as_of_date.isoformat() if isinstance(nw.as_of_date, date) else str(nw.as_of_date),
                        "kpi_movement_by_entity": brief.kpi_movement,
                        "cash_alerts": brief.cash_alerts,
                        "deals_requiring_attention": brief.deals_requiring_attention,
                        "overdue_tasks_by_owner": brief.overdue_tasks_by_owner,
                        "blocked_tasks_by_owner": brief.blocked_tasks_by_owner,
                        "top_priorities": brief.top_priorities,
                    }

                    log.info("Writing executive brief to data/output/...")

                    latest_path = out_dir / "brief_latest.json"
                    dated_name = f"brief_{brief_obj['as_of_date']}.json"
                    dated_path = out_dir / dated_name

                    with open(latest_path, "w", encoding="utf-8") as f:
                        json.dump(brief_obj, f, indent=2, ensure_ascii=False, default=_to_serializable)
                    with open(dated_path, "w", encoding="utf-8") as f:
                        json.dump(brief_obj, f, indent=2, ensure_ascii=False, default=_to_serializable)

                    # Build markdown
                    md_lines = []
                    md_lines.append(f"Executive Brief — {brief_obj['as_of_date']}")
                    md_lines.append("")
                    md_lines.append("## KPI Movement")

                    kpi = brief.kpi_movement if brief.kpi_movement else {}
                    if not kpi:
                        md_lines.append("None")
                    else:
                        for ent, val in kpi.items():
                            if isinstance(val, str):
                                md_lines.append(f"- {ent}: {val}")
                                continue
                            md_lines.append(f"- {ent}:")
                            for field, metric in val.items():
                                if metric is None:
                                    md_lines.append(f"  - {field}: None")
                                    continue
                                prior = metric.get("prior")
                                latest = metric.get("latest")
                                delta = metric.get("delta")
                                def _fmt(n):
                                    try:
                                        return f"{n:,.0f}"
                                    except Exception:
                                        return str(n)
                                sign = "+" if (isinstance(delta, (int, float)) and delta > 0) else ""
                                md_lines.append(f"  - {field}: { _fmt(prior) } -> { _fmt(latest) } (delta {sign}{ _fmt(delta) })")

                    md_lines.append("")
                    md_lines.append("## Cash Alerts")
                    if brief.cash_alerts:
                        for a in brief.cash_alerts:
                            md_lines.append(f"- {a}")
                    else:
                        md_lines.append("None")

                    md_lines.append("")
                    md_lines.append("## Deals Requiring Attention")
                    if brief.deals_requiring_attention:
                        for d in brief.deals_requiring_attention:
                            md_lines.append(f"- {d}")
                    else:
                        md_lines.append("None")

                    md_lines.append("")
                    md_lines.append("## Blocked Tasks")
                    if brief.blocked_tasks_by_owner:
                        for owner, tasks_list in brief.blocked_tasks_by_owner.items():
                            md_lines.append(f"- {owner}: {', '.join(tasks_list)}")
                    else:
                        md_lines.append("None")

                    md_lines.append("")
                    md_lines.append("## Top Priorities")
                    if brief.top_priorities:
                        for i, item in enumerate(brief.top_priorities, start=1):
                            md_lines.append(f"{i}. {item}")
                    else:
                        md_lines.append("None")

                    md_path = out_dir / f"executive_brief_{brief_obj['as_of_date']}.md"
                    with open(md_path, "w", encoding="utf-8") as f:
                        f.write("\n".join(md_lines))

                    log.info("Executive brief written successfully")
                except Exception as e:
                    log.exception("Failed to write executive brief: %s", e)

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
