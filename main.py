"""Entry point for the AI-Ops project."""
from ai_ops.src.core.logger import get_logger
from ai_ops.src.config.settings import settings
from ai_ops.src.services.data_loader import DataLoader
from ai_ops.src.integrations.spreadsheet_client import SpreadsheetClient
from pathlib import Path
from typing import Any
from ai_ops.src.services.sheet_normalizer import SheetNormalizer
from ai_ops.src.agents.executive_brief_agent import ExecutiveBriefAgent
from ai_ops.src.agents.deal_risk_agent import DealRiskAgent
from ai_ops.src.agents.accountability_agent import AccountabilityAgent
from ai_ops.src.services.narrative_composer import compose_narrative
from ai_ops.src.core.run_report import RunReport, InputsUsed
from ai_ops.src.services.run_report_renderer import render_run_report_md
from ai_ops.src.services.operator_brief_generator import generate_operator_brief_markdown
from ai_ops.src.services.deal_risk_renderer import render_deal_risk_memo_md
from ai_ops.src.services.accountability_renderer import render_accountability_report_md
import json
import os
from datetime import date, datetime
import uuid
import time


def _load_sheets_from_env(log) -> dict:
    """Load sheet data via SpreadsheetClient (Google Sheets or Excel backend).

    Uses SHEETS_BACKEND env var to decide the backend.  Returns the same
    dict[str, pd.DataFrame] structure that the rest of the pipeline expects.
    """
    client = SpreadsheetClient.from_env()
    sheets = client.get_all_tabs()
    log.info("SpreadsheetClient returned %d sheets", len(sheets))
    return sheets, client


def main() -> None:
    # Capture run timing
    run_start_time = time.time()
    started_at_iso = datetime.now().isoformat()
    run_id = datetime.now().strftime("%Y%m%d%H%M%S")  # Use timestamp-based run_id
    
    log = get_logger()
    log.info("Starting AI-Ops skeleton application")
    log.debug(f"Loaded settings: {settings}")

    loader = DataLoader()
    workbook_path = Path("data/input/master_operating_sheet.xlsx")

    # --- Data source selection ------------------------------------------------
    # SHEETS_BACKEND=google  -> read live from Google Sheets via SpreadsheetClient
    # SHEETS_BACKEND=excel   -> read local .xlsx (original behaviour)
    # If the env var is unset *and* the local workbook exists, fall back to excel
    # so we don't break existing local-dev workflows.
    backend = os.getenv("SHEETS_BACKEND", "").strip().lower()
    use_google = backend == "google" or (
        backend not in ("excel",) and not workbook_path.exists()
    )

    log.info("Loading workbook: %s", "Google Sheets" if use_google else str(workbook_path))
    try:
        if use_google:
            # -- Google Sheets (or SpreadsheetClient-based) path ---------------
            sheets, _client = _load_sheets_from_env(log)
            data_source_label = f"Google Sheets ({_client._backend.sheet_id})" if hasattr(_client._backend, "sheet_id") else "SpreadsheetClient"
        else:
            # -- Legacy local .xlsx path ---------------------------------------
            sheets = loader.load_workbook(workbook_path, allow_fallback=False)
            data_source_label = str(workbook_path)

        # sheets: dict[str, pd.DataFrame] or dict[str, SimpleDataFrame] if fallback used
        log.info("Loaded %s with %d sheets", data_source_label, len(sheets))
        print(f"Loaded {data_source_label} with {len(sheets)} sheets")

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

            # Capture inputs used for RunReport
            sheet_names = list(sheets.keys())
            row_counts = {}
            for name, df in sheets.items():
                if hasattr(df, "shape"):
                    row_counts[name] = df.shape[0]
                elif hasattr(df, "_rows"):
                    row_counts[name] = len(df._rows)
                else:
                    row_counts[name] = 0

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

                    # Create a per-run output directory so each run's artifacts
                    # are grouped. Keep latest pointers at data/output/ for
                    # convenience.
                    run_dir = out_dir / f"run_{run_id}"
                    run_dir.mkdir(parents=True, exist_ok=True)

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
                    dated_path = run_dir / dated_name

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

                    # Compose LLM narrative (if enabled) and append to markdown
                    # compose_narrative now returns (narrative_text, error_str)
                    llm_error = None
                    narrative_text, narrative_err = compose_narrative(brief_obj)
                    if narrative_err:
                        # Record the first LLM-related error and avoid further LLM calls
                        llm_error = str(narrative_err)
                    if narrative_text:
                        md_lines.append("")
                        md_lines.append("## Narrative (LLM)")
                        md_lines.append(narrative_text)

                    # BUILD RUNREPORT EARLY so we can generate deterministic brief
                    run_end_time = time.time()
                    finished_at_iso = datetime.now().isoformat()
                    duration_ms = int((run_end_time - run_start_time) * 1000)

                    # Build summary counts
                    summary_counts = {
                        "deals_total": total_deals,
                        "deals_dd_overdue": dd_overdue,
                        "deals_dd_due_soon": dd_due_soon,
                        "deals_stalled_ge_14": stalled_ge_14,
                        "tasks_total": total_tasks,
                        "tasks_overdue": overdue_tasks,
                        "tasks_blocked": blocked_tasks,
                    }

                    # Construct inputs_used
                    inputs_used = InputsUsed(
                        workbook_path=data_source_label,
                        sheet_names=sheet_names,
                        row_counts=row_counts,
                    )

                    # Build RunReport (does not include brief JSON paths yet)
                    run_report = RunReport(
                        run_id=run_id,
                        started_at=started_at_iso,
                        finished_at=finished_at_iso,
                        duration_ms=duration_ms,
                        as_of_date=brief_obj['as_of_date'],
                        inputs_used=inputs_used,
                        output_paths=[],  # Will update later
                        summary_counts=summary_counts,
                        reasoning_trace=brief.reasoning_trace,
                        confidence_flags=brief.confidence_flags,
                        errors=[],
                        retries=0,
                    )

                    # Generate deterministic operator brief from RunReport
                    operator_md, operator_err = generate_operator_brief_markdown(run_report)
                    brief_write_paths = [str(latest_path), str(dated_path)]
                    if operator_err:
                        llm_error = operator_err
                        log.warning("Operator brief generation error: %s", operator_err)
                    else:
                        # Use operator brief as main executive brief (deterministic, no LLM)
                        if operator_md:
                            md_path = run_dir / f"executive_brief_{brief_obj['as_of_date']}.md"
                            md_latest = out_dir / "executive_brief_latest.md"
                            with open(md_path, "w", encoding="utf-8") as f:
                                f.write(operator_md)
                            with open(md_latest, "w", encoding="utf-8") as f:
                                f.write(operator_md)
                            brief_write_paths.append(str(md_path))
                            log.info(f"Executive brief (deterministic) written to {md_latest}")
                        else:
                            # Fallback to manual markdown if operator brief generation failed
                            md_path = run_dir / f"executive_brief_{brief_obj['as_of_date']}.md"
                            with open(md_path, "w", encoding="utf-8") as f:
                                f.write("\n".join(md_lines))
                            brief_write_paths.append(str(md_path))

                    # Update output paths to include brief JSON/MD files
                    run_report.output_paths = brief_write_paths

                    # Render Markdown version of RunReport
                    run_report_md = render_run_report_md(run_report)
                    run_report_latest_md_path = out_dir / "run_report_latest.md"
                    run_report_dated_md_path = run_dir / f"run_report_{run_id}.md"

                    with open(run_report_latest_md_path, "w", encoding="utf-8") as f:
                        f.write(run_report_md)
                    with open(run_report_dated_md_path, "w", encoding="utf-8") as f:
                        f.write(run_report_md)

                    log.info(f"Run Report Markdown written to {run_report_latest_md_path} and {run_report_dated_md_path}")
                    print(f"Run Report Markdown written to {run_report_latest_md_path}")

                    # If any error occurred, record it once
                    if llm_error and llm_error not in run_report.errors:
                        run_report.errors.append(llm_error)
                        run_report.confidence_flags.append(f"LLM_UNAVAILABLE: {llm_error}")

                    # Persist RunReport (after possibly recording operator errors)
                    run_latest_path = out_dir / "run_latest.json"
                    run_dated_path = run_dir / f"run_{run_id}.json"

                    with open(run_latest_path, "w", encoding="utf-8") as f:
                        f.write(run_report.to_json_str(indent=2))
                    with open(run_dated_path, "w", encoding="utf-8") as f:
                        f.write(run_report.to_json_str(indent=2))

                    log.info(f"RunReport written to {run_latest_path} and {run_dated_path}")
                    print(f"\nRunReport written to {run_latest_path}")

                    log.info("Executive brief written successfully")
                
                except Exception as e:
                    log.exception("Failed to write executive brief: %s", e)

            except Exception as e:
                print(f"Executive brief error: {e}")

            # ── Agent 2 — Deal Risk & Closing Monitor ──────────────────────
            try:
                deal_risk_agent = DealRiskAgent(today=nw.as_of_date)
                deal_risk_memo = deal_risk_agent.run(nw)

                print(f"\nDEAL RISK MEMO (Agent 2)")
                print(f"Deals scored: {deal_risk_memo.summary['total_deals']}")
                print(f"RED: {deal_risk_memo.summary['red']}  YELLOW: {deal_risk_memo.summary['yellow']}  GREEN: {deal_risk_memo.summary['green']}")

                for d in deal_risk_memo.deals:
                    if d.risk_level in ("RED", "YELLOW"):
                        print(f"  {d.risk_level}: {d.deal_name} (score {d.risk_score}) — {', '.join(d.risk_drivers[:3])}")

                # Persist Agent 2 outputs
                try:
                    out_dir = Path("data/output")
                    run_dir = out_dir / f"run_{run_id}"
                    run_dir.mkdir(parents=True, exist_ok=True)

                    memo_json_path = run_dir / f"deal_risk_memo_{run_id}.json"
                    memo_md_path = run_dir / f"deal_risk_memo_{run_id}.md"
                    memo_latest_json = out_dir / "deal_risk_memo_latest.json"
                    memo_latest_md = out_dir / "deal_risk_memo_latest.md"

                    memo_json_str = json.dumps(deal_risk_memo.to_dict(), indent=2, ensure_ascii=False, default=str)
                    memo_md_str = render_deal_risk_memo_md(deal_risk_memo)

                    for path in (memo_json_path, memo_latest_json):
                        with open(path, "w", encoding="utf-8") as f:
                            f.write(memo_json_str)
                    for path in (memo_md_path, memo_latest_md):
                        with open(path, "w", encoding="utf-8") as f:
                            f.write(memo_md_str)

                    log.info("Agent 2 outputs written to %s", run_dir)
                    print(f"Deal Risk Memo written to {memo_latest_md}")

                except Exception as e:
                    log.exception("Failed to write Agent 2 outputs: %s", e)

            except Exception as e:
                print(f"Agent 2 (Deal Risk) error: {e}")
                log.exception("Agent 2 error: %s", e)

            # ── Agent 3 — Accountability & Follow-Up Engine ────────────────
            try:
                accountability_agent = AccountabilityAgent(today=nw.as_of_date)
                accountability_report = accountability_agent.run(nw)

                print(f"\nACCOUNTABILITY REPORT (Agent 3)")
                print(f"Tasks scored: {accountability_report.system_summary['total_tasks']}")
                print(f"Overdue: {accountability_report.system_summary['overdue']}  Blocked: {accountability_report.system_summary['blocked']}")

                for o in accountability_report.owners:
                    if o.risk_level in ("RED", "YELLOW"):
                        print(f"  {o.risk_level}: {o.owner} (score {o.score}) — overdue={o.overdue}, blocked={o.blocked}")

                if accountability_report.follow_up_drafts:
                    print(f"  Follow-up drafts: {len(accountability_report.follow_up_drafts)}")

                # Persist Agent 3 outputs
                try:
                    out_dir = Path("data/output")
                    run_dir = out_dir / f"run_{run_id}"
                    run_dir.mkdir(parents=True, exist_ok=True)

                    acct_json_path = run_dir / f"accountability_report_{run_id}.json"
                    acct_md_path = run_dir / f"accountability_report_{run_id}.md"
                    acct_latest_json = out_dir / "accountability_report_latest.json"
                    acct_latest_md = out_dir / "accountability_report_latest.md"

                    acct_json_str = json.dumps(accountability_report.to_dict(), indent=2, ensure_ascii=False, default=str)
                    acct_md_str = render_accountability_report_md(accountability_report)

                    for path in (acct_json_path, acct_latest_json):
                        with open(path, "w", encoding="utf-8") as f:
                            f.write(acct_json_str)
                    for path in (acct_md_path, acct_latest_md):
                        with open(path, "w", encoding="utf-8") as f:
                            f.write(acct_md_str)

                    log.info("Agent 3 outputs written to %s", run_dir)
                    print(f"Accountability Report written to {acct_latest_md}")

                except Exception as e:
                    log.exception("Failed to write Agent 3 outputs: %s", e)

            except Exception as e:
                print(f"Agent 3 (Accountability) error: {e}")
                log.exception("Agent 3 error: %s", e)

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
