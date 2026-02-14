"""Simple data loader utilities.

Provides a thin wrapper around pandas for reading Excel files when
available. Includes a minimal .xlsx parser fallback implemented with
stdlib only so this project can run without adding dependencies.
"""
from pathlib import Path
import zipfile
import xml.etree.ElementTree as ET
from typing import List, Optional, Any, Dict, TYPE_CHECKING
from pathlib import Path

if TYPE_CHECKING:
    # For type checking only; pandas is optional at runtime
    import pandas as pd

from ai_ops.src.core.logger import get_logger

log = get_logger(__name__)


class SimpleDataFrame:
    """A very small DataFrame-like wrapper for basic inspection.

    Supports: `shape`, `columns`, `head(n) -> SimpleDataFrame`, and
    `to_string(index=False)` for printing.
    """

    def __init__(self, columns: List[str], rows: List[List[Any]]):
        self.columns = columns
        self._rows = rows

    @property
    def shape(self):
        return (len(self._rows), len(self.columns))

    def head(self, n: int = 5) -> "SimpleDataFrame":
        return SimpleDataFrame(self.columns, self._rows[:n])

    def to_string(self, index: bool = False) -> str:
        # Simple column-aligned formatter
        cols = self.columns
        rows = self._rows
        widths = [len(str(c)) for c in cols]
        for r in rows[:3]:
            for i, v in enumerate(r):
                widths[i] = max(widths[i], len(str(v)))

        header = "  ".join(c.ljust(widths[i]) for i, c in enumerate(cols))
        lines = [header]
        for r in rows[:3]:
            lines.append("  ".join(str(v).ljust(widths[i]) for i, v in enumerate(r)))
        return "\n".join(lines)


def _col_letters_to_index(letters: str) -> int:
    # A -> 0, B -> 1, Z -> 25, AA -> 26, etc.
    result = 0
    for ch in letters:
        result = result * 26 + (ord(ch.upper()) - ord("A")) + 1
    return result - 1


def _parse_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    try:
        data = zf.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    root = ET.fromstring(data)
    strings: List[str] = []
    for si in root.findall(".{http://schemas.openxmlformats.org/spreadsheetml/2006/main}si"):
        # concatenate all text nodes under si
        text_parts: List[str] = []
        for t in si.iter():
            if t.text:
                text_parts.append(t.text)
        strings.append("".join(text_parts))
    return strings


def _parse_sheet(zf: zipfile.ZipFile, sheet_name: Optional[str] = None) -> List[List[str]]:
    # Choose first worksheet if not provided
    candidates = [n for n in zf.namelist() if n.startswith("xl/worksheets/sheet")]
    if not candidates:
        raise ValueError("No worksheets found in .xlsx file")
    sheet_path = sheet_name or candidates[0]
    data = zf.read(sheet_path)
    root = ET.fromstring(data)
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

    rows: List[List[str]] = []
    max_cols = 0
    for row in root.findall(f"./{ns}sheetData/{ns}row"):
        cells = []
        cell_map = {}
        for c in row.findall(f"{ns}c"):
            ref = c.attrib.get("r")  # e.g., A1
            if ref is None:
                continue
            # split letters from numbers
            letters = ''.join([ch for ch in ref if ch.isalpha()])
            idx = _col_letters_to_index(letters)
            v = c.find(f"{ns}v")
            cell_map[idx] = v.text if v is not None else ""
        if cell_map:
            max_col = max(cell_map.keys())
            if max_col + 1 > max_cols:
                max_cols = max_col + 1
            row_list = [cell_map.get(i, "") for i in range(max_cols)]
        else:
            row_list = []
        rows.append(row_list)

    # normalize row lengths
    for r in rows:
        if len(r) < max_cols:
            r.extend([""] * (max_cols - len(r)))
    return rows


class DataLoader:
    def load_excel(self, path: str) -> Any:
        """Backward-compatible single-sheet loader (keeps existing behavior)."""
        return self.load_workbook(Path(path), allow_fallback=True)

    def load_workbook(self, path: Path, allow_fallback: bool = False) -> Dict[str, "pd.DataFrame"]:
        """Load all sheets from a workbook as pandas DataFrames.

        Primary behavior: use pandas + openpyxl. If `allow_fallback=True` and the
        primary path is unavailable (pandas/openpyxl missing), attempt the
        minimal stdlib fallback. The fallback returns SimpleDataFrame objects if
        pandas is not present.
        """
        log.info("Starting to load workbook: %s", str(path))

        if not path.exists():
            raise FileNotFoundError(f"Workbook not found: {path}")

        # Try primary path: pandas + openpyxl
        try:
            import pandas as pd  # type: ignore
            try:
                # ensure openpyxl is importable
                import openpyxl  # type: ignore
            except Exception as e:
                raise RuntimeError("openpyxl is required to read .xlsx files: pip install openpyxl") from e

            try:
                sheets = pd.read_excel(path, sheet_name=None, engine="openpyxl")
            except Exception as e:
                raise RuntimeError(f"Failed to read workbook with pandas/openpyxl: {e}") from e

            if not isinstance(sheets, dict):
                raise RuntimeError("Unexpected result from pandas.read_excel; expected dict of DataFrames")

            log.info("Loaded workbook %s with %d sheets", str(path), len(sheets))
            for name, df in sheets.items():
                try:
                    log.info("Loaded sheet '%s' shape=%s", name, df.shape)
                except Exception:
                    log.info("Loaded sheet '%s' (shape unknown)", name)

            return sheets
        except ImportError:
            # pandas not installed
            if not allow_fallback:
                raise RuntimeError("pandas and openpyxl are required; install with: pip install pandas openpyxl")
            log.info("pandas not installed; attempting fallback parser because allow_fallback=True")

        except RuntimeError:
            # propagate runtime errors from openpyxl/read_excel
            raise

        # Fallback path: attempt minimal stdlib parsing for each sheet file
        if allow_fallback:
            results: Dict[str, Any] = {}
            with zipfile.ZipFile(path, "r") as zf:
                shared = _parse_shared_strings(zf)
                # find all worksheet files
                candidates = [n for n in zf.namelist() if n.startswith("xl/worksheets/sheet")]
                for idx, sheet_path in enumerate(candidates, start=1):
                    rows = _parse_sheet(zf, sheet_name=sheet_path)
                    # resolve shared strings
                    def _maybe_resolve(val: Optional[str]) -> str:
                        if val is None:
                            return ""
                        if val.isdigit():
                            i = int(val)
                            if 0 <= i < len(shared):
                                return shared[i]
                        return val

                    resolved = [[_maybe_resolve(v) for v in row] for row in rows]
                    if not resolved:
                        headers: List[str] = []
                        data_rows: List[List[str]] = []
                    else:
                        headers = [c if c != "" else f"col_{i}" for i, c in enumerate(resolved[0])]
                        data_rows = resolved[1:]

                    sdf = SimpleDataFrame(headers, data_rows)
                    # Use sheet filename as sheet name (best-effort)
                    sheet_name = Path(sheet_path).stem
                    results[sheet_name] = sdf
                    log.info("Fallback loaded sheet '%s' rows=%d cols=%d", sheet_name, len(data_rows), len(headers))
            # Return dict of SimpleDataFrame (not pandas) when pandas is absent
            return results
        # Should not reach here
        raise RuntimeError("Unable to load workbook: unknown error")
