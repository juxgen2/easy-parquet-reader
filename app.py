"""
Easy Parquet Reader – a local Flask app for exploring tabular files.
"""

import io
import re
import uuid
from typing import Any

import pandas as pd
from flask import Flask, redirect, render_template, request, url_for

# ---------------------------------------------------------------------------
# App configuration
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200 MB

# In-memory store: dataset_id -> {"df": pd.DataFrame, "filename": str}
_STORE: dict[str, dict[str, Any]] = {}

ACCEPTED_EXTENSIONS = {".parquet", ".csv", ".xlsx", ".xls", ".json", ".jsonl", ".feather", ".pkl"}

# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------


def load_dataframe(file_storage) -> pd.DataFrame:
    """Read a FileStorage object into a pandas DataFrame based on its extension."""
    filename: str = file_storage.filename or ""
    name_lower = filename.lower()

    data = file_storage.read()
    buf = io.BytesIO(data)

    if name_lower.endswith(".parquet"):
        return pd.read_parquet(buf, engine="pyarrow")
    if name_lower.endswith(".csv"):
        for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                buf.seek(0)
                return pd.read_csv(buf, encoding=enc, low_memory=False)
            except UnicodeDecodeError:
                continue
        buf.seek(0)
        return pd.read_csv(buf, encoding="latin-1", low_memory=False)
    if name_lower.endswith((".xlsx", ".xls")):
        return pd.read_excel(buf)
    if name_lower.endswith(".jsonl"):
        buf.seek(0)
        return pd.read_json(buf, lines=True)
    if name_lower.endswith(".json"):
        buf.seek(0)
        return pd.read_json(buf)
    if name_lower.endswith(".feather"):
        return pd.read_feather(buf)
    if name_lower.endswith(".pkl"):
        # Pickle deserialization is only safe with files you personally created.
        # This app is local-only; do not open .pkl files from untrusted sources.
        return pd.read_pickle(buf)  # noqa: S301
    raise ValueError(f"Unsupported file extension: {filename!r}")


def infer_column_filters(df: pd.DataFrame) -> dict[str, dict]:
    """
    Return metadata per column to drive filter widgets.
    Each entry: {"type": "select"|"text"|"number"|"date", "options": [...] or None}
    """
    result = {}
    for col in df.columns:
        series = df[col]
        col_dtype = series.dtype

        # Detect date columns
        if pd.api.types.is_datetime64_any_dtype(col_dtype):
            result[col] = {"type": "date", "options": None}
            continue

        # Numeric columns
        if pd.api.types.is_numeric_dtype(col_dtype):
            n_unique = series.nunique(dropna=True)
            if n_unique <= 50:
                opts = sorted(series.dropna().unique().tolist())
                result[col] = {"type": "select", "options": [str(o) for o in opts]}
            else:
                result[col] = {"type": "number", "options": None}
            continue

        # Text / object columns
        n_unique = series.nunique(dropna=True)
        if n_unique <= 50:
            opts = sorted(series.dropna().astype(str).unique().tolist())
            result[col] = {"type": "select", "options": opts}
        else:
            result[col] = {"type": "text", "options": None}

    return result


def apply_filters(df: pd.DataFrame, filters: dict[str, str], col_meta: dict[str, dict]) -> pd.DataFrame:
    """Apply column filters to a DataFrame and return the filtered copy."""
    mask = pd.Series([True] * len(df), index=df.index)

    for col, value in filters.items():
        if not value or col not in df.columns:
            continue
        value = value.strip()
        if not value:
            continue

        meta = col_meta.get(col, {})
        ftype = meta.get("type", "text")

        if ftype == "select":
            mask &= df[col].astype(str) == value
        elif ftype == "number":
            m = re.match(r"^\s*(>=|<=|>|<|=|!=)?\s*(-?\d+(?:\.\d+)?)\s*$", value)
            if m:
                op, num_str = m.group(1) or "=", float(m.group(2))
                ops = {">": "__gt__", "<": "__lt__", ">=": "__ge__", "<=": "__le__", "=": "__eq__", "!=": "__ne__"}
                mask &= getattr(df[col], ops[op])(num_str)
        elif ftype == "date":
            mask &= df[col].astype(str).str.contains(value, case=False, na=False)
        else:
            # text – case-insensitive contains
            mask &= df[col].astype(str).str.contains(value, case=False, na=False, regex=False)

    return df[mask]


def paginate_dataframe(df: pd.DataFrame, page: int, page_size: int) -> tuple[pd.DataFrame, int, int]:
    """
    Return (page_df, total_pages, total_rows).
    page is 1-based.
    """
    total_rows = len(df)
    total_pages = max(1, (total_rows + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    return df.iloc[start:end], total_pages, total_rows


def safe_values(df: pd.DataFrame) -> list[list[str]]:
    """Convert DataFrame rows to lists of HTML-safe strings."""
    rows = []
    for _, row in df.iterrows():
        cells = []
        for val in row:
            if pd.isna(val) if not isinstance(val, (list, dict)) else False:
                cells.append("")
            else:
                cells.append(str(val))
        rows.append(cells)
    return rows


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    file = request.files.get("file")
    if not file or file.filename == "":
        return render_template("index.html", error="No file selected.")

    filename: str = file.filename or ""
    ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in ACCEPTED_EXTENSIONS:
        return render_template("index.html", error=f"Unsupported file type '{ext}'. Accepted: {', '.join(sorted(ACCEPTED_EXTENSIONS))}")

    try:
        df = load_dataframe(file)
    except Exception as exc:
        return render_template("index.html", error=f"Could not read file: {exc}")

    if df.empty:
        return render_template("index.html", error="The file appears to be empty.")

    # Deduplicate column names
    seen: dict[str, int] = {}
    new_cols = []
    for c in df.columns:
        c_str = str(c)
        if c_str in seen:
            seen[c_str] += 1
            new_cols.append(f"{c_str}_{seen[c_str]}")
        else:
            seen[c_str] = 0
            new_cols.append(c_str)
    df.columns = new_cols

    dataset_id = str(uuid.uuid4())
    _STORE[dataset_id] = {"df": df, "filename": filename}

    return redirect(url_for("dataset", dataset_id=dataset_id))


@app.route("/dataset/<dataset_id>")
def dataset(dataset_id: str):
    entry = _STORE.get(dataset_id)
    if not entry:
        return render_template("index.html", error="Dataset not found or expired. Please upload again.")

    df: pd.DataFrame = entry["df"]
    filename: str = entry["filename"]
    col_meta = infer_column_filters(df)

    # Query params
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 100))
    if page_size not in (25, 50, 100, 250, 500, 1000):
        page_size = 100

    # Active filters
    filters: dict[str, str] = {}
    for col in df.columns:
        val = request.args.get(f"filter_{col}", "")
        if val:
            filters[col] = val

    # Visible columns
    visible_cols = request.args.getlist("cols")
    if not visible_cols:
        visible_cols = list(df.columns)
    # Ensure they exist
    visible_cols = [c for c in visible_cols if c in df.columns]
    if not visible_cols:
        visible_cols = list(df.columns)

    filtered_df = apply_filters(df, filters, col_meta)
    view_df = filtered_df[visible_cols]
    page_df, total_pages, total_rows = paginate_dataframe(view_df, page, page_size)

    rows = safe_values(page_df)

    return render_template(
        "dataset.html",
        dataset_id=dataset_id,
        filename=filename,
        all_columns=list(df.columns),
        visible_cols=visible_cols,
        visible_cols_set=set(visible_cols),
        col_meta=col_meta,
        filters=filters,
        rows=rows,
        page_columns=list(page_df.columns),
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        total_rows=total_rows,
        filtered_rows=len(filtered_df),
        total_df_rows=len(df),
        total_df_cols=len(df.columns),
    )


@app.route("/dataset/<dataset_id>/table")
def table_partial(dataset_id: str):
    """HTMX partial – returns only the table + pagination HTML."""
    entry = _STORE.get(dataset_id)
    if not entry:
        return "<p>Dataset not found.</p>", 404

    df: pd.DataFrame = entry["df"]
    col_meta = infer_column_filters(df)

    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 100))
    if page_size not in (25, 50, 100, 250, 500, 1000):
        page_size = 100

    filters: dict[str, str] = {}
    for col in df.columns:
        val = request.args.get(f"filter_{col}", "")
        if val:
            filters[col] = val

    visible_cols = request.args.getlist("cols")
    if not visible_cols:
        visible_cols = list(df.columns)
    visible_cols = [c for c in visible_cols if c in df.columns]
    if not visible_cols:
        visible_cols = list(df.columns)

    filtered_df = apply_filters(df, filters, col_meta)
    view_df = filtered_df[visible_cols]
    page_df, total_pages, total_rows = paginate_dataframe(view_df, page, page_size)

    rows = safe_values(page_df)

    return render_template(
        "_table.html",
        dataset_id=dataset_id,
        rows=rows,
        page_columns=list(page_df.columns),
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        total_rows=total_rows,
        filtered_rows=len(filtered_df),
        visible_cols=visible_cols,
        filters=filters,
        col_meta=col_meta,
    )


# ---------------------------------------------------------------------------
# Template filters
# ---------------------------------------------------------------------------


@app.template_filter("number_format")
def number_format_filter(value):
    """Format an integer with thousands separators."""
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return value


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
