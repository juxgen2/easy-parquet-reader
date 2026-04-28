# Easy Parquet Reader

A small, clean, **local-first** Flask web app for exploring tabular files (Parquet, CSV, Excel, JSON, and more) in a nice table interface.

## Quick start

```bash
git clone https://github.com/juxgen2/easy-parquet-reader.git
cd easy-parquet-reader
chmod +x start.sh
./start.sh
```

Then open **http://127.0.0.1:5000** in your browser.

`start.sh` will:
1. Create a Python virtual environment (`.venv`) if missing.
2. Install all dependencies from `requirements.txt`.
3. Start the Flask dev server on port 5000.

## Supported file formats

| Extension | Notes |
|-----------|-------|
| `.parquet` | via `pyarrow` |
| `.csv` | auto-detects common encodings |
| `.xlsx` / `.xls` | via `openpyxl` / `xlrd` |
| `.json` | standard JSON (array or records) |
| `.jsonl` | JSON Lines |
| `.feather` | via `pyarrow` |
| `.pkl` | pandas pickle |

## Features

- **Drag-and-drop** upload – files are never saved to disk.
- **Column visibility** – show/hide individual columns instantly.
- **Per-column filters** – dropdowns for low-cardinality columns, text/number search for the rest.
- **Pagination** – choose 25 / 50 / 100 / 250 / 500 / 1 000 rows per page.
- **HTMX** partial updates – no full page reloads for filters, columns, or pagination.

## Upload size limit

Default maximum upload size is **200 MB**. To change it, edit `app.py`:

```python
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # bytes
```

## Local-only notice

This app is designed for **local use only**. It has no authentication, no database,
and no cloud connectivity. Uploaded files are kept in memory only for the duration
of the session and are never written to disk.

## HTMX note

HTMX is loaded from the `unpkg` CDN (`https://unpkg.com/htmx.org@1.9.10`).
If you are working fully offline, download it manually and place it at `static/htmx.min.js`,
then update `templates/base.html` to point to the local file:

```html
<script src="{{ url_for('static', filename='htmx.min.js') }}" defer></script>
```

## Dependencies

- [Flask](https://flask.palletsprojects.com/) ≥ 3.0
- [pandas](https://pandas.pydata.org/) ≥ 2.0
- [pyarrow](https://arrow.apache.org/docs/python/) ≥ 14.0
- [openpyxl](https://openpyxl.readthedocs.io/) ≥ 3.1
- [xlrd](https://xlrd.readthedocs.io/) ≥ 2.0
