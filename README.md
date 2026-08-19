# QuickBooks Desktop Connectivity — Staging Script

## Overview

`staging.py` extracts QuickBooks Desktop data over QODBC and loads it into Deltalake tables on Google Cloud Storage.

This branch handles **one server hosting several company files**. Each company file has its own QODBC DSN, and the script processes them one after another in a single run, switching between them by logging QuickBooks out of the current company file.

The table list, the queries and the run status all live in a Google Sheet, so the load can be changed without touching the code.

## How it runs

```bash
python staging.py
```

For each DSN in `qb_cred.dsn_name`, in order:

1. Clean up QuickBooks if this company file has been loaded before (see [initial_load](#initial_load)).
2. Connect through QODBC, logging out of any other open company file if needed.
3. Read the load configuration from that company file's worksheet.
4. For each active table: split the query into monthly chunks, run each chunk, write to Deltalake, and write the status, row count, timings and Delta version back to the sheet.

At the end of the run the log is uploaded to the bucket and one summary email is sent covering every company file.

## Setup

1. Install the dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Create one **64-bit QODBC DSN per company file** (ODBC Data Sources (64-bit)).

3. Fill in `data_store_config.yml` (see below).

4. Make sure `iconfig.yml` in the bucket has an entry in `company_file_mapping` for every DSN.

Runs on Windows only — it uses `pywinauto` to drive the QuickBooks UI and `psutil` to manage `QBW.EXE`.

## Configuration

### `data_store_config.yml` (on the client machine)

| Key | Description |
|---|---|
| `qb_cred.path_to_qb` | Full path to `QBW.EXE`. Used to attach to the QuickBooks window for logout and to relaunch it |
| `qb_cred.dsn_name` | **Comma-separated** DSN names, one per company file, processed in this order |
| `qb_cred.server_name` | Label for this machine. Appears in the email subject and body only |
| `bucket_cred.bucket_name` | Bucket holding `iconfig.yml` |
| `bucket_cred.orgid` / `datasetid` | Used to locate `iconfig.yml` and the log folder |
| `bucket_cred.bucket_key` | Base64-encoded service-account JSON. **Only** used to download `iconfig.yml` |
| `initial_load` | One `True`/`False` entry per DSN — see below |

<a name="initial_load"></a>**`initial_load`** marks whether a company file has ever been loaded by this script:

```yaml
initial_load:
    'DSN_ONE' : True
    'DSN_TWO' : True
```

On a company file's first run QuickBooks was not started by this script, so whatever the user has open is left alone and released by a graceful logout. On later runs the leftover QuickBooks process is killed before connecting. The script flips each entry to `False` after that company file's first run; a DSN that is not listed is treated as `True`.

### `iconfig.yml` (in the bucket)

Downloaded at runtime from `gs://<bucket_name>/<orgid>/<datasetid>/config/iconfig.yml`. It is **never** committed to this repository.

| Key | Description |
|---|---|
| `excel_cred.key` | Base64-encoded service-account JSON. Used for Google Sheets **and** for writing the Deltalake tables and the log — it needs object-admin on `target_bucket_name` |
| `excel_cred.name` | Spreadsheet name holding the load configuration |
| `company_file_mapping` | DSN name → worksheet name, one entry per company file |
| `target_bucket_name` | Bucket the Deltalake tables and the run log are written to |
| `mail_cred` | `sender_email`, `smtp_host`, `smtp_port`, `smtp_username`, `smtp_password`, `emailto` |
| `send_mail_for_all_success` | The string `'True'` to email on success too; anything else emails only on failure |

### Google Sheet columns

One worksheet per company file, header in row 1, data from row 2.

| Column | Read / Written | Notes |
|---|---|---|
| `Source_Entity_Name` | read | Table or report name. Must be unique in the worksheet |
| `Active_Flag` | read | Exactly `Y` to process the row |
| `Source_Query_Type` | read | `table` or `report` |
| `Load_Type` | read | `full` (overwrite, then append per chunk) or `delta` (merge on `Primary_Key`) |
| `Source_Query` | read | The SQL sent to QODBC |
| `Primary_Key` | read | Comma-separated; used to build the merge predicate |
| `Audit_Column` | read | Incremental column, e.g. `TimeModified` |
| `Current_Date_Flag` | read | `Y` extends the query's end date to today |
| `Target_Path` | read | Bucket-relative folder, e.g. `data/<orgid>/<datasetid>/delta/`. **Required** — a blank value fails the table |
| `Start_Datetime` | read + written | Delta watermark, then overwritten with this run's start. Format `YYYY-MM-DD HH:MM:SS` |
| *(end time)* | written | Must be the column **immediately right** of `Start_Datetime` — written by position, not by name |
| `Run_Status` | written | `Running` → `Success` / `Failed …` |
| `Table_Count` | written | Rows loaded this run. Optional — omit the column to skip it |
| `Delta_version` | read + written | Delta table version; blank is treated as `-1` |

Tables are written to `gs://<target_bucket_name>/<Target_Path>/<Source_Entity_Name>`. To move a table, change `Target_Path` — but a `delta` load cannot create a table, so reseed once with `Load_Type = full` and clear `Delta_version`.

## QuickBooks handling

- **Connection failure** — retried 3 times. Error `8004040A` (another company file is open) first triggers a graceful logout through the File menu, then a forced restart of QuickBooks if that does not release it. Other errors wait 5 minutes between attempts.
- **Empty result** — retried 3 times, then treated as a **success** with 0 rows. Nothing to load is a normal outcome for an incremental load.
- **End of run** — QuickBooks is restarted so it is back up for the next scheduled run.

## Logging and notification

Logs are written to `log_history/log_<timestamp>.txt`, tagged with the company file they belong to:

```
2026-08-19 02:14:07 - INFO - [QB_ALPHA]  >> Invoice finished successfully with 12,904 row(s)
```

At the end of the run the log is uploaded to `gs://<target_bucket_name>/<orgid>/<datasetid>/quickbooks_log/` and **one** email is sent for the whole run, naming the company files that failed:

```
Failed QuickBooks load from server SERVER_A - 1 succeeded, 2 failed (QB_BETA, QB_GAMMA)
```

## Do not commit

`iconfig.yml` holds a service-account key and the SMTP password. It lives in the bucket and is gitignored here. `log_history/` and `__pycache__/` are ignored too.
