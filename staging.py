import logging
import pyodbc
import polars as pl
import yaml
import os
import re
import psutil
import base64, json
from google.cloud import storage
from pathlib import Path
from datetime import datetime, timedelta
from deltalake import DeltaTable
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import asyncio
import threading
import sys
import subprocess
from pywinauto import Application, Desktop

script_path = Path(__file__).resolve() if '__file__' in globals() else Path(sys.argv[0]).resolve()
script_dir = script_path.parent
config_file_path = script_dir / "data_store_config.yml"
log_dir = "log_history"
os.makedirs(log_dir, exist_ok=True) 
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = f'log_{timestamp}.txt'
log_filepath = os.path.join(log_dir, log_filename)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DsnContextFilter(logging.Filter):
    """
    Tags every log record with the company file (DSN) being processed, so the
    lines stay attributable when several company files run in one log file.
    Set DsnContextFilter.current_dsn before processing each company file.
    """
    current_dsn = ''

    def filter(self, record):
        record.dsn = f'[{DsnContextFilter.current_dsn}] ' if DsnContextFilter.current_dsn else ''
        return True


log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(dsn)s%(message)s')
file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
file_handler.setFormatter(log_format)
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_format)
logger.addHandler(console_handler)
logger.addFilter(DsnContextFilter())
load_failed = False
# Per company file outcome for this run, in the order the DSNs are processed.
# Filled in by main() and summarised in the notification email.
dsn_status = {}
run_started_at = datetime.now()

config = {}
dsn_names = []

try:

    async def read_database(connection,query,table,itr_count):
        """
        Executes a database query asynchronously using the provided connection.
        Returns the result dataframe and a retry flag.
        """
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(dsn_connection, connection, query,table,itr_count), timeout= 600
                )
        except asyncio.TimeoutError:
            close_connection(connection, f'the time limit on {table}')
            logger.info(f" >> Time limit exceeded. Waiting for 5 mins and retrying. QODBC connection closed.\n")
            return None,True
    
    def dsn_connection(run_connection, run_query,table,itr_count):
        """
        Executes a SQL query using the provided pyodbc connection.
        Returns the result dataframe and a retry flag.
        """
        try:
            logger.info(f' >> Executing chunk {itr_count} for the table {table}-->\n')
            sql_query = run_query
            out_df = pl.read_database(sql_query, run_connection)
            return out_df,False
            
        except Exception as e:
            logger.error(f" >> Error in dsn while executing query {run_query} : {e}\n")
            close_connection(run_connection, f'a query error on {table}')
            return None,True

    def decode_bucket_cred(encoded_str):
        """
        Decodes a base64-encoded string and returns the decoded string.
        """
        try: 
            decoded_bytes = base64.b64decode(encoded_str)
            decoded_str = decoded_bytes.decode('utf-8')
            return decoded_str
        except Exception as e:
            # The encoded value is a service account key - never write it to the log,
            # the log is emailed out and uploaded to the bucket.
            logger.error(f" >> \t Error in decoding the base64 credential : {str(e)}\n")
            return None
     
    def is_quickbooks_running():
        """
        Checks if QuickBooks is currently running on the system.
        Returns True if running, False otherwise.
        """
        try:
            for proc in psutil.process_iter(['name']):
                try:
                    if (proc.info.get('name') or '').lower() in ['qbw.exe', 'qbw32.exe']:
                        return True
                except Exception:
                    continue
            return False
        except Exception as e:
            logger.error(f" >> Error while checking for a running QuickBooks process: {str(e)}\n")
            return False
    
    # Dialogs QuickBooks puts up that stop a run dead until somebody clicks them.
    # Each entry is (window title regex, text the window must contain or None,
    # button to click). The text check stops a loose title regex from matching
    # QuickBooks' own main window.
    BLOCKING_QB_DIALOGS = [
        # Shown while the company file opens when the IE zone security level is
        # above default. Cancel is deliberate: it dismisses the dialog without
        # changing the machine's security settings, and QODBC does not need the
        # features the dialog warns about.
        (r'^Internet Security Levels Are Set Too High$', None, r'^Cancel$'),
        (r'^Internet Security Levels Confirmation$', None, r'^Yes$'),
        # The crash on the way out of a QODBC session.
        (r'^QuickBooks - Unrecoverable Error$', None, r"^Don.?t Send$"),
        (r'.*Intuit QuickBooks.*', 'Aborting Application', r'^OK$'),
    ]

    def dismiss_blocking_dialogs():
        """
        Clicks away any known blocking QuickBooks dialog currently on screen.

        Returns the number of dialogs dismissed. Never raises: this runs on a
        watcher thread and beside a blocked QODBC call, so a failure here must
        not take anything else down with it.
        """
        dismissed = 0
        try:
            desktop = Desktop(backend='uia')
        except Exception as e:
            logger.error(f" >> Could not reach the desktop to look for QuickBooks dialogs: {e}\n")
            return 0

        for title_re, required_text, button_re in BLOCKING_QB_DIALOGS:
            try:
                windows = desktop.windows(title_re=title_re, top_level_only=True)
            except Exception:
                continue

            for win in windows:
                try:
                    # Read the title up front: it comes back empty once the
                    # window starts closing.
                    title = win.window_text()
                    if required_text:
                        texts = []
                        try:
                            texts = [c.window_text() for c in win.descendants(control_type='Text')]
                        except Exception:
                            pass
                        haystack = ' '.join([title] + texts).lower()
                        if required_text.lower() not in haystack:
                            continue

                    # descendants() is used rather than child_window(): windows()
                    # hands back wrappers, and child_window() only exists on a
                    # WindowSpecification.
                    button = None
                    for candidate in win.descendants(control_type='Button'):
                        if re.match(button_re, candidate.window_text().strip()):
                            button = candidate
                            break
                    if button is None:
                        continue

                    try:
                        # Invoke pattern - does not move the real mouse pointer.
                        button.click()
                    except Exception:
                        button.click_input()

                    logger.info(f" >> Dismissed QuickBooks dialog '{title}'.\n")
                    dismissed += 1
                except Exception:
                    continue

        return dismissed

    _dialog_watcher = {'stop': None, 'thread': None}

    def start_dialog_watcher(interval=5):
        """
        Polls for the dialogs above on a daemon thread. Needed because they
        appear while the main thread is blocked inside a QODBC call - during
        `pyodbc.connect` as the company file opens, and again on close - so
        nothing on the main thread is in a position to notice them.
        """
        if _dialog_watcher['thread'] is not None:
            return

        stop = threading.Event()

        def _watch():
            while not stop.wait(interval):
                dismiss_blocking_dialogs()

        thread = threading.Thread(target=_watch, name='qb-dialog-watcher', daemon=True)
        _dialog_watcher['stop'] = stop
        _dialog_watcher['thread'] = thread
        thread.start()
        logger.info(f" >> Watching for blocking QuickBooks dialogs every {interval}s.\n")

    def stop_dialog_watcher():
        """Stops the watcher started by `start_dialog_watcher`. Safe to call twice."""
        stop = _dialog_watcher.get('stop')
        thread = _dialog_watcher.get('thread')
        if stop is None:
            return
        stop.set()
        if thread is not None:
            thread.join(10)
        _dialog_watcher['stop'] = None
        _dialog_watcher['thread'] = None
        logger.info(" >> Stopped watching for QuickBooks dialogs.\n")

    def kill_quickbooks_process():
        """
        Find and kill any running QuickBooks processes.

        Returns:
            List[dict]: A list of dicts describing the killed processes. Each dict
            contains 'name' and 'exe' keys. If nothing was found or an error
            occurred an empty list is returned.
        """
        try:
            qb_processes = []
            logger.info(f" >> Looking for QuickBooks processes.\n")
            for proc in psutil.process_iter(['pid', 'name', 'exe']):
                try:
                    pname = (proc.info.get('name') or '').lower()
                except Exception:
                    # proc.info access can fail for system/privileged processes
                    continue

                if pname in ['qbw.exe', 'qbw32.exe']:
                    logger.info(f" >> Found process: {proc.info.get('name')} (PID: {proc.info.get('pid')})")
                    qb_processes.append({
                        'name': proc.info.get('name'),
                        'exe': proc.info.get('exe')
                    })
                    try:
                        proc.terminate()
                    except Exception as e:
                        logger.error(f" >> Error terminating process {proc.pid if hasattr(proc,'pid') else '?'}: {str(e)}\n")
                        continue
                    try:
                        # Bounded: a QuickBooks left on a crash dialog can outlive the
                        # terminate, and an unbounded wait would stall the whole run.
                        proc.wait(timeout=60)
                    except Exception as e:
                        logger.error(f" >> Process {proc.pid if hasattr(proc,'pid') else '?'} did not exit after terminate: {str(e)}\n")

            if qb_processes:
                logger.info(f" >> Killed {len(qb_processes)} QuickBooks process(es).\n")
            else:
                logger.info(f" >> No QuickBooks processes found to kill.\n")

            return qb_processes

        except Exception as e:
            logger.error(f" >> Error in killing QuickBooks process: {str(e)}\n")
            return []

    def relaunch_quickbooks_processes(qb_processes, qb_path=None, delay_seconds=60):
        """
        Relaunch QuickBooks processes from a list produced by
        `kill_quickbooks_process`.

        Args:
            qb_processes (list): List of dicts with 'name' and 'exe' keys.
            qb_path (str): Executable to fall back on when psutil could not read
                the exe path of the killed process.
            delay_seconds (int): Seconds to wait before attempting relaunch.
        """
        try:
            if not qb_processes:
                logger.info(" >> No QuickBooks processes to relaunch.\n")
                return

            logger.info(f" >> Waiting {delay_seconds} second(s) before relaunch...\n")
            time.sleep(delay_seconds)

            for qb in qb_processes:
                try:
                    exe_path = qb.get('exe') or qb_path
                    if exe_path:
                        subprocess.Popen([exe_path])
                        logger.info(f" >> Relaunched QuickBooks process: {qb.get('name')}\n")
                    else:
                        logger.warning(f" >> No executable path for process {qb.get('name')}; cannot relaunch.\n")
                except Exception as e:
                    logger.error(f" >> Error relaunching QuickBooks process {qb.get('name')}: {str(e)}\n")

        except Exception as e:
            logger.error(f" >> Error in relaunching QuickBooks processes: {str(e)}\n")

    def restart_quickbooks(qb_path=None):
        """
        Force restart of QuickBooks: kill every running instance and bring the
        same executables back up. Used as the recovery step for connection
        failures that a graceful logout could not resolve.
        """
        qb_processes = kill_quickbooks_process()
        relaunch_quickbooks_processes(qb_processes, qb_path)
        return qb_processes

    def logout_quickbooks(qb_path):
        """
        Logs out of QuickBooks by automating the UI to close the company/log off.
        Handles backup dialogs if they appear.
        """
        if not is_quickbooks_running():
            logger.info("QuickBooks is not running. Exiting...")
            return

        try:
            app = Application(backend='uia').connect(path=rf'{qb_path}')
            logger.info("Connected to QuickBooks application.")
            main_win = None
            for win in app.windows():
                title = win.window_text()
                if "Intuit QuickBooks" in title:
                    logger.info(f"Found QuickBooks window: {title}")
                    main_win = win
                    break
            if main_win:
                main_win.set_focus()
                logger.info("QuickBooks window activated.")
        except Exception as e:
            logger.error(f"Failed to connect to QuickBooks window: {e}")
            return

        # Access 'File' menu and 'Close Company/Logoff'
        try:
            # Use the top menu bar
            logger.info("Attempting to open File menu...")
            main_win.type_keys("%F", set_foreground=True)
            time.sleep(0.5) 
            main_win.type_keys("C")
            logger.info("Logout option selected from File menu.")
        except Exception as e:
            logger.error(f"Menu navigation failed: {e}")
            return

        # Handle optional backup/save dialog
        time.sleep(2)
        try:
            logger.info("Checking for backup/save dialogs...")
            children = main_win.children()
            
            backup_dialog = None
            for child in children:
                if child.window_text() == "Automatic Backup" and child.friendly_class_name() == "Dialog":
                    backup_dialog = child
                    break
            if backup_dialog:
                no_button = None
                for child in backup_dialog.children():
                    if child.window_text() == "No":
                        no_button = child
                        break
                
                if no_button:
                    no_button.click_input()
                    logger.info("Clicked 'No' on the 'Automatic Backup' dialog.")
                else:
                    logger.warning("Could not locate the 'No' button within the 'Automatic Backup' dialog.")
            else:
                logger.info("The 'Automatic Backup' dialog did not appear among the main window's children.")
        except Exception as e:
            logger.warning(f"Could not handle backup dialog: {e}")
            
        logger.info("Logged out of QuickBooks successfully.")
        return

    def check_connection_status(connection):
        """
        Checks if the pyodbc connection is open.
        Returns True if open, False otherwise.
        """
        try:
            # pyodbc connection has a closed attribute: 0=open, 1=closed
            if hasattr(connection, 'closed'):
                return connection.closed == 0
            else:
                # If the connection object does not have 'closed' attribute, assume it's open
                return True
        except Exception:
            return False

    def close_connection(connection, context, timeout=120):
        """
        Closes the QODBC connection if it is still open. Safe to call with None.

        The close runs on a daemon thread because it is a blocking QODBC call: if
        QuickBooks dies on its way out of the session it leaves a modal dialog on
        the desktop and the call does not return until somebody clicks it away,
        which stalls everything after it - including the notification email.

        When the close does not come back within `timeout` seconds QuickBooks is
        killed. That is the same hard kill as before, but it now fires only when
        the graceful release actually failed, so a clean run leaves the company
        file properly released instead of locked by an abandoned QODBC session.
        """
        if not connection:
            return
        if not check_connection_status(connection):
            return

        outcome = {}

        def _close():
            try:
                connection.close()
                outcome['closed'] = True
            except Exception as e:
                outcome['error'] = e

        closer = threading.Thread(target=_close, name='qodbc-close', daemon=True)
        closer.start()
        closer.join(timeout)

        if closer.is_alive():
            logger.error(
                f" >> QODBC connection did not close within {timeout}s after {context}. "
                f"QuickBooks is most likely showing a dialog and still holding the "
                f"company file. Killing QuickBooks to release it.\n"
            )
            if dismiss_blocking_dialogs():
                # Clicking the dialog away may be enough to let the close finish.
                closer.join(30)
            if closer.is_alive():
                kill_quickbooks_process()
            # Killing QuickBooks normally unblocks the pending ODBC call.
            closer.join(30)
            if closer.is_alive():
                logger.error(f" >> QODBC close is still hanging after {context}; continuing without it.\n")
            return

        if 'error' in outcome:
            logger.error(f" >> Error while closing connection: {outcome['error']}\n")
        else:
            logger.info(f" >> QODBC connection closed after {context}.\n")

    def split_query_by_month(query,type,current_date_flag):
        """
        Splits a query into monthly chunks based on date parameters.
        Returns a list of queries for each month.
        """
        try:
            excecute = 0
            if 'parameters' not in query.lower() and type.lower() == 'report':
                return [query.strip()]
            if type.lower() == 'report':
                date_pattern = r"(DateFrom\s*=\s*\{d'(\d{4}-\d{2}-\d{2})'\}.*?DateTo\s*=\s*\{d'(\d{4}-\d{2}-\d{2})'\})"
                match = re.search(date_pattern, query, re.IGNORECASE | re.DOTALL)
                if not match :
                    date_patterns = [r"(DateFrom\s*=\s*\{d'(\d{4}-\d{2}-\d{2})'\}.*?,)" ,r"(DateFrom\s*=\s*\{d'(\d{4}-\d{2}-\d{2})'\}.*?)"]
                    for pattern in date_patterns:
                        match = re.search(pattern, query, re.IGNORECASE | re.DOTALL)
                        if match:
                            break
                    if not match:
                        return [query.strip()]
                    full_match, start_date_str, end_date_str = match.group(1),match.group(2),datetime.now().strftime("%Y-%m-%d")
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
                    excecute = 1
            elif type.lower() == 'table':
                date_pattern = r"(?i)(TimeModified\s*between\s*{ts'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d{3})'\}\s*and\s*{ts'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d{3})'\})"
                match = re.search(date_pattern, query, re.IGNORECASE | re.DOTALL)
                if not match :
                    date_patterns = [r"(?i)(TimeModified\s*between\s*{ts'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d{3})'\}\s*and)" ,r"(?i)(TimeModified\s*between\s*{ts'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d{3})'\}\s)"]
                    for pattern in date_patterns:
                        match = re.search(pattern, query, re.IGNORECASE | re.DOTALL)
                        if match:
                            break
                    if not match and current_date_flag.lower() == 'y':
                        date_patterns = [r"(?i)(TimeModified\s*>\s*{ts'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d{3})'\}\s)",r"(?i)(TimeModified\s*<\s*{ts'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d{3})'\}\s)",r"(?i)(TimeModified\s*>=\s*{ts'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d{3})'\}\s)",r"(?i)(TimeModified\s*<=\s*{ts'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}\.\d{3})'\}\s)",r"(?i)(TimeModified\s*>\s)",r"(?i)(TimeModified\s*<\s)",r"(?i)(TimeModified\s*<=\s)",r"(?i)(TimeModified\s*>=\s)"]
                        for pattern in date_patterns:
                            match = re.search(pattern, query, re.IGNORECASE | re.DOTALL)
                            if match:
                                full_match = match.group(1)
                                if '>=' in full_match:
                                    modified_query = query.replace(
                                        full_match,
                                        f"TimeModified >= {{ts'{datetime.now().strftime('%Y-%m-%d')} 00:00:00.000'}} "
                                    ).strip()
                                    return [modified_query.replace("\n", " ")]
                                
                                elif '<=' in full_match:
                                    modified_query = query.replace(
                                        full_match,
                                        f"TimeModified <= {{ts'{datetime.now().strftime('%Y-%m-%d')} 00:00:00.000'}} "
                                    ).strip()
                                    return [modified_query.replace("\n", " ")]
                                elif '>' in full_match:
                                    modified_query = query.replace(
                                        full_match,
                                        f"TimeModified > {{ts'{datetime.now().strftime('%Y-%m-%d')} 00:00:00.000'}} "
                                    ).strip()
                                    return [modified_query.replace("\n", " ")]
                                elif '<' in full_match:
                                    modified_query = query.replace(
                                        full_match,
                                        f"TimeModified < {{ts'{datetime.now().strftime('%Y-%m-%d')} 00:00:00.000'}} "
                                    ).strip()
                                    return [modified_query.replace("\n", " ")]
                               
                    if not match: 
                        return [query.strip()]
                    full_match, start_date_str, end_date_str = match.group(1), match.group(2), datetime.now().strftime("%Y-%m-%d")
                    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
                    excecute = 1
            if not match:
                return [query.strip()]
            
            
            if excecute == 0:
                full_match, start_date_str, end_date_str = match.groups() if type.lower() == 'report' else (match.group(1), match.group(2), match.group(4))
                start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
                if current_date_flag.lower() == 'y':
                    end_date_str = datetime.now().strftime("%Y-%m-%d")
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            

            queries = []

            logger.info(f' >> Processing the {type} from {start_date_str} to {end_date_str}\n') 

            current_date = start_date
            if type.lower() == 'report':
                while current_date <= end_date:
                    month_start = current_date.replace(day=1)
                    next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
                    month_end = next_month - timedelta(days=1)
                    month_start = max(month_start, start_date)
                    month_end = min(month_end, end_date)
                    month_start_str = month_start.strftime("%Y-%m-%d")
                    month_end_str = month_end.strftime("%Y-%m-%d")
                    modified_query = query.replace(
                        full_match,
                        f"DateFrom = {{d'{month_start_str}'}}, DateTo = {{d'{month_end_str}'}}"
                    ).strip()
                    queries.append(modified_query)
                    current_date = next_month
                return queries


            while current_date <= end_date:
                month_start = current_date.replace(day=1)
                next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
                month_end = next_month - timedelta(days=1)
                month_start, month_end = max(month_start, start_date), min(month_end, end_date)


                modified_query = query.replace(
                    full_match,
                    f"TimeModified BETWEEN {{ts'{month_start.strftime('%Y-%m-%d')} 00:00:00.000'}} "
                    f"AND {{ts'{month_end.strftime('%Y-%m-%d')} 23:59:59.999'}}"
                ).strip()

                cleaned_query = modified_query.replace("\n", " ")
                queries.append(cleaned_query)
                current_date = next_month 
            
            return queries
        
        except Exception as e:
            logger.error(f">> Error in chunking the query => {query} : {str(e)}\n")
            return None 
        
        
    def datatype_conversion(df,table_name):
        """
        Converts dataframe column datatypes to appropriate formats for Deltalake.
        Returns the converted dataframe.
        """
        try:
            dtype = dict(df.schema)
            for cols in df.columns:
                if "String" in str(dtype[cols]):
                    df = df.with_columns([
                        pl.col(cols).fill_null("").cast(pl.Utf8)
                    ])
                elif "Date" in str(dtype[cols]):

                    df = df.with_columns(
                        pl.col(cols).cast(pl.Utf8)
                    )
                    df = df.with_columns(
                        pl.col(cols).str.to_datetime()
                    )
                elif "Decimal" in str(dtype[cols]):
                    df = df.with_columns(
                        pl.col(cols).fill_null(0).cast(pl.Float64)
                    )
                elif "Int" in str(dtype[cols]):
                    df = df.with_columns(
                        pl.col(cols).fill_null(0).cast(pl.Int64)
                    )
                elif "Bool" in str(dtype[cols]):
                    df = df.with_columns(
                        pl.col(cols).cast(pl.Boolean))
                    
            logger.info(f" >> \t Datatype conversion done for {table_name}\n")
            return df
        
        except Exception as e:
            logger.error(f" >> Failed in the datatype_conversion {str(e)}\n")
            return None
     
    def load_data(df, primary_key, table_path, overwrite, load_type, storage_options):
        """
        Loads the dataframe to Deltalake using the specified mode and options.
        Returns a status dictionary indicating success or failure.
        """
        try:
            if load_type.lower() == 'full':
                if overwrite:
                    mode = "overwrite"
                    logger.info(f" >> Loading the data in overwrite mode\n")
                else:
                    mode = "append"
                    logger.info(f" >> Loading the data in append mode\n")
            else:
                mode = "merge"
                logger.info(f" >> Loading the data in delta mode\n")
            
            delta_write_options = {'schema_mode': 'overwrite'} if mode == "overwrite" else {}
            
            
            if mode == "merge":
                predicate = " AND ".join([f"src.{key} = tgt.{key}" for key in primary_key.split(',')])
                delta_merge_options = {
                    "predicate": predicate,
                    "source_alias": "src",
                    "target_alias": "tgt",
                }
                
            
                res = df.write_delta(
                    table_path,
                    storage_options=storage_options,
                    mode=mode,
                    delta_write_options=delta_write_options,
                    delta_merge_options=delta_merge_options
                ).when_matched_update_all().when_not_matched_insert_all().execute()
            
                
                
            else:
                
                res = df.write_delta(
                    table_path,
                    storage_options=storage_options,
                    mode=mode,
                    delta_write_options=delta_write_options
                )
            
            return {"status" : "success", "message" : "Data uploaded to Deltalake Successfully"}
                
        except Exception as e:
            logger.error(f" >> Failed in the loading files to the bucket => {str(e)}\n")
            return {'status' : 'failed', 'message' : f'{str(e)}'}
    
    def setVersion(table_name,table_path,storage_options,backup_version):
        """
        Restores the DeltaTable to the specified backup version and returns the latest version.
        """
        try:
            delta_table = DeltaTable(table_path , storage_options=storage_options)
            latest_version = delta_table.history()[0]
            if latest_version['version'] == backup_version:
                logger.info(f" >> The table {table_name} is already in the backup version => {backup_version}\n")
                return latest_version['version']
            delta_table.restore(backup_version)
            latest_version = delta_table.history()[0]
            logger.info(f" >> Restored the table {table_name} to the backup version => {backup_version}\n")
            return latest_version['version']

        except Exception as e:
            logger.error(f' >> Error in getting version for the table {table_name} => {str(e)}\n')
            return backup_version


    def email_process(load_failed, config_cred_path, file_path):
        """
        Sends an email notification with the log file attached after data load.
        Uploads the log file to the bucket and sends email based on configuration.
        """
        result = ""
        config_file_path = config_cred_path
        with open(config_file_path, 'r') as file:
            config = yaml.safe_load(file) 
        # bucket_cred is only used to fetch iconfig.yml
        json_key = decode_bucket_cred(config['bucket_cred']['bucket_key'])
        cred = json.loads(json_key)
        client = storage.Client.from_service_account_info(cred)
        server_name = config['qb_cred']['server_name']
        bucket_name=config['bucket_cred']['bucket_name']
        orgid=config['bucket_cred']['orgid']
        datasetid = config['bucket_cred']['datasetid']
        bucket = client.bucket(bucket_name)
        excel_cred_path = f'{orgid}/{datasetid}/config/iconfig.yml'
        blob = bucket.blob(excel_cred_path)
        yaml_data = blob.download_as_bytes()
        config = yaml.safe_load(yaml_data)
        credential = config['mail_cred']
        send_mail_if_success = True if config['send_mail_for_all_success'] == 'True' else False

        # Upload log file to the target bucket, using the credentials from iconfig
        log_uri = None
        try:
            ecred_key_dict = json.loads(decode_bucket_cred(config['excel_cred']['key']))
            target_bucket_name = config['target_bucket_name']
            log_client = storage.Client.from_service_account_info(ecred_key_dict)
            log_bucket = log_client.bucket(target_bucket_name)
            log_path_in_bucket = f'{orgid}/{datasetid}/quickbooks_log/{log_filename}'
            log_blob = log_bucket.blob(log_path_in_bucket)
            log_blob.upload_from_filename(file_path)
            log_uri = f'gs://{target_bucket_name}/{log_path_in_bucket}'
            logger.info(f'>> Log file uploaded to {log_uri}\n')
        except Exception as e:
            logger.error(f'>> Failed to upload the log file to the target bucket: {str(e)}\n')
        
        
        # Email configuration
        sender_email = credential['sender_email']
        smtp_host = credential['smtp_host']
        smtp_port = credential['smtp_port']
        smtp_username = credential['smtp_username']
        smtp_password = credential['smtp_password']
        receiver_emails = credential['emailto']


        receiver_emails = list(set(receiver_emails))

        succeeded = [name for name, status in dsn_status.items() if str(status).startswith('Success')]
        failed = [name for name, status in dsn_status.items() if not str(status).startswith('Success')]

        if not dsn_status:
            # The run did not get as far as processing a single company file
            subject = f'Failed QuickBooks load from server {server_name} - no company file was processed'
        elif failed:
            named = ', '.join(failed[:3]) + (f' and {len(failed) - 3} more' if len(failed) > 3 else '')
            subject = f'Failed QuickBooks load from server {server_name} - {len(succeeded)} succeeded, {len(failed)} failed ({named})'
        else:
            subject = f'Successful QuickBooks load from server {server_name} - {len(succeeded)} company file(s) loaded'

        finished_at = datetime.now()
        elapsed = str(finished_at - run_started_at).split('.')[0]
        ok = bool(dsn_status) and not failed

        # ---- plain text part, for clients that do not render HTML ----
        status_lines = '\n'.join(f'   {name} : {status}' for name, status in dsn_status.items()) or '   (no company file was processed)'
        message = (
            f"Hello,\n\n"
            f" Please find attached the text file containing the log status from the client system for the server {server_name}.\n\n"
            f" Server        : {server_name}\n"
            f" Started       : {run_started_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f" Finished      : {finished_at.strftime('%Y-%m-%d %H:%M:%S')} (took {elapsed})\n"
            f" Company files : {len(dsn_status)} processed, {len(succeeded)} succeeded, {len(failed)} failed\n\n"
            f" Status per company file:\n{status_lines}\n\n"
            f" Log file      : {log_filename}\n\n"
            f" Thanks,\n\n Team Conversight"
        )

        # ---- html part ----
        def esc(value):
            return (str(value).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'))

        accent = '#0f7b3f' if ok else '#b3261e'
        tint = '#eaf6ee' if ok else '#fdeceb'
        headline = 'Load completed successfully' if ok else 'Load finished with errors'

        rows = []
        for n, (name, status) in enumerate(dsn_status.items()):
            good = str(status).startswith('Success')
            label, _, detail = str(status).partition(' - ')
            stripe = '#ffffff' if n % 2 == 0 else '#fafbfc'
            rows.append(
                f'<tr style="background:{stripe};">'
                f'<td style="padding:10px 14px;border-top:1px solid #eceff1;font-size:14px;color:#202124;'
                f'font-weight:600;white-space:nowrap;">{esc(name)}</td>'
                f'<td style="padding:10px 14px;border-top:1px solid #eceff1;white-space:nowrap;">'
                f'<span style="background:{"#eaf6ee" if good else "#fdeceb"};color:{"#0f7b3f" if good else "#b3261e"};'
                f'font-size:11px;font-weight:700;letter-spacing:.04em;padding:3px 9px;border-radius:10px;">'
                f'{"SUCCESS" if good else "FAILED"}</span></td>'
                f'<td style="padding:10px 14px;border-top:1px solid #eceff1;font-size:13px;color:#5f6368;">'
                f'{esc(detail or label)}</td></tr>'
            )
        if not rows:
            rows.append(
                '<tr><td colspan="3" style="padding:14px;border-top:1px solid #eceff1;font-size:13px;'
                'color:#5f6368;font-style:italic;">No company file was processed.</td></tr>'
            )

        def meta_row(label, value):
            return (f'<tr><td style="padding:3px 0;font-size:13px;color:#5f6368;width:130px;">{esc(label)}</td>'
                    f'<td style="padding:3px 0;font-size:13px;color:#202124;">{esc(value)}</td></tr>')

        meta = (
            meta_row('Server', server_name)
            + meta_row('Started', run_started_at.strftime('%Y-%m-%d %H:%M:%S'))
            + meta_row('Finished', f"{finished_at.strftime('%Y-%m-%d %H:%M:%S')}  (took {elapsed})")
            + meta_row('Company files', f'{len(dsn_status)} processed, {len(succeeded)} succeeded, {len(failed)} failed')
            + meta_row('Log file', log_filename)
            + (meta_row('Log location', log_uri) if log_uri else '')
        )

        html = f"""<html><body style="margin:0;padding:0;background:#f1f3f4;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f1f3f4;padding:24px 12px;">
<tr><td align="center">
<table role="presentation" width="620" cellpadding="0" cellspacing="0" style="max-width:620px;width:100%;background:#ffffff;border:1px solid #e0e3e7;border-radius:10px;overflow:hidden;font-family:'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
  <tr><td style="background:{accent};height:5px;line-height:5px;font-size:0;">&nbsp;</td></tr>
  <tr><td style="padding:24px 28px 4px 28px;">
    <div style="font-size:11px;letter-spacing:.1em;text-transform:uppercase;color:#80868b;">QuickBooks Data Load</div>
    <div style="font-size:21px;font-weight:600;color:#202124;padding-top:6px;">{esc(headline)}</div>
  </td></tr>
  <tr><td style="padding:14px 28px 0 28px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
           style="background:{tint};border-radius:8px;"><tr><td style="padding:12px 16px;font-size:14px;color:{accent};font-weight:600;">
      {len(succeeded)} of {len(dsn_status)} company file(s) loaded successfully
    </td></tr></table>
  </td></tr>
  <tr><td style="padding:20px 28px 0 28px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0">{meta}</table>
  </td></tr>
  <tr><td style="padding:20px 28px 4px 28px;">
    <div style="font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#80868b;padding-bottom:8px;">Status per company file</div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
           style="border:1px solid #eceff1;border-radius:8px;border-collapse:separate;overflow:hidden;">{''.join(rows)}</table>
  </td></tr>
  <tr><td style="padding:20px 28px 24px 28px;">
    <div style="font-size:13px;color:#5f6368;line-height:1.55;">The full run log is attached to this email and stored in the bucket.</div>
  </td></tr>
  <tr><td style="border-top:1px solid #eceff1;padding:14px 28px;background:#fafbfc;">
    <div style="font-size:12px;color:#80868b;">Team ConverSight &middot; automated notification</div>
  </td></tr>
</table>
</td></tr></table>
</body></html>"""

        # Create a message object: alternative bodies first, then the log attachment
        msg = MIMEMultipart('mixed')
        msg["Subject"] = subject
        msg["From"] = sender_email
        msg["To"] = ", ".join(receiver_emails)
        body = MIMEMultipart('alternative')
        body.attach(MIMEText(message, "plain"))
        body.attach(MIMEText(html, "html"))
        msg.attach(body)

        text_file_path = file_path  # Update with your actual text file path
        with open(text_file_path, "r") as text_file:
            text_content = text_file.read()
            text_attachment = MIMEText(text_content, _subtype="plain")
            text_attachment.add_header(
                "Content-Disposition",
                'attachment; filename="{}"'.format(os.path.basename(text_file_path))
            )
            msg.attach(text_attachment)


        try:
            if send_mail_if_success or load_failed:
                smtp = smtplib.SMTP(host=smtp_host, port=smtp_port)
                smtp.starttls()
                smtp.login(smtp_username, smtp_password)
                smtp.sendmail(sender_email, receiver_emails, msg.as_string())

                result = f"Email has been sent successfully to -> {','.join(receiver_emails)}"
                logger.info('>> Email sent successfully.\n')
                return result
            
            else:   
                logger.info('>> Email is not configured to send.\n') 
                return 'Email not configured to send.'
            
        except Exception as e:
            result = "Email sending failed:", str(e) 

    async def connect_to_qodbc(qb_path, dsn_name, max_retries=3, timeout=600):
        """
        Asynchronously attempts to connect to QuickBooks via pyodbc.
        Retries on failure up to max_retries. Returns connection and failure flag.

        When QuickBooks reports 8004040A (a different company file is already
        open) the recovery escalates: first a graceful UI logout, and only if
        that does not release the file a forced restart of QuickBooks.
        """
        retries = 0
        logout_attempts = 0
        while retries < max_retries:
            try:
                connection = await asyncio.wait_for(
                    asyncio.to_thread(pyodbc.connect, f'DSN={dsn_name}', autocommit=True), 
                    timeout=timeout
                )
                logger.info(f'Quickbooks Connected successfully..\n')
                return connection, False 
            
            except asyncio.TimeoutError:
                retries += 1
                logger.info(f'Attempt {retries}: Timeout occurred. Retrying in 5 minutes...\n')
                await asyncio.sleep(300) 
            except pyodbc.Error as e1:
                retries += 1
                error_message = str(e1)
                logger.info(f'>> Error in Initialing Quickbooks => {error_message}\n')
                if '8004040a' in error_message.lower():
                    logout_attempts += 1
                    if logout_attempts <= 1:
                        logger.info(f'Attempt {retries}: QuickBooks is already logged in with another company file. Logging out...\n')
                        logout_quickbooks(qb_path)
                        await asyncio.sleep(30)
                    else:
                        logger.info(f'Attempt {retries}: Graceful logout did not release the company file. Restarting QuickBooks...\n')
                        restart_quickbooks(qb_path)
                else:
                    logger.info(f"Attempt {retries}: unable to connect quickbooks through pyodbc..Retrying in 5 minutes...")
                    await asyncio.sleep(300)
            except Exception as e:
                logger.error(f'Unexpected error while Quickbooks connection => {e}\n')
                return None, True

        logger.error(f'Max retries reached. Connection failed..\n')
        restart_quickbooks(qb_path)
        return None, True

    def consume_initial_load_flag(dsn_name):
        """
        Returns the `initial_load` flag of a single company file (DSN) and marks
        that company file as loaded, so only its first ever run is treated as the
        initial load. A DSN that is not listed yet defaults to True.

        `initial_load` is a mapping in the config file:

            initial_load:
                'DSN_ONE' : True
                'DSN_TWO' : True

        The file is rewritten line by line so the template comments survive.
        """
        try:
            with open(config_file_path, 'r') as file:
                initial_load = (yaml.safe_load(file) or {}).get('initial_load') or {}
            if not isinstance(initial_load, dict):
                # validate_config refuses to start in this case; guard anyway so the
                # writer below can never nest DSN entries under a scalar value.
                raise ValueError("initial_load in the config file is not a mapping")
            flag = initial_load.get(dsn_name, True)

            with open(config_file_path, 'r') as file:
                lines = file.readlines()

            entry = f"    '{dsn_name}' : False\n"
            block_start = None
            for idx, line in enumerate(lines):
                if re.match(r'^\s*initial_load\s*:', line):
                    block_start = idx
                    break

            if block_start is None:
                # No initial_load section yet - start one at the end of the file
                if lines and not lines[-1].endswith('\n'):
                    lines[-1] = lines[-1] + '\n'
                lines.append('initial_load:\n')
                lines.append(entry)
            else:
                # Walk the indented entries of the section looking for this DSN
                dsn_pattern = re.compile(rf"^\s+['\"]?{re.escape(dsn_name)}['\"]?\s*:")
                idx = block_start + 1
                updated = False
                while idx < len(lines) and (lines[idx].startswith((' ', '\t')) or not lines[idx].strip()):
                    if dsn_pattern.match(lines[idx]):
                        lines[idx] = entry
                        updated = True
                        break
                    idx += 1
                if not updated:
                    lines.insert(block_start + 1, entry)

            # Never write a file we cannot read back: the config carries the bucket
            # credentials, and a corrupted one stops every future run.
            candidate = "".join(lines)
            check = yaml.safe_load(candidate)
            if not isinstance(check, dict) or 'qb_cred' not in check or 'bucket_cred' not in check:
                raise ValueError("rewritten config did not parse back into a valid configuration")

            with open(config_file_path, 'w') as f:
                f.write(candidate)

            return flag

        except Exception as e:
            logger.error(f" >> Error while reading/updating the initial_load flag for {dsn_name}: {str(e)}\n")
            return True

    async def main(dsn_name):
        """
        Main function to orchestrate QuickBooks data extraction, transformation, and loading.
        Handles configuration, connection, data processing, and logging.
        """
        global load_failed
        if not os.path.exists(config_file_path):
            logger.info(f" >> Config file does not exist in the path => {config_file_path}\n")
            load_failed = True
            dsn_status[dsn_name] = 'Failed - config file not found'
            return
        logger.info(f'Config file fetched from path => {config_file_path}\n')
        with open(config_file_path, 'r') as file:
            data_store_config = yaml.safe_load(file)

        server_name = data_store_config['qb_cred']['server_name']
        qb_path = data_store_config['qb_cred']['path_to_qb']
        if not os.path.exists(qb_path):
            logger.info(f" >> Quickbooks path does not exist in the path => {qb_path}\n")
            load_failed = True
            dsn_status[dsn_name] = 'Failed - QuickBooks path not found'
            return

        # The first time a company file is loaded QuickBooks was not started by this
        # script, so leave whatever the user has open alone and let the graceful
        # logout in connect_to_qodbc release it. From the second run of the same
        # company file onwards, clear out what the previous run left behind.
        if consume_initial_load_flag(dsn_name):
            logger.info(f" >> Initial load for {dsn_name}: skipping the QuickBooks cleanup before connecting.\n")
        else:
            kill_quickbooks_process()

        connection = None
        Spreadsheet_name = None
        worksheet_name = None
        failed_tables = []
        tables_processed = 0
        dsn_rows = 0

        try:
            logger.info(f" >> Connecting to Quickbooks with DSN: {dsn_name}\n")

            connection, failed = await connect_to_qodbc(qb_path, dsn_name)
            if failed :
                logger.info(f" >> Quickbooks connection failed even after the retries..Terminating the code..\n")
                load_failed = True
                dsn_status[dsn_name] = 'Failed - could not connect to QuickBooks'
                return
            # bucket_cred is only used to fetch iconfig.yml
            json_key = decode_bucket_cred(data_store_config['bucket_cred']['bucket_key'])
            cred = json.loads(json_key)
            client = storage.Client.from_service_account_info(cred)
            bucket_name = data_store_config['bucket_cred']['bucket_name']
            orgid = data_store_config['bucket_cred']['orgid']
            datasetid = data_store_config['bucket_cred']['datasetid']
            bucket = client.bucket(bucket_name)
            excel_cred_path = f'{orgid}/{datasetid}/config/iconfig.yml'
            blob = bucket.blob(excel_cred_path)
            yaml_data = blob.download_as_bytes()
            config = yaml.safe_load(yaml_data)

            if str(type(config)) != "<class 'dict'>":
                logger.info(f" >> Failed while accessing the iconfig yml file \n")

            excel_key = config['excel_cred']['key']
            json_key = decode_bucket_cred(excel_key)
            ecred_key_dict = json.loads(json_key)
            scope = ['https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive']
            ecred = ServiceAccountCredentials.from_json_keyfile_dict(ecred_key_dict,scopes=scope)
            eclient = gspread.authorize(ecred)

            # The same iconfig credentials own the Deltalake tables and the target bucket
            storage_options = {'service_account_key' : json.dumps(ecred_key_dict)}
            target_bucket_name = config['target_bucket_name']
            Spreadsheet_name = config['excel_cred']['name']
            worksheet_name = config['company_file_mapping'][dsn_name]
            logger.info(f'>> Fetching QuickBooks data from server {server_name} for company file {dsn_name}\n')
            
            spreadsheet = eclient.open(Spreadsheet_name)
            load_config = spreadsheet.worksheet(worksheet_name)
            config_data = load_config.get_all_records()
            config_df = pl.DataFrame(config_data)
            table_idx = config_df['Source_Entity_Name'].to_list()
            
            for row in (config_df.filter(pl.col('Active_Flag') == 'Y')).iter_rows():
                table_name = row[config_df.columns.index('Source_Entity_Name')].strip()
                table_type = row[config_df.columns.index('Source_Query_Type')].strip()
                load_type = row[config_df.columns.index('Load_Type')].strip()
                source_query = row[config_df.columns.index('Source_Query')].strip()
                primary_columns = row[config_df.columns.index('Primary_Key')].strip()
                load_start_time = row[config_df.columns.index('Start_Datetime')].strip()
                row_count = table_idx.index(table_name) + 2
                run_status_column = config_df.columns.index('Run_Status')+1
                table_count_column = config_df.columns.index('Table_Count')+1 if 'Table_Count' in config_df.columns else None
                delta_version_column = config_df.columns.index('Delta_version')+1
                start_date_column = config_df.columns.index('Start_Datetime')+1
                audit_column = row[config_df.columns.index('Audit_Column')].strip()
                backup_version = row[config_df.columns.index('Delta_version')]
                current_date_flag = str(row[config_df.columns.index('Current_Date_Flag')]).strip()
                target_path = str(row[config_df.columns.index('Target_Path')]).strip() if 'Target_Path' in config_df.columns else ''

                # Target_Path holds the bucket-relative folder the table is written to,
                # e.g. "data/leader-95fc/6a328272-rlcZtp-vm/delta/". There is no default.
                tables_processed += 1
                table_failed = False
                table_rows = 0

                if not target_path:
                    logger.error(f" >> No Target_Path configured for {table_name}; skipping this table\n")
                    load_failed = True
                    failed_tables.append(table_name)
                    load_config.update_cell(row_count,run_status_column,'Failed - no Target_Path')
                    continue

                try:
                    backup_version = -1 if str(backup_version).strip() == '' else int(backup_version)
                except (TypeError, ValueError):
                    logger.warning(f" >> Delta_version for {table_name} is not a number ({backup_version!r}); treating it as -1\n")
                    backup_version = -1
                version = backup_version

                load_config.update_cell(row_count,run_status_column,'Running')

                overwrite = True
                if target_path.startswith('gs://'):
                    table_path = f"{target_path.rstrip('/')}/{table_name}"
                else:
                    table_path = f"gs://{target_bucket_name}/{target_path.strip('/')}/{table_name}"
                logger.info(f" >> {table_name} [{load_type}/{table_type}] will be written to {table_path}\n")

                if load_type.lower() == 'delta' and table_type.lower() == 'table':
                    start_date = datetime.strptime(load_start_time, "%Y-%m-%d %H:%M:%S")
                    start_date = start_date - timedelta(days=1)
                    start_date = start_date.strftime('%Y-%m-%d %H:%M:%S.%f')
                    currenttime = datetime.now()
                    if 'where' in source_query.lower():
                        source_query = source_query + f" AND {table_name}.{audit_column} between {{ts'{start_date}'}} and {{ts'{currenttime.strftime('%Y-%m-%d %H:%M:%S.%f')}'}}"
                    else:
                        source_query = source_query + f" where {table_name}.{audit_column} between {{ts'{start_date}'}} and {{ts'{currenttime.strftime('%Y-%m-%d %H:%M:%S.%f')}'}}"

                load_start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                query_list = split_query_by_month(source_query, table_type,current_date_flag)
                if not query_list:
                    # split_query_by_month returns None when it cannot chunk the query
                    logger.error(f" >> Could not build the query chunks for {table_name} (check Source_Query_Type / Current_Date_Flag); skipping this table\n")
                    load_failed = True
                    failed_tables.append(table_name)
                    load_config.update_cell(row_count,run_status_column,'Failed - could not parse the query')
                    continue
                total_chunks = len(query_list)
                retry_times = 3
                logger.info(f">> Processing {table_name}: {total_chunks} chunk(s) identified\n")     
                load_config.update_cell(row_count, start_date_column, load_start_time)

                brk_toggle = False

                for i, query in enumerate(query_list, start=1):

                    if brk_toggle:
                        break

                    itr_count=i
                    executions = 1
                    enable_retry = True
                    itr_toggle = True
                    chunk_empty = False

                    while executions <= retry_times and enable_retry:
                        df, enable_retry = await read_database(connection,query,table_name,itr_count) 

                        if df is None and enable_retry:
                            logger.info(f">> Retrying the same query for table {table_name} as failing in passing query through pyodbc\n")
                            enable_retry = True
                            connection, failed = await connect_to_qodbc(qb_path, dsn_name)
                            if failed :
                                logger.info(f" >> Quickbooks connection failed even after the retries..Terminating the code..\n")
                                logger.info(f" >> Failed to load {table_name}. Updating the delta table to the backup version {backup_version}")
                                load_config.update_cell(row_count,run_status_column,'Failed')
                                table_failed = True
                                upd_version = setVersion(table_name,table_path,storage_options,backup_version)
                                load_config.update_cell(row_count,delta_version_column,upd_version)
                                break
                        
                        elif df is None:
                            logger.info(f">> Interrupting the code as the query failed executing\n")
                            brk_toggle = True
                            break

                        elif df.shape[0]==0:
                            chunk_empty = True
                            close_connection(connection, f'an empty chunk for {table_name}')
                            logger.info(f" >> Retrying the same query for table {table_name} after 3 minutes, as an empty dataframe was received during the previous attempt...Initiating the Quicbooks connection again\n")
                            await asyncio.sleep(180)
                            enable_retry = True
                            connection, failed = await connect_to_qodbc(qb_path, dsn_name)
                            if failed :
                                logger.info(f" >> Quickbooks connection failed even after the retries..Terminating the code..\n")
                                logger.info(f" >> data load not done for table {table_name}..updating the delta table to the backup version ie {backup_version}")
                                load_config.update_cell(row_count,run_status_column,'Failed')
                                table_failed = True
                                upd_version=setVersion(table_name,table_path,storage_options,backup_version)
                                load_config.update_cell(row_count,delta_version_column,upd_version)
                                brk_toggle = True
                                break
                            

                        else :

                            chunk_empty = False
                            logger.info(f">> Data successfully retrieved for table {table_name} with count {df.shape}\n")
                            res = load_data(df,primary_columns,table_path,overwrite,load_type,storage_options)
                            if res['status'] == 'success' and itr_count == total_chunks:
                                load_config.update_cell(row_count,run_status_column,'Success')
                                itr_toggle = False
                            if res['status'] == 'success':
                                version = version + 1
                                table_rows = table_rows + df.shape[0]
                                # Only a write that actually landed consumes the overwrite;
                                # otherwise the next chunk would append onto stale data.
                                overwrite = False
                            else:
                                logger.error(f" >> Chunk {itr_count} of {table_name} was read but not written to {table_path} => {res['message']}\n")
                                table_failed = True
                                

                        executions = executions + 1 

                        if executions > retry_times and df is None:
                            logger.info(f" >> Maximum retries reached for the query {query}\n")
                            load_failed = True
                            logger.info(f" >> data load not done for table {table_name}..updating the delta table to the backup version ie {backup_version}")
                            load_config.update_cell(row_count,run_status_column,'Failed')
                            table_failed = True
                            upd_version=setVersion(table_name,table_path,storage_options,backup_version)
                            version = upd_version
                            load_config.update_cell(row_count,delta_version_column,upd_version)
                            brk_toggle = True
                            break

                        if itr_count == total_chunks and itr_toggle and not chunk_empty:
                            load_config.update_cell(row_count,run_status_column,"Last chunk completed but not uploaded to delta")

                    if chunk_empty and not table_failed:
                        # Nothing to load for this period - a normal outcome for an
                        # incremental load, so it is a success and not a failure.
                        logger.info(f" >> Chunk {itr_count} of {table_name} returned no rows after {retry_times} attempt(s); nothing to load for this period\n")
                        if itr_count == total_chunks and itr_toggle:
                            load_config.update_cell(row_count,run_status_column,'Success')
                            itr_toggle = False

                load_end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                load_config.update_cell(row_count, start_date_column+1, load_end_time)
                load_config.update_cell(row_count,delta_version_column,version)

                if table_count_column:
                    load_config.update_cell(row_count, table_count_column, table_rows)
                dsn_rows += table_rows

                if table_failed:
                    load_failed = True
                    failed_tables.append(table_name)
                    logger.error(f" >> {table_name} finished with errors ({table_rows:,} row(s) loaded)\n")
                else:
                    logger.info(f" >> {table_name} finished successfully with {table_rows:,} row(s)\n")

                await asyncio.sleep(10)

            if failed_tables:
                dsn_status[dsn_name] = f"Failed - {len(failed_tables)} of {tables_processed} table(s) failed: {', '.join(failed_tables)} ({dsn_rows:,} row(s) loaded)"
            else:
                dsn_status[dsn_name] = f"Success - {tables_processed} table(s), {dsn_rows:,} row(s) loaded"

            close_connection(connection, 'data load')

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f'>> Spreadsheet "{Spreadsheet_name}" not found or access denied \n')
            load_failed = True
            dsn_status[dsn_name] = f'Failed - spreadsheet "{Spreadsheet_name}" not found or access denied'
            close_connection(connection, 'failure')
            return
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f'>> Worksheet "{worksheet_name}" not found in spreadsheet "{Spreadsheet_name}" \n')
            load_failed = True
            dsn_status[dsn_name] = f'Failed - worksheet "{worksheet_name}" not found'
            close_connection(connection, 'failure')
            return
        except Exception as e:
            logger.error(f">> Error in the connection part to the query => {str(e)}\n")
            load_failed = True
            dsn_status[dsn_name] = f'Failed - {str(e)}'
            close_connection(connection, 'failure')
            restart_quickbooks(qb_path)
            return
        
    async def run_all_dsns():
        """
        Runs the load for every configured DSN (company file) in sequence. Each
        company file decides for itself whether QuickBooks needs cleaning up first
        (see the per-DSN initial_load flag in main); QuickBooks is restarted once
        after the last company file.
        """
        qb_path = config['qb_cred']['path_to_qb']
        logger.info(f" >> Run started for {len(dsn_names)} company file(s): {', '.join(dsn_names)}\n")

        start_dialog_watcher()
        try:
            await load_every_dsn(qb_path)
        finally:
            stop_dialog_watcher()

    async def load_every_dsn(qb_path):
        """
        The body of `run_all_dsns`, split out so the dialog watcher wraps all of
        it - including the restart at the end.
        """
        for position, dsn_name in enumerate(dsn_names, start=1):
            dsn_started_at = datetime.now()
            DsnContextFilter.current_dsn = dsn_name
            logger.info(f" >> [{position}/{len(dsn_names)}] Starting company file {dsn_name}\n")

            await main(dsn_name)

            if dsn_name not in dsn_status:
                # main() returned without recording an outcome
                dsn_status[dsn_name] = 'Failed - did not complete'
            elapsed = datetime.now() - dsn_started_at
            logger.info(f" >> [{position}/{len(dsn_names)}] Completed company file {dsn_name} in {elapsed} => {dsn_status[dsn_name]}\n")
            DsnContextFilter.current_dsn = ''
            await asyncio.sleep(10)

        logger.info(f"\n >> ===== Run summary ({datetime.now() - run_started_at} elapsed) =====\n")
        for name, status in dsn_status.items():
            logger.info(f" >>   {name} : {status}\n")

        # Bring QuickBooks back up for the user / the next scheduled run
        restart_quickbooks(qb_path)

    def validate_config(config):
        """
        Checks the shape of data_store_config.yml before anything runs, so a mistake
        in the file is reported as itself instead of surfacing later as a failed load.
        Returns the list of DSN names. Raises ValueError on the first problem found.
        """
        for section in ('qb_cred', 'bucket_cred'):
            if not isinstance(config.get(section), dict):
                raise ValueError(f"'{section}' is missing from {config_file_path} or is not a section")

        for key in ('dsn_name', 'path_to_qb', 'server_name'):
            if not str(config['qb_cred'].get(key) or '').strip():
                raise ValueError(f"'qb_cred.{key}' is empty in {config_file_path}")

        for key in ('bucket_name', 'orgid', 'datasetid', 'bucket_key'):
            if not str(config['bucket_cred'].get(key) or '').strip():
                raise ValueError(f"'bucket_cred.{key}' is empty in {config_file_path}")

        dsn_names = [dsn.strip() for dsn in str(config['qb_cred']['dsn_name']).split(',') if dsn.strip()]
        if not dsn_names:
            raise ValueError(f"No dsn_name configured in {config_file_path}")
        if len(set(dsn_names)) != len(dsn_names):
            raise ValueError(f"'qb_cred.dsn_name' lists the same DSN more than once in {config_file_path}")

        initial_load = config.get('initial_load')
        if initial_load is not None and not isinstance(initial_load, dict):
            raise ValueError(
                f"'initial_load' in {config_file_path} must be one entry per DSN, not a single value. Expected:\n"
                "initial_load:\n" + "".join(f"    '{d}' : True\n" for d in dsn_names)
            )

        return dsn_names

    # Read the config only after every function above exists, so that a missing or
    # invalid config file still reaches email_process in the finally block below.
    with open(config_file_path, 'r') as file:
        config = yaml.safe_load(file) or {}

    dsn_names = validate_config(config)
    logger.info(f" >> Config validated: {len(dsn_names)} company file(s) configured on server "
                f"{config['qb_cred']['server_name']}\n")

    if __name__ == "__main__":
        logger.info(f" >> Function started with a new event loop\n")
        asyncio.run(run_all_dsns())

except Exception as e:
    logger.error(f" >> Encountered error \n{e}\n")
    

finally:
    logger.info(f" >> Code execution completed and ready to send mail\n")
    try:
        email_process(load_failed, config_file_path, log_filepath)
    except Exception as e:
        # Never let the notification step hide the run's own failure
        logger.error(f" >> Could not send the notification email: {str(e)}\n")
    
