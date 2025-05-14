import logging
import pyodbc
import polars as pl
import yaml
import os
import re
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
import sys

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
file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)
load_failed = False

try:

    async def read_database(connection,query,table,itr_count):
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(dsn_connection, connection, query,table,itr_count), timeout= 600
                )
        except asyncio.TimeoutError:
            connection.close()
            logger.info(f" >> Time limit exceeded. Waiting for 5 mins and retrying. QODBC connection closed.\n")
            return None,True
    
    def dsn_connection(run_connection, run_query,table,itr_count):
        

        try:
            logger.info(f' >> Executing chunk {itr_count} for the table {table}-->\n')
            sql_query = run_query
            out_df = pl.read_database(sql_query, run_connection)
            return out_df,False
            
        except Exception as e:
            logger.error(f" >> Error in dsn while executing query {run_query} : {e}\n")
            run_connection.close()
            return None,True

    def decode_bucket_cred(encoded_str):
        
        try: 
            decoded_bytes = base64.b64decode(encoded_str)
            decoded_str = decoded_bytes.decode('utf-8')
            return decoded_str
        except Exception as e:
            logger.error(f" >> \t Error in decoding the bucket with key {encoded_str} : {str(e)}\n")
            return None     
     
    def split_query_by_month(query,type,current_date_flag):

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
        try:
            delta_table = DeltaTable(table_path , storage_options=storage_options)
            delta_table.restore(backup_version)
            latest_version = delta_table.history()[0]
            logger.info(f"Updating the latest version for the table {table_name} to => {latest_version['version']}")
            return latest_version['version']
        
        except Exception as e:
            logger.error(f'Error in getting version for the table {table_name} => {str(e)}\n')
            return 
        

    def email_process(load_failed, config_cred_path, file_path):
        
        result = ""
        config_file_path = config_cred_path
        with open(config_file_path, 'r') as file:
            config = yaml.safe_load(file) 
        cred=decode_bucket_cred(config['bucket_cred']['bucket_key'])
        json_key = decode_bucket_cred(config['bucket_cred']['bucket_key'])
        cred = json.loads(json_key)
        storage_options = {'service_account_key' : json.dumps(cred)}
        client = storage.Client.from_service_account_info(cred)
        server_name = config['qb_cred']['server_name']
        bucket_name=config['bucket_cred']['bucket_name']
        orgid=config['bucket_cred']['orgid']
        datasetid = config['bucket_cred']['datasetid']
        bucket = client.bucket(bucket_name)
        base_path=f"gs://{bucket_name}/data/{orgid}/{datasetid}/parquet"
        excel_cred_path = f'{orgid}/{datasetid}/config/iconfig.yml'
        blob = bucket.blob(excel_cred_path)
        yaml_data = blob.download_as_bytes()
        config = yaml.safe_load(yaml_data)
        credential = config['mail_cred']
        send_mail_if_success = True if config['send_mail_for_all_success'] == 'True' else False

        # Upload log file to bucket
        log_path_in_bucket = f'{orgid}/{datasetid}/quickbooks_log/{log_filename}'
        log_blob = bucket.blob(log_path_in_bucket)
        log_blob.upload_from_filename(file_path)
        logger.info('>> Log file uploaded to bucket.\n')
        
        
        # Email configuration
        sender_email = credential['sender_email']
        smtp_host = credential['smtp_host']
        smtp_port = credential['smtp_port']
        smtp_username = credential['smtp_username']
        smtp_password = credential['smtp_password']
        receiver_emails = credential['emailto']


        receiver_emails = list(set(receiver_emails))
        if load_failed:
            subject = f'Failed QuickBooks load from server {server_name}' 
        else:
            subject = f"Successfull QuickBooks load from server {server_name}" 
        message = f"Hello,\n\n Please find attached the text file containing the log status from the client system for the server {server_name}.\n\n Thanks,\n\n Team Conversight"  # change body here

        # Create a message object
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = sender_email
        msg["To"] = ", ".join(receiver_emails)
        msg.attach(MIMEText(message, "plain"))

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

    async def connect_to_qodbc(dsn_name, max_retries=3, timeout=600):
        retries = 0
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
                logger.info(f'>> Error in Initialing Quickbooks => {str(e1)}\n')
                logger.info(f"Attempt {retries}: unable to connect quickbooks through pyodbc..Retrying in 5 minutes...")
                await asyncio.sleep(300) 
            except Exception as e:
                logger.error(f'Unexpected error while Quickbooks connection => {e}\n')
                return None, True 
             
        logger.error(f'Max retries reached. Connection failed..\n')
        return None, True

    async def main():
        global load_failed
        if not os.path.exists(config_file_path):
            logger.info(f" >> Config file does not exist in the path => {config_file_path}\n")
            return
        logger.info(f'Config file fetched from path => {config_file_path}\n')
        with open(config_file_path, 'r') as file:
            config = yaml.safe_load(file)  
        
        dsn_name = config['qb_cred']['dsn_name']
        server_name = config['qb_cred']['server_name']
        

        try: 
            
            connection, failed = await connect_to_qodbc(dsn_name)
            if failed :
                logger.info(f" >> Quickbooks connection failed even after the retries..Terminating the code..\n")
                load_failed = True
                return
            cred = decode_bucket_cred(config['bucket_cred']['bucket_key'])
            json_key = decode_bucket_cred(config['bucket_cred']['bucket_key'])
            cred = json.loads(json_key)
            storage_options = {'service_account_key' : json.dumps(cred)}
            client = storage.Client.from_service_account_info(cred)
            bucket_name = config['bucket_cred']['bucket_name']
            orgid = config['bucket_cred']['orgid']
            datasetid = config['bucket_cred']['datasetid']
            bucket = client.bucket(bucket_name)
            base_path = f"gs://{bucket_name}/data/{orgid}/{datasetid}/parquet"
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
            Spreadsheet_name = config['excel_cred']['name']
            worksheet_name = config['server_mapping'][server_name]
            logger.info(f'>> Fetching QuickBooks data from server {server_name} \n')
            
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
                delta_version_column = config_df.columns.index('Delta_version')+1
                start_date_column = config_df.columns.index('Start_Datetime')+1
                audit_column = row[config_df.columns.index('Audit_Column')].strip()
                backup_version = row[config_df.columns.index('Delta_version')]
                current_date_flag=row[config_df.columns.index('Current_Date_Flag')]

                if backup_version == '':
                    backup_version = -1
                version = int(backup_version)

                load_config.update_cell(row_count,run_status_column,'Running')

                overwrite = True
                table_path =  f"{base_path}/{table_name}"

                if load_type.lower() == 'delta' and table_type.lower() == 'table':
                    start_date = datetime.strptime(load_start_time, "%Y-%m-%d %H:%M:%S")
                    start_date = start_date - timedelta(days=1)
                    start_date = start_date.strftime('%Y-%m-%d %H:%M:%S.%f')
                    currenttime = datetime.now()
                    if 'where' in source_query.lower():
                        source_query = source_query + f" AND {table_name}.{audit_column} between {{ts'{start_date}'}} and {{ts'{currenttime.strftime('%Y-%m-%d %H:%M:%S.%f')}'}}"
                    else:
                        source_query = source_query + f" where {table_name}.{audit_column} between {{ts'{start_date}'}} and {{ts'{currenttime.strftime('%Y-%m-%d %H:%M:%S.%f')}'}}"

                load_start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                query_list = split_query_by_month(source_query, table_type,current_date_flag) 
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
                    
                    while executions <= retry_times and enable_retry:
                        df, enable_retry = await read_database(connection,query,table_name,itr_count) 

                        if df is None and enable_retry:
                            logger.info(f">> Retrying the same query for table {table_name} as failing in passing query through pyodbc\n")
                            enable_retry = True
                            connection, failed = await connect_to_qodbc(dsn_name)
                            if failed :
                                logger.info(f" >> Quickbooks connection failed even after the retries..Terminating the code..\n")
                                logger.info(f" >> Failed to load {table_name}. Updating the delta table to the backup version {backup_version}")
                                load_config.update_cell(row_count,run_status_column,'Failed')  
                                upd_version = setVersion(table_name,table_path,storage_options,backup_version)
                                load_config.update_cell(row_count,delta_version_column,upd_version)
                                break
                        
                        elif df is None:
                            logger.info(f">> Interrupting the code as the query failed executing\n")
                            brk_toggle = True
                            break

                        elif df.shape[0]==0:
                            connection.close()
                            logger.info(f" >> Retrying the same query for table {table_name} after 3 minutes, as an empty dataframe was received during the previous attempt...Initiating the Quicbooks connection again\n")
                            time.sleep(180)
                            enable_retry = True
                            connection, failed = await connect_to_qodbc(dsn_name)
                            if failed :
                                logger.info(f" >> Quickbooks connection failed even after the retries..Terminating the code..\n")
                                logger.info(f" >> data load not done for table {table_name}..updating the delta table to the backup version ie {backup_version}")
                                load_config.update_cell(row_count,run_status_column,'Failed')  
                                upd_version=setVersion(table_name,table_path,storage_options,backup_version)
                                load_config.update_cell(row_count,delta_version_column,upd_version)
                                brk_toggle = True
                                break
                            

                        else :    
                            
                            logger.info(f">> Data successfully retrieved for table {table_name} with count {df.shape}\n")
                            res = load_data(df,primary_columns,table_path,overwrite,load_type,storage_options)
                            if res['status'] == 'success' and itr_count == total_chunks:
                                load_config.update_cell(row_count,run_status_column,'Success')
                                itr_toggle = False
                            if res['status'] == 'success':
                                version = version + 1
                                

                        executions = executions + 1 

                        if executions > retry_times and df is None:
                            logger.info(f" >> Maximum retries reached for the query {query}\n")
                            load_failed = True
                            logger.info(f" >> data load not done for table {table_name}..updating the delta table to the backup version ie {backup_version}")
                            load_config.update_cell(row_count,run_status_column,'Failed')  
                            upd_version=setVersion(table_name,table_path,storage_options,backup_version)
                            version = upd_version
                            load_config.update_cell(row_count,delta_version_column,upd_version)
                            brk_toggle = True
                            break

                        if itr_count == total_chunks and itr_toggle:
                            load_config.update_cell(row_count,run_status_column,"Last chunk completed but not uploaded to delta")

                    overwrite = False 

                load_end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                load_config.update_cell(row_count, start_date_column+1, load_end_time)
                load_config.update_cell(row_count,delta_version_column,version)

        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f'>> Spreadsheet "{Spreadsheet_name}" not found or access denied \n')
            load_failed = True
            return
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f'>> Worksheet "{worksheet_name}" not found in spreadsheet "{Spreadsheet_name}" \n')
            load_failed = True
            return
        except Exception as e:
            logger.error(f">> Error in the connection part to the query => {str(e)}\n")
            return
        
    if __name__ == "__main__":
        try:
            loop = asyncio.get_running_loop()  # Check if an event loop is running
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            logger.info(f" >> Function started and running in notebook\n")
            asyncio.create_task(main())
        else:
            logger.info(f" >> Function started with a new event loop\n")
            asyncio.run(main())

except Exception as e:
    logger.error(f" >> Encountered error \n{e}\n")
    

finally:
    logger.info(f" >> Code execution completed and ready to send mail\n")
    email_process(load_failed, config_file_path, log_filepath)
    
