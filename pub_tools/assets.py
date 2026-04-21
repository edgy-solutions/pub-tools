import csv
import os
import tempfile
import zipfile
from datetime import datetime
from typing import Iterator

import dlt
import requests
from dagster import AssetExecutionContext, MaterializeResult, asset

# URL for the DLA PUB LOG flat files (you might want to make this configurable)
DLA_PUBLOG_URL = "https://www.dla.mil/Portals/104/Documents/LogisticsOps/FLIS/PUBLOG_FLIS_Data_Flat_Files.zip"

def download_and_extract_publog(url: str, extract_dir: str):
    """Downloads the DLA PUB LOG zip file and extracts it."""
    zip_path = os.path.join(extract_dir, "publog.zip")
    
    # Download the file
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    response = requests.get(url, stream=True, headers=headers)
    response.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            
    # Extract the zip file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        
    return extract_dir

def clean_string_for_parquet(s: str) -> str:
    """Basic string cleaning for Parquet compatibility (e.g., headers)."""
    return s.strip().replace(" ", "_").replace(".", "_").replace("-", "_").replace("/", "_").lower()

@dlt.resource(name="publog_ref_data", write_disposition="replace")
def publog_resource(files_dir: str) -> Iterator[dict]:
    """Streams the pipe-delimited REF.txt and other files, yielding cleaned rows."""
    
    # Iterate through all .txt files in the extracted directory
    for root, _, files in os.walk(files_dir):
        for file in files:
            if file.lower().endswith('.txt'):
                file_path = os.path.join(root, file)
                table_name = os.path.splitext(file)[0].lower() # e.g., 'ref', 'vndr'
                
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    # Read the first line to get headers
                    header_line = f.readline()
                    raw_headers = header_line.split('|')
                    clean_headers = [clean_string_for_parquet(h) for h in raw_headers]
                    
                    # Ensure no trailing empty header from trailing pipe
                    if clean_headers and clean_headers[-1] == "":
                        clean_headers = clean_headers[:-1]
                        
                    # Reset to beginning, read with DictReader
                    f.seek(0)
                    reader = csv.DictReader(f, fieldnames=clean_headers, delimiter='|', quoting=csv.QUOTE_NONE)
                    
                    # skip the header row
                    next(reader)
                    
                    for row in reader:
                        # Yield the row associated with its table name for dlt routing
                        # We use dlt.mark.with_table_name to dynamically route the records to their respective tables.
                        yield dlt.mark.with_table_name(row, table_name)



