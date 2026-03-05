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
    response = requests.get(url, stream=True)
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


@asset(group_name="publog_ingestion")
def publog_lake_export(context: AssetExecutionContext) -> MaterializeResult:
    """
    Downloads the entire DLA PUB LOG dataset, extracts it, and streams the 
    pipe-delimited files into an S3 Data Lake using Parquet via dlt.
    """
    # 1. Configuration & Partitioning
    # Try to find a logical "as-of" date. We'll use the current month/year for partitioning
    # since PUB LOG is released monthly.
    as_of_date = datetime.now().strftime("%Y-%m-01") 
    
    # Fallback to a local path if S3 bucket isn't defined for testing
    bucket_url = os.environ.get("PUBLOG_S3_BUCKET_URL", f"file:///tmp/publog-lake/{as_of_date}")
    
    # 2. Download and Extract
    with tempfile.TemporaryDirectory() as temp_dir:
        context.log.info(f"Downloading DLA PUB LOG to temporary directory: {temp_dir}")
        try:
            download_and_extract_publog(DLA_PUBLOG_URL, temp_dir)
        except Exception as e:
            context.log.warning(f"Failed to download from {DLA_PUBLOG_URL}. Exception: {e}")
            context.log.warning("Please ensure the URL is correct or provide a valid one if it has changed.")
            raise

        # 3. Create dlt Pipeline
        # We partition by the "as-of-date" by including it in the destination path or pipeline name.
        pipeline = dlt.pipeline(
            pipeline_name=f"publog_lake_pipeline_{as_of_date}",
            destination=dlt.destinations.filesystem(bucket_url=bucket_url),
            dataset_name=f"publog_{as_of_date.replace('-', '_')}" 
        )

        # 4. Run dlt Pipeline
        context.log.info(f"Running dlt pipeline, writing to: {bucket_url}")
        # Configure file format to Parquet
        # Dlt automatically creates tables for each table name yielded by the resource
        load_info = pipeline.run(
            publog_resource(temp_dir),
            loader_file_format="parquet"
        )
        
        context.log.info(str(load_info))

    # 5. Metadata reporting
    total_row_count = sum(pkg.jobs.get("completed_jobs", [{}])[0].get("file_size", 0) if pkg.jobs else 0 for pkg in load_info.load_packages) # Simplified metric approximation if row count isn't directly exposed
    # For a more accurate row count from dlt, we'd query the pipeline, but we can return basic loaded stats
    
    return MaterializeResult(
        metadata={
            "s3_destination_url": bucket_url,
            "partition_date": as_of_date,
            "dlt_pipeline_name": pipeline.pipeline_name,
            "dlt_dataset_name": pipeline.dataset_name,
        }
    )
