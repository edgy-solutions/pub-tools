import s3fs
import pyarrow.parquet as pq
import sys
import os

def verify():
    endpoint_url = os.environ.get('AWS_ENDPOINT_URL', 'http://minio:9000')
    access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'minioadmin')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'minioadmin')
    
    print(f"Connecting to MinIO at {endpoint_url}...")
    fs = s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={'endpoint_url': endpoint_url}
    )
    
    bucket = "publog-lake"
    
    try:
        # Check if bucket exists and list contents
        if not fs.exists(bucket):
            print(f"Bucket {bucket} does not exist. Did you run the pipeline?")
            sys.exit(1)
            
        partitions = fs.ls(bucket)
        if not partitions:
            print(f"No data found in bucket {bucket}. Did you run the pipeline?")
            sys.exit(1)
            
        # Filter for directories (datasets)
        datasets = [p for p in partitions if 'publog_' in p]
        if not datasets:
            print(f"No publog datasets found in {partitions}")
            sys.exit(1)
            
        dataset_path = datasets[0]
        print(f"Found dataset: {dataset_path}")
        
        # Check tables inside the dataset
        tables = fs.ls(dataset_path)
        print(f"Tables found: {[t.split('/')[-1] for t in tables]}")
        
        # Verify all tables found in the dataset
        for table_path in tables:
            table_name = table_path.split('/')[-1]
            print(f"\n=========================================")
            print(f"Inspecting Table: {table_name}")
            print(f"=========================================")
            
            files = fs.ls(table_path)
            parquet_files = [f for f in files if f.endswith('.parquet')]
            
            if not parquet_files:
                print(f"⚠️ No parquet files found in {table_path}")
                continue
                
            file_to_read = parquet_files[0]
            print(f"--- Verifying {file_to_read} ---")
            
            # Read the parquet file metadata and schema
            with fs.open(file_to_read, 'rb') as f:
                parquet_file = pq.ParquetFile(f)
                
                print("\nSchema (Columns):")
                for name in parquet_file.schema.names:
                    print(f" - {name}")
                    
                num_rows = parquet_file.metadata.num_rows
                print(f"\nTotal Rows: {num_rows}")
                print(f"Total Row Groups: {parquet_file.metadata.num_row_groups}")
                
                # Simple assertions
                assert num_rows > 0, f"Parquet file {file_to_read} is empty!"
                assert len(parquet_file.schema.names) > 0, f"Parquet file {file_to_read} has no columns!"
                
                print("\nFirst few rows:")
                table = parquet_file.read()
                for row in table.to_pylist()[:5]:
                    print(row)
                
        print("\n✅ Verification successful! All Parquet files are valid and contain data.")
            
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    verify()
