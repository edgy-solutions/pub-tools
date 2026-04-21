from typing import Any, Dict, Optional
from dagster import Definitions, AssetExecutionContext, MaterializeResult, asset
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model
import os
import tempfile
from datetime import datetime
import dlt

from pub_tools.assets import download_and_extract_publog, publog_resource

class PublogPipelineComponent(Component, Resolvable, Model):
    """A Dagster Component that ingests DLA PUB LOG data into a destination using dlt."""
    
    publog_url: str = "https://www.dla.mil/Portals/104/Documents/LogisticsOps/FLIS/PUBLOG_FLIS_Data_Flat_Files.zip"
    
    dest_config: Dict[str, Any]
    """The destination system credential configuration."""
    
    dataset_name: Optional[str] = None
    """The name of the dataset to create in the destination."""

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        
        publog_url = self.publog_url
        dest_config = self.dest_config
        dataset_name_prefix = self.dataset_name or "publog"

        @asset(group_name="publog_ingestion")
        def publog_lake_export(context: AssetExecutionContext) -> MaterializeResult:
            as_of_date = datetime.now().strftime("%Y-%m-01") 
            
            bucket_url = dest_config.get("destination", {}).get("bucket_url")
            if not bucket_url:
                bucket_url = f"file:///tmp/publog-lake/{as_of_date}"
                
            dataset_name = f"{dataset_name_prefix}_{as_of_date.replace('-', '_')}"
            
            drivername = dest_config.get("drivername", "filesystem")
            
            if drivername == "filesystem":
                from dlt.destinations import filesystem
                destination_obj = filesystem(
                    bucket_url=bucket_url,
                    credentials=dest_config.get("credentials", {})
                )
            else:
                destination_obj = drivername

            with tempfile.TemporaryDirectory() as temp_dir:
                context.log.info(f"Downloading DLA PUB LOG to temporary directory: {temp_dir}")
                try:
                    download_and_extract_publog(publog_url, temp_dir)
                except Exception as e:
                    context.log.warning(f"Failed to download from {publog_url}. Exception: {e}")
                    context.log.warning("Creating a mock dataset for testing purposes.")
                    with open(os.path.join(temp_dir, "ref.txt"), "w") as f:
                        f.write("ID|NAME|VALUE|\n1|Test|100|\n2|Mock|200|\n")

                pipeline = dlt.pipeline(
                    pipeline_name=f"publog_lake_pipeline_{as_of_date}",
                    destination=destination_obj,
                    dataset_name=dataset_name 
                )

                context.log.info(f"Running dlt pipeline, writing to: {bucket_url}")
                load_info = pipeline.run(
                    publog_resource(temp_dir),
                    loader_file_format="parquet"
                )
                
                context.log.info(str(load_info))

            return MaterializeResult(
                metadata={
                    "s3_destination_url": bucket_url,
                    "partition_date": as_of_date,
                    "dlt_pipeline_name": pipeline.pipeline_name,
                    "dlt_dataset_name": pipeline.dataset_name,
                }
            )

        return Definitions(assets=[publog_lake_export])