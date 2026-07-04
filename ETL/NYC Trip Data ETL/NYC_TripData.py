import pandas as pd
import os
from datetime import datetime, time, timedelta, timezone
import pyarrow.parquet as pq
import urllib.request
from io import BytesIO
from readers import snowflake_reader as sf
import Logging

"""
data pipeline:
1. pull data (get_data)
2. load data into bucket 
3. verify data
4. transform data
5. load data into dw
6. final verify
"""

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def main():
        
    try:
        sf.load_snowflake()
    except Exception:
        logger.exception(f"An error occurred while reading from source")
        raise
    


	







