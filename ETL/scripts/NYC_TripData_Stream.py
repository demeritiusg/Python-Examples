import urllib.request
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy import exc
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# part 1 landing data
# connect to data source 1 (csv) pandas
# connect to data source 2 (parquet) spark
# connect to data source 3 (some cloud resource) kafka

# part 2 cleaning data
"""
after data is landed by source 1 and 2, cleaning will commence. source 3 will be cleaned in near real time and landed in part 3
"""

# part 3 enriched data
"""
cleaned data ready for use in cloud data stores/data warehouses
"""




