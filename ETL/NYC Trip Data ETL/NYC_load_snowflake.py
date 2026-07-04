import snowflake.sqlalchemy as sf
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)

source_url = env.get('SOURCE_URL', '')

def load_snowflake(source_url=source_url):


	try:
		sf.load_snowflake(source_url)
  
		sf.count_records()
 
		logger.info(f"Data loaded successfully into Snowflake with {sf.count_records()} records.")
	
	except Exception:
		logger.exception(f"An error occurred while reading from source")
		raise