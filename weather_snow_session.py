import logging
import json
from snowflake.snowpark import Session

# Set Logging Level
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Load creds 
connection_params = {
    "connection_name": "default"
}

## SNOWFLAKE CONNECTION
session = None
def get_client():
    global session
    if session is None:
        try:
            session = Session.builder.configs(connection_params).create()
            print("Connection established")
        except Exception as ex:
            logger.error('Failed to connect: ' + str(ex))
            raise
    return session