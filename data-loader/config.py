import os
from dotenv import load_dotenv

load_dotenv()

# Input Configuration
LOCAL_DATA_PATH = os.getenv('LOCAL_DATA_PATH', '/app/data')  # Path to extracted data directory
TEMP_UPLOAD_DIR = '/app/temp-uploads'  # Directory for temporary uploaded files

# Elasticsearch Configuration
ELASTICSEARCH_URL = os.getenv('ELASTICSEARCH_URL', 'http://elasticsearch:9200')
ELASTICSEARCH_INDEX = os.getenv('ELASTICSEARCH_INDEX', 'dsfp-screening-index')

# Data Processing Configuration
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))  # Number of documents to bulk insert
DATA_DIR = '/app/data'
MAX_FILE_SIZE_MB = int(os.getenv('MAX_FILE_SIZE_MB', '500'))  # Skip files larger than this
