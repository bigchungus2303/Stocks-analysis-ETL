import os

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Snowflake connection string template
SQLALCHEMY_EXAMPLES_URI = 'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}'

# Snowflake connection parameters
SNOWFLAKE_USER = ''
SNOWFLAKE_PASSWORD = ''
SNOWFLAKE_ACCOUNT = ''  # e.g., xy12345.us-east-1
SNOWFLAKE_DATABASE = ''
SNOWFLAKE_SCHEMA = ''
SNOWFLAKE_WAREHOUSE = ''
SNOWFLAKE_ROLE = ''

# Security configurations
SECRET_KEY = os.environ.get('', '')
SQLALCHEMY_TRACK_MODIFICATIONS = False
WTF_CSRF_ENABLED = True

# Cache configurations
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
}