import os
from dotenv import load_dotenv

# Load test environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env.test'))
