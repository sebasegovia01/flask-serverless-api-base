# app/config/config.py
import os
import json
import base64
import logging
from dotenv import load_dotenv

# Cargar las variables del archivo .env
load_dotenv()

class Config:
    GCP_CREDENTIALS_BASE64 = os.getenv("GCP_CREDENTIALS")
    ENV = os.getenv("ENV", "development")

    @staticmethod
    def get_gcp_credentials():
        if Config.GCP_CREDENTIALS_BASE64:
            try:
                credentials_json = base64.b64decode(Config.GCP_CREDENTIALS_BASE64).decode('utf-8')
                credentials = json.loads(credentials_json)
                logging.debug(f"GCP Credentials loaded successfully. Project ID: {credentials.get('project_id')}")
                return credentials
            except Exception as e:
                logging.error(f"Error decoding or parsing GCP credentials: {str(e)}")
                return None
        else:
            logging.warning("GCP_CREDENTIALS environment variable is not set")
            return None