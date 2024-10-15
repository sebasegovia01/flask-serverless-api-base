# app/config/config.py
import os
import json
import base64
from dotenv import load_dotenv

# Cargar las variables del archivo .env
load_dotenv()

class Config:
    GCP_CREDENTIALS_BASE64 = os.getenv("GCP_CREDENTIALS")
    ENV = os.getenv("ENV", "development")

    @staticmethod
    def get_gcp_credentials():
        if Config.GCP_CREDENTIALS_BASE64:
            credentials_json = base64.b64decode(Config.GCP_CREDENTIALS_BASE64).decode('utf-8')
            return json.loads(credentials_json)
        return None