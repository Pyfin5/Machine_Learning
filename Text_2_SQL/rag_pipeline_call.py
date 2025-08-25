
import os 
from sqlalchemy.engine import create_engine
from sqlalchemy import text
from functools import reduce

from IPython.display import Markdown, display
import pandas as pd
from google.oauth2 import service_account

from dotenv import load_dotenv

from llama_index.core.llms import ChatMessage
from rag_pipeline_init import RAGPipeline_init

load_dotenv()  # Load environment variables from .env file


class RAGPipeline_call:
    def __init__(self):
        """Initialize the RAG pipeline with necessary configurations"""
        self.credentials_path = os.path.join(os.path.dirname(__file__), 'GOOGLE_APPLICATION_CREDENTIALS.json') #Place GCP credentials in the same directory 
        self.credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
        
        self.oath_api_path = os.path.join(os.path.dirname(__file__), 'oauth_api.json')
        #self.oath_api = service_account.Credentials.from_service_account_file(self.oath_api_path)

        self.bucket_name = os.getenv('GCS_BUCKET_NAME')  # GCS bucket name from environment variable
        self.project_id = os.getenv('PROJECT_ID')  # Project ID from environment variable
        self.dataset_id = os.getenv('DATASET_ID')  # Dataset ID from environment variable
        self.table_id = os.getenv('TABLE_ID')  # Table ID from environment variable
        self.dataset_url = os.getenv('DATASET_URL')  # Dataset URL from environment variable
        print("dataset_url:", self.dataset_url)
    
    
    def llm_call(self, system_prompt, user_prompt, llm):
        """Call the LLM with the provided prompts"""
        messages = [ChatMessage(role="system", content=system_prompt),
                    ChatMessage(role="user", content=user_prompt)]
        response = llm.chat(messages)
        return response

