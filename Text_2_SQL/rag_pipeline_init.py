#%%
from llama_index.core import SQLDatabase, VectorStoreIndex
import os 
from sqlalchemy import MetaData, Table, Select, Column, Integer, String, DateTime, TIMESTAMP, Float  # SQLAlchemy for database operations
from sqlalchemy.engine import create_engine
from sqlalchemy import text
import pandas as pd  # Data manipulation library 
from functools import reduce
import sys
from IPython.display import Markdown, display
import pandas as pd
import vertexai
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
from llama_index.llms.google_genai import GoogleGenAI
from dotenv import load_dotenv
import chromadb
from chromadb.utils import embedding_functions
from llama_index.core.query_engine import NLSQLTableQueryEngine
from llama_index.core.llms import ChatMessage


#%%


load_dotenv()  # Load environment variables from .env file




class RAGPipeline_init:
    def __init__(self):
        """Initialize the RAG pipeline with necessary configurations"""
        self.credentials_path = os.path.join(os.path.dirname(__file__), 'GOOGLE_APPLICATION_CREDENTIALS.json') #Place GCP credentials in the same directory 
        self.credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
        
        self.oath_api_path = os.path.join(os.path.dirname(__file__), 'oauth_api.json')
    
        self.bucket_name = os.getenv('GCS_BUCKET_NAME')  # GCS bucket name from environment variable
        self.project_id = os.getenv('PROJECT_ID')  # Project ID from environment variable
        self.dataset_id = os.getenv('DATASET_ID')  # Dataset ID from environment variable
        self.table_id = os.getenv('TABLE_ID')  # Table ID from environment variable
        self.dataset_url = os.getenv('DATASET_URL')  # Dataset URL from environment variable
        self.google_ef = embedding_functions.GoogleGenerativeAiEmbeddingFunction(api_key=os.getenv('GOOGLE_AI_STUDIO_API_KEY'))
        print("dataset_url:", self.dataset_url)

    def sql_alchemy_connect(self):

        bqengine = create_engine(f"bigquery://{self.project_id}/{self.dataset_id}", 
                                 credentials_path=self.credentials_path)
        
        sql_database = SQLDatabase(bqengine)

        print("Connected to BigQuery using SQLAlchemy")
        with bqengine.connect() as con:
            rows = con.execute(text(f"SELECT * from {self.project_id}.{self.dataset_id}.bikeshare_data LIMIT 20"))
            for row in rows:
                if row is None:
                    print("No data found in the table.")
                else: 
                    print(row)
            print("Executed SQL query successfully",rows)
        return sql_database

    def vertexai_connect(self, project_id, location):
        """Connect to Vertex AI"""
        vertexai.init(project=self.credentials['project_id'])
        print("Connected to Vertex AI")

    

    def llm_init(self, sql_database): 
        
        model_kwargs={
                "max_tokens": 10000,
                "temperature": 0.2
            }

        llm = GoogleGenAI(model = "gemini-2.5-flash",
                    api_key=os.getenv('GOOGLE_AI_STUDIO_API_KEY'),
                    **model_kwargs
                    )
       
        query_engine = NLSQLTableQueryEngine(sql_database=sql_database,
                                             llm=llm,
                                             embed_model=self.google_ef,
                                             tables=['bikeshare_data'],
                                             synthesize_response=True, 
                                             verbose = True)  
        #llm = Bedrock(model_id="anthropic.claude-3-5-sonnet-20240620-v1:0", model_kwargs=model_kwargs)
        return llm, self.google_ef, query_engine
    

# %%
