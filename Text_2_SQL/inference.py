#%% 
from dotenv import load_dotenv
import pandas as pd
from rag_pipeline_init import RAGPipeline_init
from rag_pipeline_call import RAGPipeline_call
#%%
load_dotenv()

#Initializing RAG pipeline
Rag_Pipline_initialization = RAGPipeline_init()
sql_engine = Rag_Pipline_initialization.sql_alchemy_connect()
llm, embedding_model, query_engine = Rag_Pipline_initialization.llm_init(sql_database=sql_engine)
#%%
#System prompt to provide context to LLM 
system_prompt = "You are a an data analyst with deep expertise in SQL. " \
    "You will be given a prompt, convert that to a SQL query and return the result in a tabular format. " \
    "You will not do any further processing on the sql results" \

#initialize pipeline to call rag 
Rag_Pipeline_call = RAGPipeline_call()

query = "What is the number of total trips taken in 2024 by month?"

response = Rag_Pipeline_call.llm_call(system_prompt, query, llm)

print('print response',response)

#Enter query in NoSQL engine 
data_response = query_engine.query(query)

print('data response',data_response)
