# Bike Share Data Analysis and Text-to-SQL Project

## Overview

This project performs data analysis on bike share data, potentially using a Large Language Model (LLM) to translate natural language queries into SQL for querying a database. It involves data cleaning, storage in cloud services (GCS and S3), and integration with LLMs for question answering.

## File Structure

*   **`bikeshare_data_processor.py`**:  The main script for processing the bike share data.  This likely handles data loading, cleaning, and storage.
*   **`data_cleaning.py`**:  Contains the `bike_share_data_clean` class, responsible for cleaning and transforming the bike share data.  It also includes functionality for uploading data to Google Cloud Storage (GCS) and Amazon S3.
*   **`rag_pipeline.py`**: Implements a Retrieval-Augmented Generation (RAG) pipeline. This likely involves using an LLM to answer questions about the bike share data, potentially by converting natural language questions into SQL queries.
*   **`requirements.txt`**:  Lists all the Python packages required to run the project.
*   **`.env` (Not explicitly listed, but likely present):** Stores sensitive information such as API keys, cloud credentials, and bucket names. **Important: This file should NOT be committed to version control.**

## Key Libraries Used

The project utilizes a wide range of Python libraries, including:

*   **Data Manipulation and Analysis:**
    *   `pandas`: For working with DataFrames.
    *   `numpy`: For numerical computations.
*   **Cloud Storage:**
    *   `boto3`: For interacting with Amazon S3.
    *   `google-cloud-storage`: For interacting with Google Cloud Storage.
*   **LLM and RAG:**
    *   `langchain`: For building LLM-powered applications.
    *   `llama-index`: A framework for building LLM-based applications over structured and unstructured data.
    *   `google-genai`: For using Google's generative AI models (e.g., Gemini).
    *   `langchain-google-vertexai`: Integration between Langchain and Google Vertex AI.
*   **SQLAlchemy:** For interacting with databases.
    *   `SQLAlchemy`: Core SQL toolkit and ORM.
    *   `sqlalchemy-bigquery`:  SQLAlchemy dialect for Google BigQuery.
*   **Other:**
    *   `python-dotenv`: For loading environment variables from a `.env` file.
    *   `requests`: For making HTTP requests.
    *   `tqdm`: For displaying progress bars.
    *   `streamlit`: For creating interactive web applications.

## Setup and Installation

1.  **Clone the repository:**

    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **Create a virtual environment (recommended):**

    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Linux/macOS
    .venv\Scripts\activate  # On Windows
    ```

3.  **Install the dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables:**

    *   Create a `.env` file in the project root.
    *   Add the necessary environment variables to the `.env` file.  This will likely include:
        *   `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (for S3 access)
        *   `GCS_BUCKET_NAME` (for Google Cloud Storage)
        *   `GOOGLE_APPLICATION_CREDENTIALS` (Path to your Google Cloud service account credentials JSON file)
        *   `PROJECT_ID` (Google Cloud Project ID)
        *   `DATASET_ID` (Google BigQuery Dataset ID)
        *   `TABLE_ID` (Google BigQuery Table ID)
        *   `GOOGLE_AI_STUDIO_API_KEY` (API key for Google AI Studio)
    *   **Important:**  Do not commit the `.env` file to version control.

## Usage

1.  **Run the data processing script:**

    ```bash
    python bikeshare_data_processor.py
    ```

    This will load, clean, and store the bike share data.

2.  **Run the RAG pipeline (if applicable):**

    ```bash
    python rag_pipeline.py
    ```

    This will initialize the LLM and allow you to ask questions about the data.

## Contributing

[Add information about how others can contribute to your project.]

## License

[Add license information.]