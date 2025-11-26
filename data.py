import os
import logging

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def extract_data(file_path, file_type):
    logging.info(f"Starting extraction: {file_path} ({file_type})")

    if not os.path.exists(file_path):
        logging.error("File does not exist")
        raise FileNotFoundError("File does not exist")

    if file_type == "CSV":
        df = pd.read_csv(file_path)
    elif file_type == "Excel":
        df = pd.read_excel(file_path)
    elif file_type == "JSON":
        df = pd.read_json(file_path)
    else:
        raise ValueError("Unsupported file type")

    logging.info("Extraction successful")
    return df

def create_db_engine(db_url: str):
    logging.info("Creating DB engine")
    engine = create_engine(db_url)
    return engine

def load_data_to_db(df, table_name, engine):
    logging.info(f"Loading data into table: {table_name}")
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info("Data loaded successfully")

def main():
    st.title("Data Extraction & Load Tool")

    uploaded_file = st.file_uploader("Choose a file", type=["csv", "xlsx", "json"])
    file_type = st.selectbox("File Type", ["CSV", "Excel", "JSON"])

    st.header("Database Connection")
    st.markdown("""
    **Example MySQL URL**
    `mysql+pymysql://root:password@localhost:3306/etl_db`
    """)

    db_url = st.text_input("SQLAlchemy Database URL")
    table_name = st.text_input("Destination Table Name", value="classification_data")

    if st.button("Extract & Load"):
        st.write("Processing... Please wait")

        if uploaded_file is None:
            st.error("Please upload a file.")
            return
        if not db_url:
            st.error("Please enter database URL.")
            return

        try:
            temp_file_path = "temp_file.csv"
            with open(temp_file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())

            df = extract_data(temp_file_path, file_type)
            engine = create_db_engine(db_url)
            load_data_to_db(df, table_name, engine)

            st.success("Data extracted and loaded successfully!")
            st.dataframe(df.head())

            os.remove(temp_file_path)

        except Exception as e:
            st.error(f"Error: {e}")

if __name__ == "__main__":
    main()
