import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# Function to clean and transform the data
def clean_data(data):
    # Strip whitespace from column names
    data.columns = data.columns.str.strip()

    # Target column to rename
    target_column = 'VALUE (₹ Crores)'
    similar_columns = [col for col in data.columns if 'VALUE' in col]

    # Rename similar column
    if similar_columns:
        data.rename(columns={similar_columns[0]: target_column}, inplace=True)

    # Convert 'VOLUME' to int
    if 'VOLUME' in data.columns:
        data['VOLUME'] = pd.to_numeric(data['VOLUME'].str.replace(',', '', regex=False), errors='coerce').fillna(
            0).astype(int)

    # Convert target column to float
    if target_column in data.columns:
        data[target_column] = data[target_column].astype(str).str.replace(',', '', regex=False).replace('nan', '0')
        data[target_column] = pd.to_numeric(data[target_column], errors='coerce').fillna(0).astype(float)

    # Clean numeric columns
    numeric_columns = ['OPEN', 'HIGH', 'LOW', 'PREV. CLOSE', 'LTP', 'CHNG', '%CHNG',
                       'VALUE (₹ Crores)', 'NAV', '52W H', '52W L', '30D  %CHNG', '365 D', '365 D % CHNG', '30 D']

    for col in numeric_columns:
        if col in data.columns:
            data[col] = data[col].astype(str).str.replace(',', '', regex=False)
            data[col] = pd.to_numeric(data[col], errors='coerce')

    return data


# Function to insert data into PostgreSQL database
def insert_data_to_db(data):
    try:
        connection = psycopg2.connect(
            "postgresql://nseetfdb009_user:5RV0z23H9w1DvirLnQeLUhQv39itnHQA@dpg-cse90um8ii6s738ut7e0-a.oregon-postgres.render.com/nseetfdb009"
        )
        cursor = connection.cursor()

        # Prepare list of tuples for bulk insert
        data_tuples = [tuple(x) for x in data.to_numpy()]

        # SQL insert query
        insert_query = """
        INSERT INTO metf_2024 (
            SYMBOL, UNDERLYING_ASSET, OPEN, HIGH, LOW, PREV_CLOSE, LTP,
            CHNG, PERCENT_CHNG, VOLUME, VALUE_CRORES, NAV, W52_H, W52_L,
            D30_PERCENT_CHNG, D365, D365_PERCENT_CHNG, D30
        ) VALUES %s
        """

        execute_values(cursor, insert_query, data_tuples)
        connection.commit()
        rows_inserted = cursor.rowcount
        cursor.close()
        connection.close()
        return rows_inserted

    except (Exception, psycopg2.Error) as error:
        st.error(f"Error while inserting data to PostgreSQL: {error}")
        return 0


# Function to execute user SQL queries
def execute_query(query):
    try:
        connection = psycopg2.connect(
            "postgresql://nseetfdb009_user:5RV0z23H9w1DvirLnQeLUhQv39itnHQA@dpg-cse90um8ii6s738ut7e0-a.oregon-postgres.render.com/nseetfdb009"
        )
        cursor = connection.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        connection.close()
        return pd.DataFrame(data, columns=columns)

    except (Exception, psycopg2.Error) as error:
        st.error(f"Error executing the query: {error}")
        return pd.DataFrame()


# Streamlit App
st.title("CSV to PostgreSQL Data Uploader & Query Runner")
st.write("Upload a CSV file to clean the data and insert it into a PostgreSQL database.")

# File upload
uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

if uploaded_file:
    # Load and clean data
    data = pd.read_csv(uploaded_file)
    cleaned_data = clean_data(data)

    # Show preview of cleaned data
    st.write("Preview of cleaned data:")
    st.dataframe(cleaned_data.head())

    # Insert data into the database
    if st.button("Insert Data into Database"):
        total_rows = len(cleaned_data)
        rows_inserted = insert_data_to_db(cleaned_data)

        # Show summary
        st.success(f"Total rows in CSV file: {total_rows}")
        st.success(f"Total rows inserted into the database: {rows_inserted}")

    # Optional: Show SQL query
    st.code("""
    INSERT INTO metf_2024 (
        SYMBOL, UNDERLYING_ASSET, OPEN, HIGH, LOW, PREV_CLOSE, LTP,
        CHNG, PERCENT_CHNG, VOLUME, VALUE_CRORES, NAV, W52_H, W52_L,
        D30_PERCENT_CHNG, D365, D365_PERCENT_CHNG, D30
    ) VALUES %s
    """, language='sql')

# User SQL Query
st.write("Run SQL Queries:")
user_query = st.text_area("Write your SQL query here:")

if user_query.strip():
    if st.button("Execute Query"):
        query_result = execute_query(user_query)
        if not query_result.empty:
            st.write("Query Result:")
            st.dataframe(query_result)
        else:
            st.warning("No data returned from the query.")
