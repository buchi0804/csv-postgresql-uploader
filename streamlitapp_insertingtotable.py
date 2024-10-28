import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Step 1: Load the CSV file into a DataFrame
data = pd.read_csv(r'C:\New folder\MW-ETF-16-Aug-2024.csv')

# Strip whitespace from column names
data.columns = data.columns.str.strip()

# Print the cleaned column names for debugging
print("Column names after stripping whitespace:")
print(data.columns.tolist())  # Print all column names

# Step 2: Identify the column to fix its name if necessary
# Check for 'VALUE (₹ Crores)' and print related info
target_column = 'VALUE (₹ Crores)'
if target_column not in data.columns:
    print(f"Column '{target_column}' not found. Available columns:", data.columns.tolist())

# Instead of using the exact name, let's find the column that contains "VALUE" and print it
similar_columns = [col for col in data.columns if 'VALUE' in col]
print("Similar columns found:", similar_columns)

# Try to rename the column if it exists but with different formatting
if similar_columns:
    # Assuming there's only one similar column, take the first match
    data.rename(columns={similar_columns[0]: target_column}, inplace=True)

# Now check if the target column exists
if target_column in data.columns:
    # Replace non-numeric values with NaN and convert to appropriate data types
    # Convert 'VOLUME' to int
    if 'VOLUME' in data.columns:
        data['VOLUME'] = pd.to_numeric(data['VOLUME'].str.replace(',', '', regex=False), errors='coerce').fillna(
            0).astype(int)

    # Convert target column to float, ensuring to handle it as a string first
    if target_column in data.columns:
        data[target_column] = data[target_column].astype(str).str.replace(',', '', regex=False).replace('nan', '0')
        data[target_column] = pd.to_numeric(data[target_column], errors='coerce').fillna(0).astype(float)

    # Drop any non-numeric values from other numeric columns
    numeric_columns = ['OPEN', 'HIGH', 'LOW', 'PREV. CLOSE', 'LTP', 'CHNG', '%CHNG',
                       'VALUE (₹ Crores)', 'NAV', '52W H', '52W L', '30D  %CHNG', '365 D', '365 D % CHNG', '30 D']

    for col in numeric_columns:
        if col in data.columns:
            # Convert to string and then clean
            data[col] = data[col].astype(str).str.replace(',', '', regex=False)
            data[col] = pd.to_numeric(data[col], errors='coerce')

    # Print the cleaned DataFrame for debugging
    print("Cleaned DataFrame:")
    print(data.head())
    print("Columns:", data.columns)

    # Get the total number of rows in the DataFrame
    total_rows = len(data)
    print(f"Total rows in CSV file: {total_rows}")

    rows_inserted = 0  # Initialize rows_inserted

    try:
        # Step 3: Connect to your PostgreSQL database
        connection = psycopg2.connect(
            "postgresql://nseetfdb009_user:5RV0z23H9w1DvirLnQeLUhQv39itnHQA@dpg-cse90um8ii6s738ut7e0-a.oregon-postgres.render.com/nseetfdb009"
        )
        cursor = connection.cursor()

        # Step 4: Prepare the list of tuples for bulk insert
        data_tuples = [tuple(x) for x in data.to_numpy()]

        # Execute the bulk insert
        insert_query = """
        INSERT INTO metf_2024 (
            SYMBOL, UNDERLYING_ASSET, OPEN, HIGH, LOW, PREV_CLOSE, LTP,
            CHNG, PERCENT_CHNG, VOLUME, VALUE_CRORES, NAV, W52_H, W52_L,
            D30_PERCENT_CHNG, D365, D365_PERCENT_CHNG, D30
        ) VALUES %s
        """

        execute_values(cursor, insert_query, data_tuples)

        # Commit the changes to the database
        connection.commit()
        rows_inserted = cursor.rowcount  # Get the number of rows inserted
        print(f"Data inserted successfully! Rows inserted: {rows_inserted}")

    except (Exception, psycopg2.Error) as error:
        print("Error while inserting data to PostgreSQL:", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

    # Summary of the operation
    print(f"Total rows in CSV file: {total_rows}")
    print(f"Total rows inserted into the database: {rows_inserted}")
else:
    print(f"Target column '{target_column}' still not found after renaming similar columns.")
