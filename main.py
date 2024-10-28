import psycopg2

try:
    connection = psycopg2.connect(
        "postgresql://nseetfdb009_user:5RV0z23H9w1DvirLnQeLUhQv39itnHQA@dpg-cse90um8ii6s738ut7e0-a.oregon-postgres.render.com/nseetfdb009"
    )
    cursor = connection.cursor()

    # SQL command to create the table
    create_table_query = '''
    CREATE TABLE metf_2024 (
        SYMBOL VARCHAR(50),
        UNDERLYING_ASSET VARCHAR(100),
        OPEN NUMERIC,
        HIGH NUMERIC,
        LOW NUMERIC,
        PREV_CLOSE NUMERIC,
        LTP NUMERIC,
        CHNG NUMERIC,
        PERCENT_CHNG NUMERIC,
        VOLUME BIGINT,
        VALUE_CRORES NUMERIC,
        NAV NUMERIC,
        W52_H NUMERIC,
        W52_L NUMERIC,
        D30_PERCENT_CHNG NUMERIC,
        D365 NUMERIC,
        D365_PERCENT_CHNG NUMERIC,
        D30 NUMERIC
    );
    '''

    # Execute the SQL command
    cursor.execute(create_table_query)
    connection.commit()

    print("Table 'metf_2024' created successfully.")

except (Exception, psycopg2.Error) as error:
    print("Error while creating the table:", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")
