import psycopg2

# Function to delete all entries from the database
def delete_all_entries():
    try:
        # Connect to your PostgreSQL database
        connection = psycopg2.connect(
            "postgresql://nseetfdb009_user:5RV0z23H9w1DvirLnQeLUhQv39itnHQA@dpg-cse90um8ii6s738ut7e0-a.oregon-postgres.render.com/nseetfdb009"
        )
        cursor = connection.cursor()

        # Delete all entries from the specified table
        delete_query = "DELETE FROM metf_2024"
        cursor.execute(delete_query)

        # Commit the changes to the database
        connection.commit()
        print("All entries deleted successfully!")

    except (Exception, psycopg2.Error) as error:
        print("Error while deleting entries from PostgreSQL:", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

# Call the function to delete all entries
delete_all_entries()
