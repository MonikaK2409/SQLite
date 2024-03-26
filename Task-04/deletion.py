import csv
import sqlite3
import time

# Connect to the database
conn = sqlite3.connect('flow.db')
cursor = conn.cursor()

def delete_tuples_from_db(csv_file, table_name):
    try:
        # Open the CSV file and read the data
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                # Extract the values for each attribute
                source_ip, destination_ip, source_port, destination_port, version = row
                # Construct the SQL query to delete the tuple based on all five attributes
                cursor.execute(f"DELETE FROM {table_name} WHERE source_ip = ? AND destination_ip = ? AND source_port = ? AND destination_port = ? AND version = ?", (source_ip, destination_ip, source_port, destination_port, version))
          
        # Commit the changes
        conn.commit()
        print("Deletion complete")
    except Exception as e:
        print("Error occurred:", e)

# Example usage:
csv_file = 'data_100_delete.csv'
table_name = 'Netflow5'
start_time = time.time()
delete_tuples_from_db(csv_file, table_name)
end_time = time.time()
elapsed_time = end_time - start_time
print(f"Successfully deleted tuples in {elapsed_time:.4f} seconds")
# Close the database connection
conn.close()

