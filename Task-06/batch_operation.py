import sqlite3
import pandas as pd
import time
import matplotlib.pyplot as plt

def insert_data_batch(table_name, csv_file, batch_size):
    try:
        conn = sqlite3.connect('flow.db')
        cursor = conn.cursor()

        cursor.execute(f'DROP TABLE IF EXISTS {table_name}')  # Drop the table if it already exists

        # Create the table
        cursor.execute(f'''CREATE TABLE {table_name} (
                                source_ip TEXT,
                                destination_ip TEXT,
                                source_port INTEGER,
                                destination_port INTEGER,
                                version INTEGER,
                                PRIMARY KEY(source_ip, destination_ip, source_port, destination_port, version)
                            )''')

        start_time = time.time()

        # Read data from CSV in batches and insert into the database
        for chunk in pd.read_csv(csv_file, header=None, chunksize=batch_size):
            chunk.columns = ['source_ip', 'destination_ip', 'source_port', 'destination_port', 'version']
            try:
                chunk.to_sql(table_name, conn, if_exists='append', index=False)
            except sqlite3.Error as e:
                print(f"Error inserting data: {e}")

        conn.commit()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Successfully inserted data from {csv_file} into {table_name} in {elapsed_time:.4f} seconds")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except pd.errors.ParserError as e:
        print(f"CSV parsing error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if conn:
            conn.close()

def delete_data(table_name, csv_file, batch_size):
    try:
        conn = sqlite3.connect('flow.db')
        cursor = conn.cursor()

        # Begin a transaction
        cursor.execute('BEGIN TRANSACTION')

        df = pd.read_csv(csv_file, header=None)
        df.columns = ['source_ip', 'destination_ip', 'source_port', 'destination_port', 'version']

        total_rows = len(df)
        num_batches = (total_rows + batch_size - 1) // batch_size  # Calculate number of batches

        start_time = time.time()

        for i in range(num_batches):
            # Extract the current batch
            batch_df = df[i * batch_size : (i + 1) * batch_size]

            # Iterate over the batch and delete each row individually
            for _, row in batch_df.iterrows():
                cursor.execute(f'DELETE FROM {table_name} WHERE source_ip = ? AND destination_ip = ? AND source_port = ? AND destination_port = ? AND version = ?',
                               (row['source_ip'], row['destination_ip'], row['source_port'], row['destination_port'], row['version']))

        # Commit the transaction
        conn.commit()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Successfully deleted {total_rows} tuples in {elapsed_time:.4f} seconds")

    except sqlite3.Error as e:
        # Rollback transaction if an error occurs
        conn.rollback()
        print(f"SQLite error occurred during deletion: {e}")

    except pd.errors.ParserError as e:
        print(f"CSV parsing error: {e}")

    except Exception as e:
        # Rollback transaction if an error occurs
        conn.rollback()
        print(f"Error occurred during deletion: {e}")

    finally:
        # Close cursor and connection
        cursor.close()
        conn.close()

def update_data(table_name, csv_file, batch_size):
    try:
        conn = sqlite3.connect('flow.db')
        cursor = conn.cursor()

        # Begin a transaction
        cursor.execute('BEGIN TRANSACTION')

        df = pd.read_csv(csv_file, header=None)
        df.columns = ['source_ip', 'destination_ip', 'source_port', 'destination_port', 'version']

        total_rows = len(df)
        num_batches = (total_rows + batch_size - 1) // batch_size  # Calculate number of batches

        start_time = time.time()

        for i in range(num_batches):
            # Extract the current batch
            batch_df = df[i * batch_size : (i + 1) * batch_size]

            # Iterate over the batch and delete each row individually
            for _, row in batch_df.iterrows():
                cursor.execute(f'UPDATE {table_name} SET source_ip = 100 WHERE source_ip = ? AND destination_ip = ? AND source_port = ? AND destination_port = ? AND version = ?',
                               (row['source_ip'], row['destination_ip'], row['source_port'], row['destination_port'], row['version']))

        # Commit the transaction
        conn.commit()
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Successfully updated {total_rows} tuples in {elapsed_time:.4f} seconds")

    except sqlite3.Error as e:
        # Rollback transaction if an error occurs
        conn.rollback()
        print(f"SQLite error occurred during updation: {e}")

    except pd.errors.ParserError as e:
        print(f"CSV parsing error: {e}")

    except Exception as e:
        # Rollback transaction if an error occurs
        conn.rollback()
        print(f"Error occurred during updation: {e}")

    finally:
        # Close cursor and connection
        cursor.close()
        conn.close()

while True:
    print("Options:")
    print("1. Insert into Database ")
    print("2. Delete")
    print("3. Update")
    print("Type 'exit' to exit")

    # Get user input
    choice = input("Enter your choice: ")
    
    if choice == "1":
        batch_size= int(input("Enter Batch size: "))
        insert_data_batch('Netflow', 'data_1000000_tuples.csv', batch_size)
        
    elif choice == "2":
        batch_size= int(input("Enter Batch size: "))
        delete_data('Netflow', 'shuffled_data.csv', batch_size)
        
    elif choice == "3":
         batch_size= int(input("Enter Batch size: "))
         update_data('Netflow', 'shuffled_data.csv', batch_size)
    elif choice.lower() == "exit":
        break
    else:
        print("Invalid choice. Please select 1, 2, or exit.")

