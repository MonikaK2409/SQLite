import sqlite3
import pandas as pd
import time
import matplotlib.pyplot as plt

def insert_data_batch(table_name, csv_file, batch_size):
    try:
        conn = sqlite3.connect('flow.db')

        # Set PRAGMA parameters for cache_size and temp_store
        conn.execute(f'PRAGMA cache_size = {batch_size}')
        conn.commit()
        conn.execute(f'PRAGMA temp_store = MEMORY')
        conn.commit()

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

        total_time = 0

        # Read data from CSV in batches and insert into the database
        for chunk in pd.read_csv(csv_file, header=None, chunksize=batch_size):
            chunk.columns = ['source_ip', 'destination_ip', 'source_port', 'destination_port', 'version']
            try:
                start_time = time.time()
                chunk.to_sql(table_name, conn, if_exists='append', index=False)
                conn.commit()
                end_time = time.time()
                elapsed_time = end_time - start_time
                total_time += elapsed_time
            except sqlite3.Error as e:
                print(f"Error inserting data: {e}")

        print(f"Successfully inserted data from {csv_file} into {table_name} in {total_time:.4f} seconds")

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

        total_time=0
        
        #setting pragma parameters for cache_size and temporary store
        conn.execute(f'PRAGMA cache_size = {batch_size}')
        conn.execute(f'PRAGMA temp_store = MEMORY')
        
        for i in range(num_batches):
            # Extract the current batch
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]

            # Extract the data to be deleted from the batch
            batch_data = [(row['source_ip'], row['destination_ip'], row['source_port'], row['destination_port'], row['version']) for _, row in batch_df.iterrows()]

            # Create the SQL DELETE statement with placeholders for batch deletion
            sql_delete = f'DELETE FROM {table_name} WHERE source_ip = ? AND destination_ip = ? AND source_port = ? AND destination_port = ? AND version = ?'

            # Execute the batch deletion operation
            start_time = time.time()
            execute_batch(sql_delete, batch_data)
            conn.commit()
            end_time = time.time()
            elapsed_time = end_time - start_time
            total_time += elapsed_time
           
       
        print(f"Successfully deleted {total_rows} tuples in {total_time:.4f} seconds")

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

        total_time=0
        
        #setting pragma parameters for cache_size and temporary store
        conn.execute(f'PRAGMA cache_size = {batch_size}')
        conn.commit()
        conn.execute(f'PRAGMA temp_store = MEMORY')
        conn.commit()
        
        for i in range(num_batches):
            # Extract the current batch
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]

            # Extract the data to be deleted from the batch
            batch_data = [(row['source_ip'], row['destination_ip'], row['source_port'], row['destination_port'], row['version']) for _, row in batch_df.iterrows()]
            
            # Create the SQL UPDATE statement with placeholders for batch deletion
            sql_update = f'UPDATE {table_name} SET source_port = 100 WHERE source_ip = ? AND destination_ip = ? AND source_port = ? AND destination_port = ? AND version = ?'
                              
            start_time = time.time()
            # Execute the batch deletion operation
            cursor.execute_batch(sql_update, batch_data)
            conn.commit()
            end_time = time.time()
            elapsed_time = end_time - start_time
            total_time += elapsed_time
        # Commit the transaction
        
        print(f"Successfully updated {total_rows} tuples in {total_time:.4f} seconds")

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

