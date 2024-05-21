import sqlite3
import pandas as pd
import time
import matplotlib.pyplot as plt

def insert_data(table_name,csv_file):
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
 
       total_time=0
       df = pd.read_csv(csv_file, header=None)
       df.columns = ['source_ip', 'destination_ip', 'source_port', 'destination_port', 'version']

       for index, row in df.iterrows():
          try:
            start_time = time.time()
            cursor.execute(f'''
                INSERT INTO {table_name} (source_ip, destination_ip, source_port, destination_port, version)
                VALUES (?, ?, ?, ?, ?)
            ''', (row['source_ip'], row['destination_ip'], row['source_port'], row['destination_port'], row['version']))
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
            
def update_data(table_name,csv_file):
    try:
       conn = sqlite3.connect('flow.db')
       cursor = conn.cursor()
        
       total_time=0
       df = pd.read_csv(csv_file, header=None)
       df.columns = ['source_ip', 'destination_ip', 'source_port', 'destination_port', 'version']

       for index, row in df.iterrows():
          try:
            start_time = time.time()
            cursor.execute(f'''
                UPDATE {table_name} SET source_port = 100 WHERE source_ip = ? AND destination_ip = ? AND source_port = ? AND destination_port = ? AND version = ?
            ''', (row['source_ip'], row['destination_ip'], row['source_port'], row['destination_port'], row['version']))
            conn.commit()
            end_time = time.time()
            elapsed_time = end_time - start_time
            total_time += elapsed_time
          except sqlite3.Error as e:
             print(f"Error updating data: {e}")

       print(f"Successfully updated data from {csv_file} into {table_name} in {total_time:.4f} seconds")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except pd.errors.ParserError as e:
        print(f"CSV parsing error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if conn:
            conn.close()
            
def delete_data(table_name,csv_file):
    try:
       conn = sqlite3.connect('flow.db')
       cursor = conn.cursor()
        
       total_time=0
       df = pd.read_csv(csv_file, header=None)
       df.columns = ['source_ip', 'destination_ip', 'source_port', 'destination_port', 'version']

       for index, row in df.iterrows():
          try:
            start_time = time.time()
            cursor.execute(f'''
                DELETE from {table_name} WHERE source_ip = ? AND destination_ip = ? AND source_port = ? AND destination_port = ? AND version = ?
            ''', (row['source_ip'], row['destination_ip'], row['source_port'], row['destination_port'], row['version']))
            conn.commit()
            end_time = time.time()
            elapsed_time = end_time - start_time
            total_time += elapsed_time
          except sqlite3.Error as e:
             print(f"Error deleting data: {e}")

       print(f"Successfully deleted data from {csv_file} into {table_name} in {total_time:.4f} seconds")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except pd.errors.ParserError as e:
        print(f"CSV parsing error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if conn:
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
        insertion_times = []
        insert_data('Netflow1', 'data_100_tuples.csv')
        insert_data('Netflow2', 'data_1000_tuples.csv')
        insert_data('Netflow3', 'data_10000_tuples.csv')
        
    elif choice == "2":
        deletion_times = []
        delete_data('Netflow1', 'data_100_tuples.csv')
        delete_data('Netflow2', 'data_1000_tuples.csv')
        delete_data('Netflow3', 'data_10000_tuples.csv')   
          
    elif choice == "3":
        update_times = []
        update_data('Netflow1', 'data_100_tuples.csv')
        update_data('Netflow2', 'data_1000_tuples.csv')
        update_data('Netflow3', 'data_10000_tuples.csv')
        
    elif choice.lower() == "exit":
        break
    else:
        print("Invalid choice. Please select 1, 2, or exit.")
