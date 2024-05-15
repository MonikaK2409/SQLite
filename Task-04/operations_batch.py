import sqlite3
import pandas as pd
import time
import matplotlib.pyplot as plt

def insert_data_batch(table_name, csv_file):
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
            time = time.time()
            elapsed_time = insert_end_time - insert_start_time
            total_time += insert_elapsed_time
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
        df.columns = ['source_ip', 'destination_ip', 'source_port', 'destination_port','version']

        total_rows = len(df)
        num_batches = (total_rows + batch_size - 1) // batch_size  # Calculate number of batches
        
        for i in range(num_batches):
            # Extract the current batch
            batch_df = df[i * batch_size : (i + 1) * batch_size]
            start_time = time.time()
            try:
                delete_query = f'''DELETE FROM {table_name} WHERE 
                             ( source_ip, destination_ip, source_port, destination_port,version) IN 
                             ({','.join(['(?,?,?,?,?)']*len(batch_df))})'''

            # Flatten dataframe values for placeholders in the query
                values = [val for row in batch_df.values for val in row]

            # Execute the delete query with batch processing
                cursor.execute(delete_query, values)
            except sqlite3.Error as e:
                print(f"Error deleting data: {e}")
        # Commit the transaction
        conn.commit()
        end_time = time.time()
        elapsed_time = end_time - start_time
        deletion_times.append(elapsed_time)
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

def update_data(table_name, csv_file):
    try:
        conn = sqlite3.connect('flow.db')
        cursor = conn.cursor()

        df = pd.read_csv(csv_file, header=None)
        df.columns = ['source_ip', 'destination_ip', 'source_port', 'destination_port', 'version']

        batch_size = 1000
        total_rows = len(df)
        num_batches = (total_rows + batch_size - 1) // batch_size

        cursor.execute('BEGIN TRANSACTION')

        try:
            update_query = f'''UPDATE {table_name} SET source_port = 100 WHERE 
                               source_ip = ? AND destination_ip = ? AND 
                               source_port = ? AND destination_port = ? AND version = ? '''
            start_time=time.time()
            for i in range(num_batches):
                batch_df = df[i * batch_size: (i + 1) * batch_size]
                update_values = [tuple(row) for row in batch_df[['source_ip', 'destination_ip', 'source_port', 'destination_port', 'version']].values]
                cursor.executemany(update_query, update_values)

            num_updated = cursor.rowcount
            end_time=time.time()
            elapsed_time = end_time - start_time
            update_times.append(elapsed_time)
            conn.commit()

            print(f"Successfully updated {num_updated} tuples in {elapsed_time:.4f} seconds")

        except sqlite3.Error as sqlite_error:
            print(f"SQLite error occurred: {sqlite_error}")
            conn.rollback()
            return False, None

        except Exception as e:
            print(f"Error occurred during update: {e}")
            conn.rollback()
            return False, None

        finally:
            cursor.close()
            conn.close()

    except sqlite3.Error as sqlite_error:
        print(f"SQLite connection error: {sqlite_error}")
        return False, None

    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        return False, None
   
def restore_netflow5(csv_file):
    conn = sqlite3.connect('flow.db')
    df = pd.read_csv(csv_file, header=None)
    df.columns = ['version', 'source_ip', 'destination_ip', 'source_port', 'destination_port']
    df.to_sql('Netflow5', conn, if_exists='append', index=False)
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
        insert_data_batch('Netflow1', 'data_100_tuples.csv', batch_size=10)
        insert_data_batch('Netflow2', 'data_1000_tuples.csv', batch_size=100)
        insert_data_batch('Netflow3', 'data_10000_tuples.csv', batch_size=1000)
        insert_data_batch('Netflow4', 'data_100000_tuples.csv', batch_size=10000)
        insert_data_batch('Netflow5', 'data_1000000_tuples.csv', batch_size=100000)
        x = [100, 1000, 10000, 100000, 1000000]
        y = insertion_times
        plt.plot(x, y, marker='o', linestyle='-')
        plt.xlabel('Number of insertions')
        plt.ylabel('Time (seconds)')
        plt.title('Performance of SQLite in insertion')
        plt.show()
    elif choice == "2":
        deletion_times = []
        delete_data('Netflow5', 'data_100_delete.csv', batch_size=10)
        #restore_netflow5('data_100_delete.csv')
        delete_data('Netflow5', 'data_1000_delete.csv', batch_size=100)
        #restore_netflow5('data_1000_delete.csv')
        delete_data('Netflow5', 'data_10000_delete.csv', batch_size=1000)
        #restore_netflow5('data_10000_delete.csv')
        delete_data('Netflow5', 'data_100000_delete.csv', batch_size=10000)
        #restore_netflow5('data_100000_delete.csv')
        x = [100, 1000, 10000, 100000]
        y = deletion_times
        plt.plot(x, y, marker='o', linestyle='-')
        plt.xlabel('Number of deletions')
        plt.ylabel('Time (seconds)')
        plt.title('Performance of SQLite in deletion')
        plt.show()
    elif choice == "3":
        update_times = []
        update_data('Netflow5', 'data_100_delete.csv')
        update_data('Netflow5', 'data_1000_delete.csv')
        update_data('Netflow5', 'data_10000_delete.csv')
        update_data('Netflow5', 'data_100000_delete.csv')
        x = [100, 1000, 10000, 100000]
        y = update_times
        plt.plot(x, y, marker='o', linestyle='-')
        plt.xlabel('Number of updates')
        plt.ylabel('Time (seconds)')
        plt.title('Performance of SQLite in update')
        plt.show()
    elif choice.lower() == "exit":
        break
    else:
        print("Invalid choice. Please select 1, 2, or exit.")


