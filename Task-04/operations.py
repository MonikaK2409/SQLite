import sqlite3
import pandas as pd
import time
import matplotlib.pyplot as plt


def create_table(table_name):
    conn = sqlite3.connect('flow.db')
    conn.execute(f'''CREATE TABLE IF NOT EXISTS {table_name} (
                        source_ip TEXT,
                        destination_ip TEXT,
                        source_port INTEGER,
                        destination_port INTEGER,
                        version INTEGER,
                        PRIMARY KEY (source_ip, destination_ip, source_port, destination_port,version)
                    )''')
    conn.close()


def insert_data(table_name, csv_file):
    start_time = time.time()
    conn = sqlite3.connect('flow.db')
    df = pd.read_csv(csv_file, header=None)
    df.columns = ['source_ip', 'destination_ip', 'source_port', 'destination_port', 'version']
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    end_time = time.time()
    elapsed_time = end_time - start_time
    insertion_times.append(elapsed_time)
    print(f"Successfully inserted {len(df)} tuples in {elapsed_time:.4f} seconds")


def delete_data(table_name, csv_file):
    start_time = time.time()
    conn = sqlite3.connect('flow.db')
    df = pd.read_csv(csv_file, header=None)
    df.columns = ['version', 'source_ip', 'destination_ip', 'source_port', 'destination_port']
    for index, row in df.iterrows():
        version, source_ip, destination_ip, source_port, destination_port = row
        conn.execute(f'''DELETE FROM {table_name} WHERE 
                         version=? AND source_ip=? AND destination_ip=? AND source_port=? AND destination_port=?''',
                     (version, source_ip, destination_ip, source_port, destination_port))
    conn.commit()
    conn.close()
    end_time = time.time()
    elapsed_time = end_time - start_time
    deletion_times.append(elapsed_time)
    print(f"Successfully deleted {len(df)} tuples in {elapsed_time:.4f} seconds")

def update_data(table_name, csv_file):
    start_time = time.time()
    conn = sqlite3.connect('flow.db')
    df = pd.read_csv(csv_file, header=None)
    df.columns = ['version', 'source_ip', 'destination_ip', 'source_port', 'destination_port', 'new_version']
    for index, row in df.iterrows():
        version, source_ip, destination_ip, source_port, destination_port, new_version = row
        conn.execute(f'''UPDATE {table_name} SET source_port=? WHERE 
                         version=? AND source_ip=? AND destination_ip=? AND source_port=? AND destination_port=?''',
                     (100, version, source_ip, destination_ip, source_port, destination_port))
    conn.commit()
    conn.close()
    end_time = time.time()
    elapsed_time = end_time - start_time
    update_times.append(elapsed_time)
    print(f"Successfully updated {len(df)} tuples in {elapsed_time:.4f} seconds")

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
        create_table('Netflow1')
        create_table('Netflow2')
        create_table('Netflow3')
        create_table('Netflow4')
        create_table('Netflow5')
        insertion_times = []
        insert_data('Netflow1', 'data_100_tuples.csv')
        insert_data('Netflow2', 'data_1000_tuples.csv')
        insert_data('Netflow3', 'data_10000_tuples.csv')
        insert_data('Netflow4', 'data_100000_tuples.csv')
        insert_data('Netflow5', 'data_1000000_tuples.csv')
        x = [100, 1000, 10000, 100000, 1000000]
        y = insertion_times
        plt.plot(x, y, marker='o', linestyle='-')
        plt.xlabel('Number of insertions')
        plt.ylabel('Time (seconds)')
        plt.title('Performance of SQLite in insertion')
        plt.show()
    elif choice == "2":
        deletion_times = []
        delete_data('Netflow5', 'data_100_delete.csv')
        restore_netflow5('data_100_delete.csv')
        delete_data('Netflow5', 'data_1000_delete.csv')
        restore_netflow5('data_1000_delete.csv')
        delete_data('Netflow5', 'data_10000_delete.csv')
        restore_netflow5('data_10000_delete.csv')
        delete_data('Netflow5', 'data_100000_delete.csv')
        restore_netflow5('data_100000_delete.csv')
        x = [100, 1000, 10000, 100000]
        y = deletion_times
        plt.plot(x, y, marker='o', linestyle='-')
        plt.xlabel('Number of deletions')
        plt.ylabel('Time (seconds)')
        plt.title('Performance of SQLite in deletion')
        plt.show()
    elif choice == "3":
        update_times = []
        update_data('Netflow5', 'data_100_update.csv')
        update_data('Netflow5', 'data_1000_update.csv')
        update_data('Netflow5', 'data_10000_update.csv')
        update_data('Netflow5', 'data_100000_update.csv')
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
