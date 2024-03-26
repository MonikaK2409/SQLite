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

def delete_reverse_flow():
    conn = sqlite3.connect('flow.db')
    cursor = conn.cursor()
    version = input("Enter the version: ")
    source_ip = input("Enter the source IP: ")
    destination_ip = input("Enter the destination IP: ")
    source_port = int(input("Enter the source port: "))
    destination_port = int(input("Enter the destination port: "))

    cursor.execute('''SELECT *
                      FROM Netflow1
                      WHERE (version = ? AND source_ip = ? AND destination_ip = ? AND source_port = ? AND destination_port = ?)''',
                   (version, source_ip, destination_ip, source_port, destination_port))
    row = cursor.fetchone()

    if row:
        cursor.execute('''SELECT *
                          FROM Netflow1 
                          WHERE (version = ? AND source_ip = ? AND destination_ip = ? AND source_port = ? AND destination_port = ?)''',
                       (version, destination_ip, source_ip, destination_port, source_port))
        reverse_flow_row = cursor.fetchone()

        if reverse_flow_row:
            cursor.execute('''DELETE FROM Netflow1 
                              WHERE (version = ? AND source_ip = ? AND destination_ip = ? AND source_port = ? AND destination_port = ?)''',
                           (version, destination_ip, source_ip, destination_port, source_port))
            conn.commit()
            print("Successfully deleted the reverse flow.")
        else:
            print("No reverse flow found for the given flow.")
    else:
        print("No flow found with the given parameters.")

    conn.close()

while True:
    print("Options:")
    print("1. Insert into Database ")
    print("2. Delete")
    print("Type 'exit' to exit")

    # Get user input
    choice = input("Enter your choice: ")
    if choice == "1":
        create_table('Netflow1')
        create_table('Netflow2')
        create_table('Netflow3')
        insertion_times = []
        insert_data('Netflow1', 'data_100_tuples.csv')
        insert_data('Netflow2', 'data_1000_tuples.csv')
        insert_data('Netflow3', 'data_10000_tuples.csv')
        x = [100, 1000, 10000]
        y = insertion_times
        plt.plot(x, y, marker='o', linestyle='-') 
        plt.xlabel('Number of insertions')
        plt.ylabel('Time (seconds)')
        plt.title('Performance of SQLite in insertion')
        plt.show()
    elif choice == "2":
        delete_reverse_flow()
    elif choice.lower() == "exit":
        break
    else:
        print("Invalid choice. Please select 1, 2, or exit.")

