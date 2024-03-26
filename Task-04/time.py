import sqlite3
import pandas as pd
import time
import matplotlib.pyplot as plt

insertion_times = []

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
    df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()
    end_time = time.time()
    elapsed_time = end_time - start_time
    insertion_times.append(elapsed_time)
    print(f"Successfully inserted {len(df)} tuples in {elapsed_time:.4f} seconds")

# Create tables with index column
create_table('Netflow1')
create_table('Netflow2')
create_table('Netflow3')
create_table('Netflow4')
create_table('Netflow5')

# Insert data
insert_data('Netflow1', 'data_100_tuples.csv')
insert_data('Netflow2', 'data_1000_tuples.csv')
insert_data('Netflow3', 'data_10000_tuples.csv')
insert_data('Netflow4', 'data_100000_tuples.csv')
insert_data('Netflow5', 'data_1000000_tuples.csv')

# Plotting the performance
x = [100, 1000, 10000, 100000, 1000000]
y = insertion_times
plt.plot(x, y, marker='o', linestyle='-') 
plt.xlabel('Number of insertions')
plt.ylabel('Time (seconds)')
plt.title('Performance of SQLite in insertion')
plt.show()

