import sqlite3
import pandas as pd
import time

def option_exit():
    print("Exiting program...")

def option1():
    df = pd.read_csv('test1.csv')
    df = df.drop_duplicates()
    df.reset_index(drop=True, inplace=True)
    df.index += 1
    df.to_csv('result.csv', index=False)
    start_time = time.time()
    conn = sqlite3.connect('flow.db')
    df.columns = df.columns.str.strip()
    df.to_sql('Netflow', conn, if_exists='replace')
    conn.close()
    end_time = time.time()
    print(f"Successfully inserted in {end_time - start_time:.4f} seconds")
   
def option2():
    conn = sqlite3.connect('flow.db')
    cursor = conn.cursor()
    index = int(input("Enter the index of the flow to delete: "))
    cursor.execute('DELETE FROM Netflow WHERE "index" =?', (index,))
    conn.commit()
    conn.close()
    print("Successfully deleted")

def option3():
    index = int(input("Enter the No. whose source ip is to be updated "))
    source = input("Enter the new source ip ")
    conn = sqlite3.connect('flow.db')
    cursor = conn.cursor()
    cursor.execute('''
         UPDATE Netflow
         SET Source=?
         WHERE "index"=?
     ''', (source, index))
    conn.commit()
    conn.close()
    print("Successfully updated")

def default_option():
    print("Invalid option")

# Define a dictionary that maps user input to corresponding functions
options = {
    '1': option1,
    '2': option2,
    '3': option3,
    'exit': option_exit,
}

# Main loop
while True:
    # Display menu
    print("Options:")
    print("1. Insert into Database from csv file")
    print("2. Delete")
    print("3. Update")
    print("Type 'exit' to exit")

    # Get user input
    user_input = input("Enter your choice: ")

    # Check if user wants to exit
    if user_input.lower() == 'exit':
        break  # Exit the loop

    # Use the dictionary to call the appropriate function based on user input
    selected_option = options.get(user_input, default_option)
    selected_option()

