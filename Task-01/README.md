## Overview of Task-01
- As I understood SQLite theoritically, I tried to run few queries initially.
- main.py is a python script which has a creation of table (Created a table with 5 attributes i.e, verion, source_port, destination_port, source_ip, destination_ip and all 5 attributes together as a primary key)  and insertion functions. The insertion took place using random function.
- delete_data.py takes in input of which tuple to delete an deletes it from the DB.
- update_data.py takes in input of which tuple to update and updates one of its attributes.

## Observation
- Execution of Queries: Your scripts, main.py, delete_data.py, and update_data.py, demonstrate your practical application of SQLite queries. main.py focuses on creating tables and inserting data, while delete_data.py and update_data.py handle deletion and updating of tuples respectively.
- Script Modularity: Breaking down functionality into separate scripts (main.py, delete_data.py, update_data.py) is a good practice for code organization and maintainability. It allows for easier debugging and troubleshooting of specific functionalities.
- User Input Handling: The usage of user input in delete_data.py and update_data.py indicates an interactive approach, allowing users to specify which tuples to delete or update. This enhances usability and flexibility.

## Changes Suggested
- To combine the scripts into one single file, rather than having different files.
- And to perform tasks as user desired to choose what task to perform putting a switch case in the script.

Keeping in mind the advantages of breaking down the scripts I have incoorporated the changes suggested in further tasks.
