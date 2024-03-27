## Overview of Task-02
In this task I have made sure that I am using a single file to run various operations by using a switch case in the script, by asking the user to enter what task does he want to perform and executing the same.
- networkflow.py script has the features mentioned above and I have made slight change in the way insertion happen.
- I have generated a csv file from the packets traced by WireShark (test1.csv) . As many duplicates packets are also capture, i have made sure that to drop the duplicated and make it another csv file (result.csv).
- Now the insertion happens from the tuples present in the filtered csv file (result.csv). And other operations according to the user interest is performed normally.

## Approach
- To extract packets from Wireshark, I download Wireshark.
- Then I configured the display screen to just display the needful information of the packet and discard rest.
- Then I exported the csv file from wireshark.
   
## Observation 
- Single File Execution: By consolidating all operations into a single script (networkflow.py) and utilizing a switch-case mechanism, you have streamlined the execution flow, making it more convenient for users to interact with the application.
- Improved Insertion Process: The modification in the insertion process to read data from the filtered CSV file (result.csv) instead of directly inserting from the original CSV (test1.csv) suggests a proactive approach to data management. This helps in ensuring data integrity and avoiding duplicate entries.
- Data Filtering: Preprocessing the packet data from WireShark to remove duplicate packets and generating a filtered CSV file (result.csv) showcases a data cleaning step, improving the quality of data used for database operations.

