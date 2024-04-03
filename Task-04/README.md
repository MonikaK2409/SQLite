## Overview of Task-04
- Performance review for operations like insertion, deletion and updation.
- The operations.py script has options to select of what operations performance to be reviewed.
- SQLite supports batched transactions, and when the size of the tuples there is a difficulty in executing operations like deletion, updation.
- Hence the code is now optimized to do batch transactions for easy flow of operations.
- Updation and deletion operations are performed by reading the csv file and deleting or updating them in the database rather than asking user input for any of these operations.

## CPU and memory Utilization metrix for different operations
![Utilizationmetrix(1)](https://github.com/MonikaK2409/SQLite/assets/142796975/52cb4325-9de7-489b-893e-4ae630c80a6f)




