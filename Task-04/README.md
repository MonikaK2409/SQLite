## Overview of Task-04
- Performance review for operations like insertion, deletion and updation.
- The operations.py script has options to select of what operations performance to be reviewed.
- SQLite supports batched transactions, and when the size of the tuples there is a difficulty in executing operations like deletion, updation.
- Hence the code is now optimized to do batch transactions for easy flow of operations.
- Updation and deletion operations are performed by reading the csv file and deleting or updating them in the database rather than asking user input for any of these operations.

## Observation

### Performance of SQLite in insertion
![Figure_1](https://github.com/MonikaK2409/SQLite/assets/142796975/216f7c98-f20e-4670-8900-3a7504283f5d)

### Performance of SQLite in deletion
![Figure_2](https://github.com/MonikaK2409/SQLite/assets/142796975/a8dd042c-6eb6-42f4-84a3-5644b4eb0277)

- Updation is currently taking alot of time, drawing statistics is also been a huge task.



