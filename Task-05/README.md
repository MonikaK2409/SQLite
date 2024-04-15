# Using SQLite with YCSB

## Download and Configure YCSB:

1. Download YCSB from the [official repository](https://github.com/brianfrankcooper/YCSB/releases).
2. Extract the downloaded archive.
3. Use jdbc binding. Download sqlite-jdbc binding jar file and place it in the lib folder of jdbc binding.
4. Configure the db.properties file according to the sqlite3 installed.
5. Create a database (test.db) in a directory which is accessible for ycsb, create a usertable with attributes specified in jdbc binding readme file. 
6. Define the desired workload (e.g., "workloada").
7. Set any other runtime parameters if needed.

## Load Data:

Use YCSB's load command to load key-value pairs into the SQLite database:
```sh
$ ./bin/ycsb load sqlite -P workloads/workloada -p sqlite.dbfile=/path/to/your/sqlite.db
```

### After loading workloada
![image](https://github.com/MonikaK2409/SQLite/assets/142796975/aa804de7-feea-4250-8bea-b5ba9d764846)

## Run Workload:

Execute the workload using YCSB's run command:
```sh
$ ./bin/ycsb run sqlite -P workloads/workloada -p sqlite.dbfile=/path/to/your/sqlite.db

```
### After running workloada
![image(1)](https://github.com/MonikaK2409/SQLite/assets/142796975/b12b72dc-091e-40fb-b3f3-4ed193f21ee9)


