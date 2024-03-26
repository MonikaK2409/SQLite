import sqlite3
import random
import ipaddress

def create_database():
    conn=sqlite3.connect("flow.db")
    cursor=conn.cursor()
    cursor.execute('''
      CREATE TABLE flows(
        version INTEGER,
        source_port INTEGER,
        destination_port INTEGER,
        source_ip TEXT,
        destination_ip TEXT , 
        UNIQUE(source_port, destination_port)         
      )
     ''')
    conn.commit()
    

def insert_values():
    conn=sqlite3.connect("flow.db")
    cursor=conn.cursor()

    for _ in range(1000):
        source_port=random.randint(1024, 65535)
        destination_port=random.randint(1024,65535)
        source_ip=str(ipaddress.IPv4Address(random.randint(0, 2**32-1)))
        destination_ip=str(ipaddress.IPv4Address(random.randint(0,2**32-1)))
        cursor.execute('''
            INSERT INTO flows (version, source_port, destination_port, source_ip, destination_ip)  
            VALUES(?,?,?,?,?)          
        ''', (4, source_port,destination_port,source_ip,destination_ip))
        conn.commit()
        

create_database()

insert_values()
