import sqlite3

def delete_flow(source_port):
    conn= sqlite3.connect('flow.db')
    cursor=conn.cursor()
    cursor.execute('DELETE FROM flows WHERE source_port=?',(source_port,))
    conn.commit()
    conn.close()

if __name__=="__main__":
    sourceport_to_delete =int(input("Enter the source_port of the flow to delete: "))
    delete_flow(sourceport_to_delete)
    
