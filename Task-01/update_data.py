import sqlite3

def update_flow(souce_port, new_destination_port):
     conn=sqlite3.connect('flow.db')
     cursor=conn.cursor()
     cursor.execute('''
         UPDATE flows
         SET destination_port=?
         WHERE source_port=?
      ''',(new_destination_port, souce_port))
      
     conn.commit()
     conn.close()
      
if __name__=="__main__":
     sourcep=int(input("Enter the source port whose destination port to be updated: "))
     destip=int(input("Enter the new destination port: "))
     update_flow(sourcep,destip) 
