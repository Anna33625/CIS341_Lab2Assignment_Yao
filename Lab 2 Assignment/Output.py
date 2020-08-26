import os
import mysql.connector
import json
import pandas as pd

#cmd = 'cat pg14884.txt | python mapper.py | sort -k1,1 | python reducer.py > results.txt'
#os.system(cmd)

mydb = mysql.connector.connect(
                user="root",
                passwd="12365sql",
                database="WordCount"
              )

mycursor = mydb.cursor()
#sql_table = "CREATE TABLE wordcount (word VARCHAR(50), count VARCHAR(10))"
#mycursor.execute(sql_table)
print(mydb)



#filename = 'results.txt'
data = pd.read_csv('results.txt', sep = "\t", header = None)
data.columns = ["word", "count"]
results = data.values.tolist()
print(results)

for v in results:
  sql = "INSERT INTO wordcount (word, count) VALUES (%s, %s)"
  val = (v[0], v[1])
  mycursor.execute(sql, val)
  mydb.commit()

#mycursor.executemany(sql, results1)
#mycursor.executemany(sql, val)



print(mycursor.rowcount, "was inserted.")