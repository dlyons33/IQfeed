import pandas as pd
import psycopg2

DB_NAME = 'stocks'
DB_USER = 'postgres'
DB_PASS = 'testpw!@'
DB_HOST = 'localhost'
DB_PORT = '5432'

try:
    con = psycopg2.connect(database=DB_NAME,
                            user=DB_USER,
                            password=DB_PASS,
                            host=DB_HOST,
                            port=DB_PORT)
    print('Connected to database.')
except:
    print('ERROR - Failed to connect to database!')

cur = con.cursor()

cur.execute('select * from aapl limit 20')
rows = cur.fetchall()

cols = ['id','date','time','open','high','low','close','volume','cumvol']
df = pd.DataFrame(data=rows,columns=cols)
print(df)

cur.close()
con.close()