import psycopg2
import configparser
from collections import namedtuple

Bar = namedtuple('Bar',['symbol','date','time','open','high','low','close','volume','cumvol'])

data = Bar(
        symbol='SPY',
        date='2020-04-26',
        time='09:30:15',
        open='105.60',
        high='107.80',
        low='102.60',
        close='106.05',
        volume='105236',
        cumvol='605223',
        )

try:
    conn = psycopg2.connect(database='stocks',
                            user='postgres',
                            password='testpw!@',
                            host='localhost',
                            port='5432')
    curs = conn.cursor()
except Exception as e:
    print('ERROR initializing database connection!')
    print(e)
    raise

try:
    curs.execute("select * from information_schema.tables where table_schema = 'public'")
    tups = curs.fetchall()
    if tups:
        tables = []
        for t in tups:
            tables.append(t[2])
        print(tables)
except Exception as e:
    print('Error getting tables')
    print(e)
    raise

vals = f"""'{data.symbol}','{data.date}','{data.time}',
            '{data.open}','{data.high}','{data.low}',
            '{data.close}','{data.volume}','{data.cumvol}'"""
instruction = f"insert into test (symbol,date,time,open,high,low,close,volume,cumvol) values ({vals});"

try:
    curs.execute(instruction)
    conn.commit()
    print('Executed instruction:',instruction)
except Exception as e:
    print('ERROR executing instruction!')
    print(e)
    raise