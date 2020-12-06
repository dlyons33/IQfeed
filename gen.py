import pandas as pd

df = pd.read_csv('AAPL.csv')
df['cv'] = df.groupby('date').volume.cumsum()
print(df)

with open('C:/Users/Dennis/Desktop/create_py.sql','a') as filewriter:
    filewriter.write('\n')
    counter = 0
    for row in df.itertuples():
        vals = f"('{row.date}','{row.time}','{row.open}','{row.high}','{row.low}','{row.close}','{row.volume}','{row.cv}')"
        line = f'insert into aapl (date,time,open,high,low,close,volume,cumvol) values {vals};\n'
        filewriter.write(line)