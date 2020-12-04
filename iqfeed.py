import pandas as pd
import os
import sys
import socket
from time import time

#==============================================================================

def read_historical_data_socket(sock, recv_buffer=10000):
    """
    Read the information from the socket, in a buffered
    fashion, receiving only 4096 bytes at a time.

    Parameters:
    sock - The socket object
    recv_buffer - Amount in bytes to receive per read
    """
    data = []

    while True:

        # Append each new byte string to data
        buff = sock.recv(recv_buffer)
        data.append(buff)

        # Check if the end message string arrived in most recent read
        if b"!ENDMSG!" in buff:
            break

    # Convert bytes to string & remove the end message string
    bstr = b''.join(data)
    datastr = bstr.decode()
    datastr = datastr[:-12]
    return datastr

#==============================================================================

def download_ticker(ticker):

    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket port

    print(f'Downloading historical data for symbol: {ticker}')

    # Construct the message needed by IQFeed to retrieve data
    # Hard coded time period: January 1, 2015 through present
    # DAILY CALL:
    # HDT,[ticker],[begindate],[enddate],[maxdatapoints],[datadirection],[reqid],[datapersend]
    # INTRADAY CALL:
    # HIT,[ticker],[begin date],[end date],,[time filter start],[time filter end],1\n

    #####################################################################
    # WARNING:
    # FOR SOME SICK FUCKING REASON THE PRICE DATA COMES BACK HLOC!
    #####################################################################

    # Data Request String
    message = f'HIT,{ticker},60,20150101 075000,,,093000,160000,1\n'

    # Open a streaming socket to the IQFeed server locally
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(20)
    sock.connect((host, port))

    # Send the historical data request message and buffer the data
    try:
        sock.sendall(message.encode())
        data = read_historical_data_socket(sock)
        sock.close
    except Exception as e:
        print('\n=======================================\n')
        print(f'SOCKET ERROR downloading ticker {ticker}')
        print(e)
        print('\n=======================================\n')
        data = ''

    # Remove all the endlines and line-ending comma delimiter from each record
    data = data.replace(' ',',')
    data = data.replace(',\r','')

    if len(data) > 0:
        '''
        with open(f'stocks_i/{ticker}.csv','w') as f:
            f.write('date,time,high,low,open,close,volume,openinterest\n')
            f.write(data)
        '''
        cols = ['date','time','high','low','open','close','openinterest','volume']
        short_list = ['high','low','open','close','volume']

        try:
            split_data = [s.split(',') for s in data.split('\n')]
            df = pd.DataFrame(data=split_data,columns=cols)
            df.drop('openinterest',axis=1,inplace=True)
            
            for c in short_list:
                df[c] = pd.to_numeric(df[c])

            # df.to_csv(f'stocks_i/{ticker}.csv',index=False,header=True)
            df.to_csv(f'{ticker}.csv',index=False,header=True)
        except Exception as e:
            print('\n=======================================\n')
            print('ERROR processing ticker :',ticker)
            print(e)
            print('\n=======================================\n')

    else:
        print('\n=======================================\n')
        print(f'ERROR: No data to save for ticker: {ticker}!')
        print('\n=======================================\n')

#==============================================================================

def main():
    '''
    String to send through sockets to request historical data:

    CMD,SYM,[options]\n

    The provided options are:
        [bars in seconds],
        [beginning date: CCYYMMDD HHmmSS],
        [ending date: CCYYMMDD HHmmSS], (leave blank for ending at present)
        [empty],
        [beginning time filter: HHmmSS],
        [ending time filter: HHmmSS],
        [old or new: 0 or 1],
        [empty],
        [queue data points per second]

    Data returned as:
    [YYYY-MM-DD HH:mm:SS],[OPEN],[LOW],[HIGH],[CLOSE],[VOLUME],[OPEN INTEREST]
    '''

    start_time = time()

    # REMEMBER TO RUN track_universe.py FIRST TO UPDATE TICKER LIST

    tickers = []
    with open('remaining_tickers.txt','r') as f:
        for line in f:
            security = str(line.split(',')[0])
            tickers.append(security)

    total_tickers = len(tickers)
    ticker_counter = 0

    print(f'Tickers remaining to be downloaded: {total_tickers}')
    print('\n=======================================\n')

    for tkr in tickers:
        ticker_counter += 1
        if ticker_counter < 500:
            loop_time = time()
            download_ticker(tkr)
            print(f'Downloaded ticker {ticker_counter} of {total_tickers}: {tkr}  (time: {(time()-loop_time):.2f} seconds)')

    print(f'Completed downloading tickers. Time elapsed: {(time()-start_time)/60:.2f} minutes')

#==============================================================================

if __name__ == "__main__":
    main()