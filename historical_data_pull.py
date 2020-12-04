import pandas as pd
import socket

TICKERS = [ 'SPY','QQQ',            # Base Indexes
            'TQQQ','SVXY',          # Long Equities
            'SPXS','UVXY','SQQQ']   # Long Volatility

#==============================================================================

def read_historical_data_socket(sock, recv_buffer=10000):
    """
    Read the information from the socket

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

def download_ticker(ticker,interval,start_date,end_date):
    '''
    ticker: self explanatory, e.g. 'SPY' [string]
    interval: number of seconds per bar [int]
    start & end dates: YYYYMMDD [int]

    #####################################################################################
    # WARNING:
    # FOR SOME SICK FUCKING REASON THE PRICE DATA COMES BACK HLOC instead of OHLC!

    Data returned as:
    [YYYY-MM-DD HH:mm:SS],[HIGH],[LOW],[OPEN],[CLOSE],[OPEN INTEREST],[VOLUME]
    #####################################################################################
    '''

    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket port

    print(f'Downloading historical data for symbol: {ticker}')

    # Construct the message to call for intraday data

    '''
    Construction of call string:
    HIT,
    Ticker,
    Interval (in seconds),
    Start Date & Time (YYYYMMDD HHMMSS),
    End Date & Time (YYYYMMDD HHMMSS),
    Max Datapoints,
    Begin Filter Time (HHMMSS),
    End Filter Time (HHMMSS),
    Data Direction,
    Interval Type
    <CR><LF>
    '''

    message = f'HIT,{ticker},{str(interval)},{str(start_date)} 075000,{str(end_date)} 161500,,093000,160000,1\n'

    # Open a streaming socket to the IQFeed local gateway
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(20)
    sock.connect((host, port))

    # Send the historical data request message and buffer the data
    data = ''

    try:
        sock.sendall(message.encode())
        data = read_historical_data_socket(sock)
        sock.close
    except Exception as e:
        print('\n=======================================\n')
        print(f'SOCKET ERROR downloading ticker {ticker}')
        print(e)
        print('\n=======================================\n')
        raise

    if len(data) > 0:
        # Replace spaces between fields with commas
        data = data.replace(' ',',')
        # Remove all the endlines and line-ending comma delimiter from each record
        data = data.replace(',\r','')

        # Split data into rows & fields for conversion to dataframe
        split_data = [s.split(',') for s in data.split('\n')]

        cols = ['date','time','high','low','open','close','openinterest','volume']
        numeric_list = ['high','low','open','close','volume']

        df = pd.DataFrame(data=split_data,columns=cols)

        # Reorder columns and drop 'openinterest'
        df = df[['date','time','open','high','low','close','volume']]
        
        # Convert strings to numerics for price & volume data (date & time left as strings)
        for c in numeric_list:
            df[c] = pd.to_numeric(df[c])

        return df

    else:
        print('\n=======================================\n')
        print(f'No data to save for ticker: {ticker}!')
        print('\n=======================================\n')

#==============================================================================

def main():

    for tkr in TICKERS:
        download_ticker(tkr)

#==============================================================================

if __name__ == "__main__":
    main()