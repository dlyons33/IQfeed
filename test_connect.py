from time import sleep
import connection

if __name__ == "__main__":

    host = "127.0.0.1"  # Localhost
    ##########################################
    # Need port for the Live Bar Interval data
    ##########################################
    port = # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

    print('Initilizing connection...')
    conn = connection.BarsConnection(host,port)
    conn.connect()

    print('Requesting subscription to SPY...')

    '''
    BW,[Symbol],[Interval],[BeginDate BeginTime],[MaxDaysOfDatapoints],
    [MaxDatapoints],[BeginFilterTime],[EndFilterTime],[RequestID],
    [Interval Type],[Reserved],[UpdateInterval]
    '''

    symbol = 'SPY'
    interval_len = 5
    bgn_bar_str
    lookback_days_str
    lookback_bars_str
    bf_str
    ef_str
    request_id = 'test'
    interval_type = 's'
    update_str

    cmd =   "BW,%s,%s,%s,%s,%s,%s,%s,%s,%s,'',%s\r\n" % (
            symbol, interval_len, bgn_bar_str, lookback_days_str,
            lookback_bars_str,
            bf_str, ef_str, request_id, interval_type, update_str)

    conn.send_command(cmd)

    print('Going to sleep...')
    sleep(20)

    print('CLOSING CONNECTION...')
    conn.disconnect()

    print('All done!')