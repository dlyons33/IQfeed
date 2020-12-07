from time import sleep
import configparser
import subprocess
import threading
import queue
import logging
import connection
import listener
import logger
import postgres

def start_iqconnect(pID,uID,pw):

    exe_args = [    'IQConnect.exe','-product',str(pID),'-version','1.0',
                    '-login',str(uID),'-password',str(pw),'-autoconnect']

    iqthread = threading.Thread(target=iq_thread,name='iqthread',args=(exe_args,))
    iqthread.start()
    sleep(7)

    return iqthread

def iq_thread(exe_args):
    subprocess.run(exe_args)

if __name__ == "__main__":

    try:

        config = configparser.ConfigParser()
        config.read('config.ini')

        pwd = configparser.ConfigParser()
        pwd.read('user.pwd')

        host = config['iqfeed']['host']
        port = int(config['iqfeed']['deriv_port'])
        vers = config['iqfeed']['version']

        symbols = config['market']['symbols']
        symbols = symbols.split(',')

        print('Starting IQConnect.exe')
        iqthread = start_iqconnect( pwd['iqfeed']['productID'],
                                    pwd['iqfeed']['iq_user'],
                                    pwd['iqfeed']['iq_pass'])

        print('Initializing Logger')
        log = logger.Logger(    pwd['telegram']['botToken'],
                                pwd['telegram']['chatID'],
                                config['system']['log_path'])

        print('Initializing database connection')
        db_queue = queue.Queue()
        db = postgres.DatabaseConnection(db_queue)
        db.connect()

        print('Initializing listener')
        iq_queue = queue.Queue()
        listen = listener.Listener(iq_queue,db_queue,log,symbols)
        listen.start_listening()

        print('Initilizing socket connection')
        conn = connection.BarsConnection(host,port,vers,iq_queue)
        conn.connect()

        t = db._get_tables()
        print('Tables in database:',t)

        # For dev, from here forward, use the print lock in conn object

        conn.print_msg('Requesting subscription to symbols')

        in_sec = config['market']['interval_seconds']
        start = config['market']['start_time']
        end = config['market']['end_time']

        for sym in symbols:
            cmd = f'BW,{sym},{in_sec},20201204 090000,,,{start},{end},B-{sym}-{in_sec},s,,\r\n'
            conn.send_cmd(cmd)
        
        #cmd = 'wSPY\r\n' # for live market subscription

        sleep(5)
        conn.print_msg('Requesting symbols watched')
        conn.send_cmd('S,REQUEST WATCHES\r\n')
        sleep(2)

        conn.print_msg('Closing socket & killing thread')
        conn.disconnect()
        listen.stop_listening()

        conn.print_msg('Closing database connection & killing thread')
        db.disconnect()

        iqthread.join(timeout=30)

        if iqthread.is_alive():
            print('ERROR iqthread is still alive!')

        print('All done!\n')

        print('Pulling dataframes')
        dfs = db.get_dataframes()

        for df in dfs:
            print(f'Rows in {df[0]} dataframe = {df[1].shape[0]}')
            print('\n')
            print(df[1])
            print('\n')

    except Exception as e:
        logging.exception('Main Loop Exception!')
    finally:
        print('Shutting threads down...')
        if conn:
            conn.disconnect()
        if listen:
            listen.stop_listening()
        if db:
            db.disconnect()
        if iqthread:
            iqthread.join(timeout=30)