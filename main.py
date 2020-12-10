from time import sleep
import configparser
import subprocess
import threading
import queue
import logging
import connection
import listener
import mylogger
import postgres
import traceback

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

    # Connection variables
    conn = None
    db = None
    listen = None
    iqthread = None

    # First, map symbols in config to tables in database
    #create_tables.map_tables()

    try:

        config = configparser.ConfigParser()
        config.read('config.ini')

        pwd = configparser.ConfigParser()
        pwd.read('user.pwd')

        symbols = config['market']['symbols']
        symbols = symbols.split(',')
        print('Tracking symbols:',symbols)

        print('Starting IQConnect.exe')
        iqthread = start_iqconnect( pwd['iqfeed']['productID'],
                                    pwd['iqfeed']['iq_user'],
                                    pwd['iqfeed']['iq_pass'])

        print('Initializing Logger')
        mylog = mylogger.Logger(    pwd['telegram']['botToken'],
                                    pwd['telegram']['chatID'],
                                    config['system']['log_path'])

        # Instantiate Queues
        db_queue = queue.Queue()
        iq_queue = queue.Queue()

        print('Initializing database connection')
        db = postgres.DatabaseConnection(db_queue,mylog)
        db.connect()

        print('Initializing listener')
        listen = listener.Listener(iq_queue,db_queue,mylog,symbols)
        listen.start_listening()

        print('Initilizing IQFeed socket connection')
        conn = connection.BarsConnection(iq_queue,mylog)
        conn.connect()

        sleep(3) # might be sending listener request too quickly?

        conn.subscribe_to_symbols(symbols,config)

        # Loop until user --> CTRL-C
        run = True
        while run:
            sleep(5)


    except KeyboardInterrupt:
        print('User terminating application...')
        run = False
    except Exception as e:
        msg = 'Main Loop Exception!'
        logging.exception(msg)
        # mylog.log(msg,how='tfp')
        print(traceback.format_exc())
    finally:
        print('Shutting threads down...')
        if conn:
            conn.disconnect()
        if listen:
            listen.stop_listening()
        if db:
            db.disconnect()
        if iqthread:
            print('Waiting for IQconnect.exe to shut down')
            if iqthread.is_alive():
                iqthread.join(timeout=30)
            if iqthread.is_alive():
                print('ERROR iqthread is still alive!')
        print('Shutting down...')