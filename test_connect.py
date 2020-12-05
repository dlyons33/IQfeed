from time import sleep
import configparser
import subprocess
import threading
import queue
import connection
import bridge_listener

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

    config = configparser.ConfigParser()
    config.read('config.cfg')

    host = config['system']['host']
    port = int(config['system']['deriv_port'])
    vers = config['system']['version']

    dfqueue = queue.Queue()

    print('Starting IQConnect.exe')
    iqthread = start_iqconnect( config['credentials']['productID'],
                                config['credentials']['userID'],
                                config['credentials']['pass'])

    print('Initilizing socket connection')
    conn = connection.BarsConnection(host,port,vers,dfqueue)
    conn.connect()

    print('Initializing listener')
    listen = bridge_listener.Listener(dfqueue)
    listen.start_listening()

    print('Requesting subscription to SPY')
    cmd = 'BW,SPY,30,20201204 093000,,,093000,160000,TEST,s,,\r\n'
    #cmd = 'wSPY\r\n' # for live market subscription

    conn.send_cmd(cmd)

    sleep(3)
    print('Requesting symbols watched')
    conn.send_cmd('S,REQUEST WATCHES\r\n')
    sleep(3)

    print('Closing socket & killing thread')
    conn.disconnect()
    listen.stop_listening()
    df = listen.get_df()

    iqthread.join(timeout=30)

    if iqthread.is_alive():
        print('ERROR iqthread is still alive!')

    print('All done!')

    print(df)