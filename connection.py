import socket
import threading
import pandas as pd
import os
from time import sleep

class BarsConnection():

    # IQ Feed settings constants
    protocol_version = "6.1"
    iqfeed_host = os.getenv('IQFEED_HOST', "127.0.0.1")
    deriv_port = int(os.getenv('IQFEED_PORT_DERIV', 9400))

    def __init__(self,msgqueue):
        self._name = 'LiveBarListener'
        self._host = BarsConnection.iqfeed_host
        self._port = BarsConnection.deriv_port
        self._version = BarsConnection.protocol_version

        self._msgqueue = msgqueue

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock_lock = threading.RLock()
        self._buff_lock = threading.RLock()
        self._buffered_data = ''

        self._stop = threading.Event()
        self._reader_thread = threading.Thread(target=self,name=self._name)

    ###########################################################################
    # Callable looped by the new thread (target), listening at the socket
    def __call__(self):
        while not self._stop.is_set():
            sleep(2)
            if self._read_socket():
                self._process_messages()

    ###########################################################################
    # Starting and stopping socket & thread
    def connect(self):
        try:
            self._sock.connect((self._host,self._port))
            self._sock.setblocking(False)
            self._set_protocol()
            self._start_reader()
            print('Socket connected & reader thread started!')
        except Exception as e:
            print('ERROR connecting to socket or starting thread!')
            raise

    def disconnect(self):
        self._send_cmd('S,UNWATCH ALL\r\n')
        print('Disconnecting from IQFeed socket and killing thread')
        self._stop_reader()
        if self._sock:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
            self._sock = None
        if self._reader_thread.is_alive():
            self._reader_thread.join(30)
        if self._reader_thread.is_alive():
            print('ERROR: IQ Feed socket reader thread may still be alive!')

    def _start_reader(self):
        self._stop.clear()
        if not self._reader_thread.is_alive():
            self._reader_thread.start()

    def _stop_reader(self):
        self._stop.set()
        if self._reader_thread.is_alive():
            self._reader_thread.join(30)
        if self._reader_thread.is_alive():
            print('ERROR! Reader thread still ALIVE!')

    ###########################################################################
    # Reading from & writing to socket

    def _read_socket(self):
        data_received = None
        try:
            data_received = self._sock.recv(1024).decode()
        except:
            pass
        if data_received:
            with self._buff_lock:
                self._buffered_data += data_received
            return True
        else:
            return False

    def _next_message(self):
        with self._buff_lock:
            next_delim = self._buffered_data.find('\n')
            if next_delim != -1:
                message = self._buffered_data[:next_delim].strip()
                self._buffered_data = self._buffered_data[(next_delim + 1):]
                return message
            else:
                return ''

    def _process_messages(self):
        message = self._next_message()
        while message != '':
            self._queue_message(message)            
            message = self._next_message()

    def _send_cmd(self,cmd):
        with self._sock_lock:
            #self._sock.sendall(cmd.encode(encoding='latin-1'))
            self._sock.sendall(cmd.encode())
            print('>>>>>>>>>>>>> Sent command:',cmd[:-2])

    def _queue_message(self,msg):
        fields = msg.split(',')
        self._msgqueue.put(fields)
        # print('Queue size:',self._msgqueue.qsize())
        # self._msg_count += 1
        # print(f'Queued {self._msg_count} messages')

    ###########################################################################
    # IQFeed protocols

    def _set_protocol(self):
        '''  S,SET PROTOCOL,[MAJOR VERSION].[MINOR VERSION]<CR><LF> '''
        self._send_cmd(f'S,SET PROTOCOL,{str(self._version)}\r\n')

    def subscribe_to_symbols(self,symbols,config):
        '''
        symbols = list of string symbols
        config = configparser object with symbol/market settings
        '''

        in_sec = config['market']['interval_seconds']
        start = config['market']['start_time']
        end = config['market']['end_time']

        currday = pd.Timestamp.now().strftime('%Y%m%d')

        for sym in symbols:
            cmd = f'BW,{sym},{in_sec},{currday} 072000,,,{start},{end},B-{sym}-{in_sec},s,,\r\n'
            self._send_cmd(cmd)
            print('Subscribing to symbol',sym)