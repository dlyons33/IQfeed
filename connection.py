import socket
import threading

'''
Next step:
self._listeners = []
def subscribe()
def unsubscribe()
def push_to_listeners() # push new bars
'''

class BarsConnection():

    def __init__(self,host,port):
        self._name = 'LiveBarListener'
        self._host = host
        self._port = port

        # Need Version from a config file

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock_lock = threading.RLock()
        self._buff_lock = threading.RLock()
        self._buffered_data = ''

        self._stop = threading.Event()
        self._reader_thread = thrading.Thread(target=self,name=self._name)

    def __call__(self):
        while not self._stop.is_set():
            if self._read_socket():
                self._process_messages()

    ###########################################################################
    # Starting and stopping socket & thread
    def connect(self):
        '''
        Need 'S,CONNECT' or 'S,Set Client Name,' messages to gateway?
        '''
        try:
            self._sock.connect((self._host,self._port))
            self._start_reader()
            print('Socket connected & reader thread started!')
        except Exception as e:
            print('\n===============================================\n')
            print('ERROR connecting to socket or starting thread!')
            print(e)
            print('\n===============================================\n')

    def disconnect(self):
        self.send_cmd('S,UNWATCH ALL') # terminate subscriptions?
        self._stop_reader()
        if self._sock:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
            self._sock = None

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
        data_received = self._sock.recv(1024).decode()
        if data_received:
            print('\nNew data received from socket:')
            print(data_received,'\n')
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
                print('\nNew full message buffered:')
                print(message,'\n')
                self._buffered_data = self._buffered_data[(next_delim + 1):]
                return message
            else:
                return ''

    def _process_messages(self):
        message = self._next_message()
        while message != '':
            fields = message.split(',')
            print('\n===============================================\n')
            for f in fields:
                print(f)
            print('\n===============================================\n')
            message = self._next_message()

    def send_cmd(cmd):
        with self._sock_lock:
            self._sock.sendall(cmd.encode(encoding='latin-1'))
            print('\nSent string:')
            print(cmd,'\n')

    ###########################################################################
    # IQFeed protocols

    def _set_protocol(self, protocol):
        '''  S,SET PROTOCOL,[MAJOR VERSION].[MINOR VERSION]<CR><LF> '''
        self.send_cmd(f'S,SET PROTOCOL,{str(self.protocol)}\r\n')


    # Future code - subscribe & unsubscribe, push to subs