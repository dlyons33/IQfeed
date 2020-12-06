import socket
import threading
import pandas as pd

class BarsConnection():

    def __init__(self,host,port,version,msgqueue):
        self._name = 'LiveBarListener'
        self._host = host
        self._port = port
        self._version = version

        self._msgqueue = msgqueue

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock_lock = threading.RLock()
        self._buff_lock = threading.RLock()
        self._buffered_data = ''

        self._stop = threading.Event()
        self._reader_thread = threading.Thread(target=self,name=self._name)

        self._printLock = threading.RLock()

    ###########################################################################
    # Callable looped by the new thread (target), listening at the socket
    def __call__(self):
        while not self._stop.is_set():
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
            with self._printLock:
                print('Socket connected & reader thread started!')
        except Exception as e:
            with self._printLock:
                print('\n===============================================\n')
                print('ERROR connecting to socket or starting thread!')
                print(e)
                print('\n===============================================\n')

    def disconnect(self):
        self.send_cmd('S,UNWATCH ALL\r\n')
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
            with self._printLock:
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

    def send_cmd(self,cmd):
        with self._sock_lock:
            #self._sock.sendall(cmd.encode(encoding='latin-1'))
            self._sock.sendall(cmd.encode())
            with self._printLock:
                print('>>>>>>>>>>>>> Sent string:',cmd[:-2])

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
        self.send_cmd(f'S,SET PROTOCOL,{str(self._version)}\r\n')

    def print_msg(self,msg):
        with self._printLock:
            print(msg)

    # Future code - subscribe & unsubscribe, push to subs