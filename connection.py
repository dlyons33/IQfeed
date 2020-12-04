import socket
import threading

'''
Next step:
read_messages & process_messages
'''

class BarsConnection():

    def __init__(self,host,port):
        self._name = 'LiveBarListener'
        self._host = host
        self._port = port

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self._stop = threading.Event()
        self._reader_thread = thrading.Thread(group=None,target=self,name=self._name,
                                                args=(),kwargs={},daemon=None)

        self._buffered_data = ''

    def __call__(self):
        while not self._stop.is_set():
            if self.read_socket():
                self.process_messages()

    def connect(self):
        '''
        Need 'S,CONNECT' or 'S,Set Client Name,' messages to gateway?
        '''
        try:
            self._sock.connect((self._host,self._port))
            self.start_reader()
            print('Socket connected & reader thread started!')
        except Exception as e:
            print('\n===============================================\n')
            print('ERROR connecting to socket or starting thread!')
            print(e)
            print('\n===============================================\n')

    def disconnect(self):
        self.send_cmd('S,UNWATCH ALL') # terminate subscriptions?
        self.stop_reader()
        if self._sock:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
            self._sock = None

    def start_reader(self):
        self._stop.clear()
        if not self._reader_thread.is_alive():
            self._reader_thread.start()

    def stop_reader(self):
        self._stop.set()
        if self._reader_thread.is_alive():
            self._reader_thread.join(30)
        if self._reader_thread.is_alive():
            print('ERROR! Reader thread still ALIVE!')

    def read_socket(self):
        data_received = self._sock.recv(1024).decode()
        if data_received:
            print('\nNew data received from socket:')
            print(data_received,'\n')
            self._buffered_data += data_received
            return True
        else:
            return False

    def next_message(self):
        next_delim = self._buffered_data.find('\n')
        if next_delim != -1:
            message = self._buffered_data[:next_delim].strip()
            print('\nNew full message buffered:')
            print(message,'\n')
            self._buffered_data = self._buffered_data[(next_delim + 1):]
            return message
        else:
            return ''

    def process_messages(self):
        message = self.next_message()
        while message != '':
            fields = message.split(',')
            print('\n===============================================\n')
            for f in fields:
                print(f)
            print('\n===============================================\n')
            message = self.next_message()

    def send_cmd(cmd):
        self._sock.sendall(cmd.encode(encoding='latin-1'))
        print('\nSent string:')
        print(cmd,'\n')