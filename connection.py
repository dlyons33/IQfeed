import numpy as np
import socket
import threading

'''
Next step:
read_messages & process_messages
'''

SYMBOLS = ['SPY']

class BarsConnection():

    def __init__(self,host,port):
        self._name = 'LiveBarListener'
        self._host = host
        self._port = port

        self._listeners = []

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self._stop = threading.Event()
        self._reader_thread = thrading.Thread(group=None,target=self,name=self._name,
                                                args=(),kwargs={},daemon=None)

        self._buffd_data = ''

    def __call__(self):
        while not self._stop.is_set():
            if self.read_messages(): # Test out None/True/False returns
                self.process_messages()

    def connect(self):
        '''
        Need 'S,CONNECT' or 'S,Set Client Name,' messages to gateway?
        '''
        self._sock.connect((self._host,self._port))
        self.start_reader()

    def start_reader(self):
        self._stop.clear()
        if not self._reader_thread.is_alive():
            self._reader_thread.start()

    def read_messages(self):
        data_received = self._sock.recv(1024).decode('latin-1')

    def process_messages(self):
        pass

    def add_listener(self,listener):
        if listener not in self._listeners:
            self._listeners.append(listener)

    def remove_listener(self,listener):
        if listener in self._listeners:
            self._listeners.remove(listener)