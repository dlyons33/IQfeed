import pandas as pd
from collections import namedtuple
import threading
import queue
from time import sleep

class Listener():
    def __init__(self,iq_queue,db_queue,logger,symbols):
        '''
        For logging:
        logger.log(msg,how)
        how = string = comination of 'f','p','t'
        'f' = write to file; 'p' = print; 't' = Telegram message
        '''
        self._symbol_list = symbols

        self._db_queue = db_queue
        self._Bar = namedtuple('Bar',['symbol','date','time','open',
                                'high','low','close','volume','cumvol'])

        self._logger = logger

        self._iq_queue = iq_queue
        self._listener_thread = threading.Thread(target=self,name='ListenerThread')
        self._stop = threading.Event()

        self._process_funcs = {}
        self._set_message_mappings()

    ###########################################################################
    # Threading functions
    def __call__(self):
        while not self._stop.is_set():
            self._pull_queue()

    def start_listening(self):
        self._stop.clear()
        if not self._listener_thread.is_alive():
            self._listener_thread.start()
            print('Listening on queue...')

    def stop_listening(self):
        # Main thread calls to stop - wait until all items are processed
        print('Waiting for listener queue to clear before killing')
        self._iq_queue.join()
        print('Killing Listener thread')
        self._stop.set()
        if self._listener_thread.is_alive():
            self._listener_thread.join(30)
        if self._listener_thread.is_alive():
            print('ERROR! Listener thread may still be alive!')

    ###########################################################################
    # Message parsing
    def _pull_queue(self):
        '''
        Keep pulling so long as items are in queue - sleep if queue is empty
        Queue items should only be a list of fields
        '''
        fields = None
        try:
            # With block=False, will raise exception if no item immediately available
            # Hence try/except block (may also be a way to use Select module)
            fields = self._iq_queue.get(block=False)
            handle_func = self._process_function(fields)
            handle_func(fields)
            self._iq_queue.task_done()
        except queue.Empty:
            sleep(1)

    def _set_message_mappings(self):

        self._process_funcs['n'] = self._process_wrong_symbol
        self._process_funcs['E'] = self._process_error_msg
        self._process_funcs['T'] = self._process_timestamp
        self._process_funcs['B'] = self._process_bar
        self._process_funcs['S'] = self._process_system_msg

    def _process_function(self,fields):
        pf = self._process_funcs.get(fields[0][0]) # first letter of first field
        if pf is not None:
            return pf
        else:
            return self._process_unregistered_message

    def _process_bar(self,fields):
        ###############################
        # Will need to take into account TYPE of bar
        # Type = field[1] = 'B[type]'
        # types: U = update, H = complete from history, C = complete new live bar
        ###############################
        assert fields[0][0] == 'B'
        assert fields[2] in self._symbol_list

        # field[3] = datetime = 'YYYY-MM-DD HH:MM:SS'
        dt = fields[3].split(' ')

        bar = self._Bar(
        symbol=fields[2],
        date=dt[0],
        time=dt[1],
        open=fields[4],
        high=fields[5],
        low=fields[6],
        close=fields[7],
        volume=fields[9],
        cumvol=fields[8],
        )

        self._db_queue.put(bar)

        #########################################################################
        # THIS IS WHERE I NEED TO CHANGE ONCE DATABASE IS COMPLETE

        if fields[1][1] == 'U':
            pass
        elif fields[1][1] == 'H':
            pass
        elif fields[1][1] == 'C':
            pass
        else:
            msg = 'Unidentified Bar Type Field!'
            self._logger(msg,how='tfp')
            raise Exception(msg)
        #########################################################################

    def _process_system_msg(self,fields):
        assert len(fields) > 1
        assert fields[0] == 'S'
        if fields[1] == 'WATCHES':
            self._msg_watches(fields)
        else:
            msg = ','.join(fields[1:])
            self._logger.log(msg,how='tfp')

    def _process_error_msg(self,fields):
        assert len(fields) > 1
        assert fields[0] == 'E'
        if fields[1][:7] == 'INVALID':
            msg = f'{fields[1]}'
            self._logger.log(msg,how='tfp')
        else:
            self._process_unregistered_message(fields)

    def _process_timestamp(self,fields):
        # T,[YYYYMMDD HH:MM:SS]
        assert len(fields) > 1
        assert fields[0] == "T"
        self._logger.log(f'Heartbeat: {fields[1]}',how='p')

    def _process_wrong_symbol(self,fields):
        assert field[0] == 'n'
        assert len(fields) > 1
        msg = f'IQFeed: Invalid symbol {fields[1]}'
        self._logger.log(msg,how='tfp')

    def _process_unregistered_message(self,fields):
        msg = ','.join(fields)
        self._logger.log(f'UNREGISTERED MESSAGE: {msg}',how='tfp')

    def _msg_watches(self,fields):
        assert len(fields) >= 5
        assert ((len(fields)-2) % 3) == 0
        elements = fields[2:]
        while len(elements) > 0:
            sym = elements.pop(0)
            intv = elements.pop(0)
            reqid = elements.pop(0)
            msg = f'WATCHING: Symbol: {sym}   Interval: {intv}   RequestID: {reqid}'
            self._logger.log(msg,how='fp')