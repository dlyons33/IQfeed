import pandas as pd
import configparser
import threading
import psycopg2
import queue
import traceback
from time import sleep

class DatabaseConnection():

    def __init__(self,msg_queue,logger):

        self._settings = {}
        self._symbols = []
        self._tables = []
        self._logger = logger

        self._queue = msg_queue
        self._stop = threading.Event()
        self._database_thread = threading.Thread(target=self,name='DatabaseThread')

        self._cursor_lock = threading.RLock()

        self._conn = None
        self._cursor = None

        self._load_settings()
        assert (len(self._symbols) > 0), 'Database class failed to pull symbols from config!'

    ###########################################################################
    # Starting and stopping db connection & thread

    # Target for thread
    def __call__(self):
        while not self._stop.is_set():
            self._pull_queue()

    def connect(self):
        self._conn = psycopg2.connect(  database=self._settings['name'],
                                        user=self._settings['user'],
                                        password=self._settings['pass'],
                                        host=self._settings['host'],
                                        port=self._settings['port'])
        self._cursor = self._conn.cursor()
        assert self._conn is not None
        assert self._cursor is not None
        print('Connected to database')

        self._map_symbols_and_tables()
        self._start_db_thread()

    def _start_db_thread(self):
        self._stop.clear()
        if not self._database_thread.is_alive():
            self._database_thread.start()
            print('Database thread alive')

    def disconnect(self):
        print('Waiting for database queue to clear')
        self._queue.join()
        print('Closing database connection and killing thread')
        self._stop.set()
        self._cursor.close()
        self._conn.close()
        if self._database_thread.is_alive():
            self._database_thread.join()
        if self._database_thread.is_alive():
            print('WARNING: Database thread may still be alive!')

    def _load_settings(self):
        config = configparser.ConfigParser()
        config.read('config.ini')

        pwd = configparser.ConfigParser()
        pwd.read('user.pwd')

        self._settings['name'] = config['database']['db_name']
        self._settings['user'] = pwd['postgres']['db_user']
        self._settings['pass'] = pwd['postgres']['db_pass']
        self._settings['host'] = config['database']['db_host']
        self._settings['port'] = config['database']['db_port']

        symbols = config['market']['symbols']
        self._symbols = symbols.split(',')

    def _map_symbols_and_tables(self):
        self._tables = self._get_tables()
        assert (len(self._tables) > 0), 'Database class failed to pull tables from database!'
        
        missing_symbols = []
        for symbol in self._symbols:
            if symbol not in self._tables:
                missing_symbols.append(symbol)
        
        try:
            assert(len(missing_symbols) == 0)
        except AssertionError:
            self._logger.log(f"WARNING! Not all tracked symbols are in database! Missing: {missing_symbols}",how='pft')
            for m_symbol in missing_symbols:
                self._create_table(m_symbol)
        else:
            self._logger.log('All tracked symbols are in database',how='tpf')

    ###########################################################################
    # Processing bars & database calls
    def _pull_queue(self):
        ''' Keep pulling so long as items are in queue - sleep if queue is empty '''
        data = None
        try:
            data = self._queue.get(block=False)
            self._process_data(data)
            self._queue.task_done()
        except queue.Empty:
            sleep(1)

    def _process_data(self,data):
        if data:
            if data.symbol in self._tables:
                self._insert_record(data)
            else:
                self._logger.log(f'WARNING: Received unregistered symbol (table not found): {data.symbol} - {data.date} {data.time}',how='fp')

    def _insert_record(self,data):
        vals = f"'{data.symbol}','{data.date}','{data.time}','{data.open}','{data.high}','{data.low}','{data.close}','{data.volume}','{data.cumvol}'"
        table_name = data.symbol.lower()
        instruction = f"insert into {table_name} (symbol,date,time,open,high,low,close,volume,cumvol) values ({vals});"
        with self._cursor_lock:
            try:
                self._cursor.execute(instruction)
                self._conn.commit()
            except Exception as e:
                self._logger.log('Database insertion error!',how='tfp')
                print(e)
                raise

    def _get_tables(self):
        results = None

        with self._cursor_lock:
            self._cursor.execute("select * from information_schema.tables where table_schema = 'public'")
            results = self._cursor.fetchall()

        if (len(results) > 0):
            tables = []
            for t in results:
                tables.append(t[2])
            return [t.upper() for t in tables]
        else:
            self.disconnect()
            raise Exception('Failed to pull public tables from database (needed to ID symbols)')

    def _create_table(self,symbol):
        name = symbol.lower()
        instruction = f"""      create table {name} (
                                id BIGSERIAL NOT NULL PRIMARY KEY,
                                symbol VARCHAR(10) NOT NULL,
                                date DATE NOT NULL,
                                time TIME(0) WITHOUT TIME ZONE NOT NULL,
                                open NUMERIC NOT NULL,
                                high NUMERIC NOT NULL,
                                low NUMERIC NOT NULL,
                                close NUMERIC NOT NULL,
                                volume NUMERIC NOT NULL,
                                cumvol NUMERIC NOT NULL
                                );"""
        with self._cursor_lock:
            try:
                self._cursor.execute(instruction)
                self._conn.commit()
            except Exception as e:
                msg = f'ERROR! Failure to create table for symbol {symbol}!'
                self._logger.log(msg,how='tpf')
                print(traceback.format_exc())
            else:
                msg = f'Successfully created table for {symbol}'
                self._logger.log(msg,how='tpf')