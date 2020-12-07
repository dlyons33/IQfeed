import pandas as pd
import configparser
import threading
import psycopg2

class DatabaseConnection():

    def __init__(self,msg_queue,logger):
        self._logger = logger

        self._settings = {}
        self._symbols = []
        self._df_tups = []
        self._columns = ['symbol','date','time','open','high','low','close','volume','cumvol']

        self._queue = msg_queue
        self._stop = threading.Event()
        self._database_thread = threading.Thread(target=self,name='DatabaseThread')

        self._cursor_lock = threading.RLock()

        self._conn = None
        self._cursor = None

        self._load_settings()

    ###########################################################################
    # Starting and stopping db connection & thread
    def connect(self):
        try:
            self._conn = psycopg2.connect(database=self._settings['name'],
                                        user=self._settings['user'],
                                        password=self._settings['pass'],
                                        host=self._settings['host'],
                                        port=self._settings['port'])
            self._cursor = self._conn.cursor()
            print('Connected to database')
            assert self._conn is not None
            assert self._cursor is not None
        except Exception as e:
            print('ERROR - Failed to connect to database!')
            print(e)
            raise
        self._start_db_thread()

    def _start_db_thread(self):
        self._stop.clear()
        if not self._database_thread.is_alive():
            self._database_thread.start()
            print('Database thread alive')

    def disconnect(self):
        print('Waiting for database queue to clear')
        self._queue.join()
        print('Killing database thread')
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

        self._symbols = config['market']['symbols']
        self._df_tups = [(sym,[]) for sym in self._symbols.split(',')]

    def __call__(self):
        while not self._stop.is_set():
            self._pull_queue()

    ###########################################################################
    # Database calls
    def _pull_queue(self):
        data = None
        try:
            data = self._queue.get(block=False)
            self._process_data(data)
            self._queue.task_done()
            print('Queue Size:',self._queue.qsize())
        except queue.Empty:
            pass

    def _process_data(self,data):
        if data:
            found = False
            for tup in self._df_tups:
                if tup[0] == data.symbol:
                    tup[1].append(   [data.symbol,
                                    data.date,
                                    data.time,
                                    data.open,
                                    data.high,
                                    data.low,
                                    data.close,
                                    data.volume,
                                    data.cumvol])
                    found = True
                    break
            if not found:
                print('ERROR - UNREGISTERED SYMBOL RECEIVED!')

        self._insert_record(data)

    def _insert_record(self,data):
        vals = f"'{data.symbol}','{data.date}','{data.time}','{data.open}','{data.high}','{data.low}','{data.close}','{data.volume}','{data.cumvol}'"
        instruction = f"insert into test (symbol,date,time,open,high,low,close,volume,cumvol) values ({vals});"
        with self._cursor_lock:
            try:
                self._cursor.execute(instruction)
                self._conn.commit()
                print('Inserted:',vals)
            except Exception as e:
                print('Database instruction/execution error:')
                print(e)

    def _get_tables(self):
        with self._cursor_lock:
            self._cursor.execute("select * from information_schema.tables where table_schema = 'public'")
            tups = self._cursor.fetchall()

        if tups:
            tables = []
            for t in tups:
                tables.append(t[2])
            return [t.upper() for t in tables]
        else:
            self.disconnect()
            raise Exception('Failed to pull public tables to ID symbols')

    def get_dataframes(self):
        output = []
        for tup in self._df_tups:
            df = pd.DataFrame(data=tup[1],columns=self._columns)
            output.append((tup[0],df))
        return output