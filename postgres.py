import pandas as pd
import configparser
import threading
import psycopg2

class DatabaseConnection():

    def __init__(self,msg_queue):
        self._queue = msg_queue
        self._stop = threading.Event()
        self._database_thread = threading.Thread(target=self,name='DatabaseThread')

        self._lock = threading.RLock()

        self._conn = None
        self._cursor = None

    ###########################################################################
    # Starting and stopping db connection & thread
    def connect(self):
        try:
            self._conn = psycopg2.connect(database=config['database']['db_name'],
                                        user=pwd['postgres']['db_user'],
                                        password=pwd['postgres']['db_pass'],
                                        host=config['database']['db_host'],
                                        port=config['database']['db_port'])
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
        self._queue.join(timeout=30)
        print('Killing database thread')
        self._stop.set()
        self._cursor.close()
        self._conn.close()
        if self._database_thread.is_alive():
            self._database_thread.join()
        if self._database_thread.is_alive():
            print('WARNING: Database thread may still be alive!')

    def __call__(self):
        while not self._stop.is_set():
            self._pull_queue()

    ###########################################################################
    # Database calls
    def _pull_queue(self):
        pass

    def _get_tables(self,conn):
        with self._lock:
            self._cursor.execute("select * from information_schema.tables where table_schema = 'public'")
            tups = self._cursor.fetchall()

        if tups:
            tables = []
            for t in tups:
                tables.append(t[2])
            return [t.upper() for t in tables]
        else:
            raise Exception('Failed to pull public tables to ID symbols')