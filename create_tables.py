import configparser
import psycopg2
import threading
import traceback

class SymbolTableMapper:
    ''' Ensures all symbols to be followed (in config) have a corresponding table'''

    def __init__(self):

        self._tables = []
        self._symbols = []

        self._settings = {}
        
        self._conn = None
        self._cursor = None
        self._cursor_lock = threading.RLock()


        self._load_settings()
        assert (len(self._symbols) > 0), 'Failed to load symbols list!'

    def connect(self):
        self._conn = psycopg2.connect(  database=self._settings['name'],
                                        user=self._settings['user'],
                                        password=self._settings['pass'],
                                        host=self._settings['host'],
                                        port=self._settings['port'])
        
        self._cursor = self._conn.cursor()

        assert (self._conn is not None)
        assert (self._cursor is not None)
        print('Connected to database')

    def disconnct(self):
        with self._cursor_lock:
            self._cursor.close()
            self._conn.close()
        print('Database connection closed')

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
        print(f'Loaded {len(self._symbols)} symbol(s):',self._symbols)

    def get_tables(self):
        results = None
        with self._cursor_lock:
            self._cursor.execute("select * from information_schema.tables where table_schema = 'public'")
            results = self._cursor.fetchall()
        if (len(results) > 0):
            tables = []
            for t in results:
                tables.append(t[2])
            self._tables = [t.upper() for t in tables]
            print(f'Successfully fetched {len(self._tables)} tables:',self._tables)
        else:
            raise Exception('ERROR - Failed to pull tables from database!')

    def update_tables(self):
        for symbol in self._symbols:
            if symbol in self._tables:
                print('Database already contains a table for',symbol)
            else:
                self._create_table(symbol)

    def _create_table(self,symbol):
        name = symbol.lower()
        instruction = f"""     create table {name} (
                                id BIGSERIAL NOT NULL PRIMARY KEY,
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
                print('ERROR! Failure to create table!')
                print(traceback.format_exc())
            else:
                print('Successfully created table for',symbol)


def map_tables():
    db = None
    try:
        db = SymbolTableMapper()
        db.connect()
        db.get_tables()
        db.update_tables()
    except Exception as e:
        print('MAIN EXCEPTION!')
        print(traceback.format_exc())
    finally:
        if db:
            db.disconnct()

if __name__ == "__main__":
    map_tables()