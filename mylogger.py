import requests
import threading
from datetime import datetime

class Telegram_Bot():
    '''
    token: controls the bot
    chat: ID for recipient user or channel
    '''

    def __init__(self,token,chat):
        self._base = f'https://api.telegram.org/bot{token}/sendMessage?chat_id={chat}&text='

    def send_message(self,msg):
        if msg is not None:
            url = self._base + msg
            self._get_url(url)

    def wake_up(self):
        msg = 'Initializing IQFeed logger... Get that bread!'
        url = self._base + msg
        self._get_url(url)

    def _get_url(self,url):
        requests.get(url)

class Txt_Log():
    ''' For logging to local .txt file '''

    def __init__(self,directory):
        self._directory = directory

    def log_msg(self,msg):
        dt = datetime.now()
        _date = dt.date().isoformat()
        _time = dt.time().isoformat(timespec='seconds')

        prefix = f'{_date}  {_time}  '

        file = _date.replace('-','.') + '_Log'
        path = f'{self._directory}{file}.txt'
        
        with open(path,'a+') as f:
            f.write(f'{prefix}{msg}\n')

class Logger():

    '''
    how [string] = any combination of:
    t = telegram
    f = file
    p = print
    '''
    valid_how = ['t','f','p']

    def __init__(self,botToken,botChat,directory):
        self._bot = Telegram_Bot(botToken,botChat)
        self._file = Txt_Log(directory)
        self._lock = threading.RLock()

    def log(self,msg,how='tfp'):
        
        assert msg, "Log received invalid message"
        assert (self._validate_how(how) == True), "Log received invalid method"

        with self._lock:
            if 'p' in how:
                self._print_msg(msg)
            if 'f' in how:
                self._file.log_msg(msg)
            if 't' in how:
                self._bot.send_message(msg)

    def _print_msg(self,msg):
        print('*************',msg)

    def wake_bot(self):
        with self._lock:
            self._bot.wake_up()

    def _validate_how(self,how):
        letters = [char for char in how]
        return all([ltr in Logger.valid_how for ltr in letters])