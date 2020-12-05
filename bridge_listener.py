import threading
import queue
import pandas as pd

class Listener():
    def __init__(self,msgqueue):
        self._queue = msgqueue
        self._listener_thread = threading.Thread(target=self,name='ListenerThread')
        self._stop = threading.Event()

        self._dict_list = []

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
        print('Waiting for queue to clear before killing')
        self._queue.join()
        print('Killing listener thread')
        self._stop.set()
        if self._listener_thread.is_alive():
            self._listener_thread.join(30)
        if self._listener_thread.is_alive():
            print('ERROR! Listener thread still ALIVE!')

    def _pull_queue(self):
        '''
        Queue should only be dictionaries representing bars/rows
        '''
        dic = None
        try:
            dic = self._queue.get(block=False)
            self._queue.task_done()
            if dic:
                self._dict_list.append(dic)
        except:
            pass

    def get_df(self):
        df = pd.DataFrame(data=self._dict_list)
        return df