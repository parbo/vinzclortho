import Queue
import threading

class Executor(object):
    def __init__(self):
        self._queue = Queue.Queue()
        self._shutdown = False
        self._thread = threading.Thread(target=self._worker)
        self._thread.daemon = True
        self._thread.start()

    def _worker(self):
        while True:
            try:
                func, oncomplete = self._queue.get(block=True, timeout=0.1)
            except Queue.Empty:
                if self._shutdown:
                    return
            else:
                err = None
                res = None
                try:
                    res = func()
                except Exception as e:
                    err = e
                oncomplete((res, err))

    def defer(self, func, oncomplete):
        self._queue.put((func, oncomplete))
