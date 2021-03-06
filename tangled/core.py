# -*- coding: utf-8 -*-
#
# Copyright (c) 2001-2010 Pär Bohrarper.
# See LICENSE for details.

import asynchat
import asyncore
import socket
import threading
import functools
import Queue
import sys
import time
import heapq
import select
import traceback

import logging
log = logging.getLogger("tangled.core")

__version__ = "0.1.1.2"

def succeed(r):
    """Syntactic sugar for making a synchronous call look asynchronous"""
    d = Deferred()
    d.callback(r)
    return d

def fail(r):
    """Syntactic sugar for making a synchronous call look asynchronous, failure version"""
    d = Deferred()
    d.callback(r)
    return d

def passthru(r):
    """A callback/errback that doesn't do anything"""
    return r

class Worker(threading.Thread):
    """
    This is a worker thread which executes a function and calls a callback on completion

    @param reactor: The reactor this is a worker for.
    @type reactor: L{Reactor}
    @param autostart: If true, the worker thread starts immediately. Otherwise start() has to be called.
    """
    def __init__(self, reactor, autostart=False):
        threading.Thread.__init__(self, target=self._runner)
        self._queue = Queue.Queue()
        self.reactor = reactor
        self._running = True
        self.daemon = True
        if autostart:
            self.start()

    def stop(self):
        """Stops the worker"""
        self._running = False

    def _runner(self):
        """The message pump of the worker"""
        while self._running:
            try:
                func, oncomplete = self._queue.get(block=True, timeout=1)
            except Queue.Empty:
                pass
            else:
                res = None
                try:
                    res = func()
                except:
                    res = Failure()
                oncomplete(res)

    def execute(self, func, oncomplete):
        """
        Executes func in the worker thread, which then calls oncomplete

        @type func: callable
        @type oncomplete: callable
        """
        self._queue.put((func, oncomplete))

    def defer(self, func):
        """
        Defers the call to func to this worker

        @param func: The function you want the worker to call
        @type func: callable
        @return: A L{Deferred} object that will eventually get the result of func
        @rtype: L{Deferred}
        """
        return self.reactor.defer_to_worker(func, self)

class Failure(object):
    """Like Twisted's Failure object, but with no features"""
    def __init__(self, type_=None):
        if type_ is None:
            self.type, self.value, self.tb = sys.exc_info()
        else:
            self.type = type_
            self.value = None
            self.tb = None

    def __str__(self):
        return repr(traceback.format_exception(self.type, self.value, self.tb))

    def raise_exception(self):
        """Re-raises the exception"""
        raise self.type, self.value, self.tb

    def check(self, *exceptions):
        """
        This can be used to have try/except like blocks in your errback
        """
        for e in exceptions:
            if isinstance(self.type, e):
                return True
        return False


class Deferred(object):
    """Very similar to Twisted's Deferred object, but with less features"""
    def __init__(self):
        self.callbacks = []
        self.called = False
        self.paused = 0

    def _start_callbacks(self, result):
        if not self.called:
            self.called = True
            self.result = result
            self._run_callbacks()

    def _run_callbacks(self):
        if not self.paused:
            while self.callbacks:
                try:
                    cb, eb = self.callbacks.pop(0)
                    if isinstance(self.result, Failure):
                        cb = eb
                    self.result = cb(self.result)
                    if isinstance(self.result, Deferred):
                        self.pause()
                        # This will cause the callback chain to resume later,
                        # or immediately (recursively) if result is already
                        # available
                        self.add_both(self._continue)
                except:
                    self.result = Failure()
            if isinstance(self.result, Failure):
                log.error("Unhandled Failure: %s", self.result)

    def add_callback(self, cb):
        """See L{add_callbacks}"""
        self.add_callbacks(cb)

    def add_errback(self, eb):
        """
        Adds errback only (a passthru will be used for the callback, so the result is not lost)

        @param eb: errback
        """
        self.add_callbacks(passthru, eb)

    def add_both(self, cb):
        """
        Adds one function as both callback and errback

        @param cb: callback and errback
        """
        self.add_callbacks(cb, cb)

    def add_callbacks(self, cb, eb=None):
        """
        Adds callback and errback

        @param cb: callback
        @param eb: errback
        """
        self.callbacks.append((cb, eb or passthru))
        if self.called:
            self._run_callbacks()

    def pause(self):
        self.paused = self.paused + 1

    def unpause(self):
        self.paused = self.paused - 1
        if self.paused > 0:
            return
        if self.called:
            self._run_callbacks()

    def _continue(self, result):
        self.result = result
        self.unpause()

    def callback(self, result):
        self._start_callbacks(result)

    def errback(self, fail=None):
        if not isinstance(fail, Failure):
            fail = Failure(fail)
        self._start_callbacks(fail)


def set_reuse_addr(s):
    """Try to re-use a server port if possible. Useful in development."""
    try:
        s.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR,
            s.getsockopt(socket.SOL_SOCKET,
                         socket.SO_REUSEADDR) | 1
            )
    except socket.error:
        pass

class BindError(Exception):
    pass

class Trigger(asyncore.dispatcher):
    """Used to trigger the event loop with external stuff,
    also from Medusa"""

    def __init__(self):
        a = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        w = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        set_reuse_addr(a)
        set_reuse_addr(w)

        # set TCP_NODELAY to true to avoid buffering
        w.setsockopt(socket.IPPROTO_TCP, 1, 1)

        # tricky: get a pair of connected sockets
        host='127.0.0.1'
        port=19999
        while 1:
            try:
                self.address = (host, port)
                a.bind(self.address)
                break
            except:
                if port <= 19950:
                    raise BindError
                port = port - 1

        a.listen(1)
        w.setblocking(0)
        try:
            w.connect(self.address)
        except:
            pass
        r, addr = a.accept()
        a.close()
        w.setblocking(1)
        self.trigger = w

        self.lock = threading.Lock()
        self.funcs = []

        asyncore.dispatcher.__init__(self, r)

    def readable(self):
        return 1

    def writable(self):
        return 0

    def handle_connect(self):
        pass

    def pull_trigger(self, func=None):
        if func:
            try:
                self.lock.acquire()
                self.funcs.append(func)
            finally:
                self.lock.release()
                self.trigger.send('x')

    def handle_read(self):
        self.recv(8192)
        try:
            self.lock.acquire()
            for func in self.funcs:
                func()
            self.funcs = []
        finally:
            self.lock.release()

class Reactor(object):
    """The reactor is the engine of your asynchronous application."""
    # trigger object to wake the loop
    _trigger = Trigger()
    use_poll = False

    def __init__(self):
        self._pending_calls = []

    def wake(self):
        """Uses the trigger to wake the async loop"""
        self._trigger.pull_trigger()

    def run_in_main(self, func):
        """
        Wakes the async loop, and calls func in it

        @param func: The function to call from the main loop
        """     
        self._trigger.pull_trigger(func)

    def defer_to_worker(self, func, worker):
        """
        Calls a function in a worker, and return the result as a L{Deferred}

        @param func: The function to call in the worker
        @param worker: The worker that should handle the call
        @type worker: L{Worker}
        @return: A L{Deferred} objeft that will eventually contain the result
        @rtype: L{Deferred}
        """
        d = Deferred()
        def callback(result):
            if isinstance(result, Failure):
                self.run_in_main(functools.partial(d.errback, result))
            else:
                self.run_in_main(functools.partial(d.callback, result))
        worker.execute(func, callback)
        return d

    def call_later(self, func, timeout):
        """
        Call a function at a later time in the main loop
   
        @param func: The function to call
        @param timeout: How long (in seconds) to the call shall be made
        """
        heapq.heappush(self._pending_calls, (time.time() + timeout, func))
        self.wake()

    def _timeout(self):
        if not self._pending_calls:
            return None
        return max(0, self._pending_calls[0][0] - time.time())

    def loop(self):
        if self.use_poll and hasattr(select, 'poll'):
            poll_fun = asyncore.poll2
        else:
            poll_fun = asyncore.poll

        while asyncore.socket_map:
            timeout = self._timeout()
            poll_fun(timeout, asyncore.socket_map)
            # check expired timeouts
            t = time.time()
            while self._pending_calls:
                timeout, func = self._pending_calls[0]
                if timeout < t:
                    func()
                    heapq.heappop(self._pending_calls)
                else:
                    # No timeout
                    break

