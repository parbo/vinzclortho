from BaseHTTPServer import BaseHTTPRequestHandler
import asynchat
import asyncore
import socket
import cStringIO
import threading
import functools
import optparse

import vinzclortho

__version__ = "0.1"

def raise_error(e):
    if e is not None:
        raise e

class AsyncHTTPRequestHandler(asynchat.async_chat, BaseHTTPRequestHandler):
    """An asynchronous HTTP request handler inspired somewhat by the 
    http://code.activestate.com/recipes/440665-asynchronous-http-server/
    recipe.
    """

    server_version = "VinzClortho/" + __version__

    class Pusher(object):
        def __init__(self, obj):
            self.obj = obj
        def write(self, data):
            self.obj.push(data)

    def __init__(self, conn, addr, server):
        asynchat.async_chat.__init__(self, conn)
        self.client_address = addr
        self.connection = conn
        self.server = server
        self.keymaster = server.keymaster
        # set the terminator : when it is received, this means that the
        # http request is complete ; control will be passed to
        # self.found_terminator
        self.set_terminator ('\r\n\r\n')
        self.incoming = []
        self.rfile = None
        self.wfile = AsyncHTTPRequestHandler.Pusher(self)
        self.found_terminator = self.handle_request_line
        self.request_version = "HTTP/1.1"
        self.code = None
        self.trigger = self.server.trigger

    def collect_incoming_data(self,data):
        self.incoming.append(data)

    def handle_junk(self):
        pass

    def create_rfile(self):
        self.rfile = cStringIO.StringIO(''.join(self.incoming))
        self.incoming = []
        self.rfile.seek(0)

    def prepare_request(self):
        """Prepare to read the request body"""
        bytesremaining = int(self.headers.getheader('content-length'))
        # set terminator to length (will read bytesremaining bytes)
        self.set_terminator(bytesremaining)
        self.incoming = []
        # control will be passed to a new found_terminator
        self.found_terminator = self.handle_request_data
    
    def handle_request_data(self):
        """Called when a request body has been read"""
        self.create_rfile()
        # set up so extra data is thrown away
        self.set_terminator(None)
        self.found_terminator = self.handle_junk
        # Actually handle the request
        self.handle_request()

    def handle_request(self):
        """Dispatch the request to a handler"""
        try:
            handler = getattr(self, "do_" + self.command)
            handler()
        except AttributeError:
            self.send_response(501)
            self.end_headers()        
            self.close_when_done()

    def handle_request_line(self):
        """Called when the http request line and headers have been received"""
        # prepare attributes needed in parse_request()
        self.create_rfile()
        self.raw_requestline = self.rfile.readline()
        self.parse_request()

        if self.command in ["PUT", "POST"]:
            # Wait for the data to come in before processing the request
            self.prepare_request()
        else:
            self.handle_request()

    def log_message(self, format, *args):
        pass

    def deferred(self, func):
        return functools.partial(self.trigger.pull_trigger, func)

    def do_request(self):
        do = getattr(self.keymaster, "do_" + self.command)
        try:
            do(self.path, self.rfile, self.deferred(self.complete_request))
        except AttributeError:
            self.send_response(501)
            self.end_headers()        
            self.close_when_done()

    def complete_request(self, result):
        try:
            value, e = result
            raise_error(e)
            self.send_response(200)
            self.end_headers()
            if value is not None:
                self.push(value)
        except KeyError:
            self.send_response(404)
            self.end_headers()
        except:
            raise
            self.send_response(501)
            self.end_headers()
        finally:
            self.close_when_done()

    def do_GET(self):
        self.do_request()

    def do_PUT(self):
        self.do_request()

    def do_POST(self):
        self.do_request()

    def do_DELETE(self):
        self._do_request()


def set_reuse_addr(s):
    # try to re-use a server port if possible
    try:
        s.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR,
            s.getsockopt(socket.SOL_SOCKET,
                         socket.SO_REUSEADDR) | 1
            )
    except socket.error:
        pass

class Trigger(asyncore.dispatcher):
    """Used to trigger the asyncore event loop with external stuff,
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
                    raise 'Bind Error', 'Cannot bind trigger!'
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
        self.thunks = []
        
        asyncore.dispatcher.__init__(self, r)
        
    def readable(self):
        return 1

    def writable(self):
        return 0

    def handle_connect(self):
        pass

    def pull_trigger(self, thunk, params):
        if thunk:
            try:
                self.lock.acquire()
                self.thunks.append((thunk, params))
            finally:
                self.lock.release()
                self.trigger.send('x')

    def handle_read(self):
        self.recv(8192)
        try:
            self.lock.acquire()
            for thunk, params in self.thunks:
                thunk(params)
            self.thunks = []
        finally:
            self.lock.release()

class AsyncHTTPServer(asyncore.dispatcher):
    """Cobbled together from various sources, most of them state that they 
    copied from the Medusa http server.. 
    """
    def __init__(self, keymaster):
        self.keymaster = keymaster
        self.trigger = Trigger()
        self.handler = AsyncHTTPRequestHandler
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(self.keymaster.address)
        self.listen(5)

    def handle_accept(self):
        try:
            conn, addr = self.accept()
        except socket.error:
            self.log_info('warning: server accept() threw an exception', 'warning')
            return
        except TypeError:
            self.log_info('warning: server accept() threw EWOULDBLOCK', 'warning')
            return
        # creates an instance of the handler class to handle the request/response
        # on the incoming connection
        self.handler(conn, addr, self)

if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("-a", "--address", dest="address", default="localhost",
                      help="Bind to ADDRESS", metavar="ADDRESS")
    parser.add_option("-p", "--port", dest="port", action="store", type="int", default=8080,
                      help="Bind to PORT", metavar="PORT")
    (options, args) = parser.parse_args()    

    keymaster = vinzclortho.VinzClortho((options.address, options.port), True)
    server = AsyncHTTPServer(keymaster)
    print 'Starting Vinz Clortho, use <Ctrl-C> to stop'
    asyncore.loop()
    sys.exit(0)
