from BaseHTTPServer import BaseHTTPRequestHandler
import asynchat
import asyncore
import socket
import cStringIO
import threading
import functools
import urlparse
import optparse

__version__ = "0.1"

def raise_error(e):
    if e is not None:
        raise e

class Deferred(object):
    def __init__(self):
        self._backs = []

class Request(object):
    def __init__(self, method, path, headers, data, groups):
        self.method = method
        self.path = path
        self.headers = headers
        self.data = data
        self.groups = groups


class Response(object):
    def __init__(self, code, headers, data):
        self.code
        self.headers = headers
        self.data = data


class AsyncHTTPRequestHandler(asynchat.async_chat, BaseHTTPRequestHandler):
    """An asynchronous HTTP request handler inspired somewhat by the 
    http://code.activestate.com/recipes/440665-asynchronous-http-server/
    recipe.
    """

    server_version = "Tangled/" + __version__
    methods = ["HEAD", "GET", "POST", "PUT", "DELETE", "TRACE", "OPTIONS", "CONNECT", "PATCH"]

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
    
    def handle_junk(self):
        pass

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
        for r, h in self.urlhandlers:
            m = re.match(self.path)
            if m is not None:
                try:
                    handler = getattr(h, "do_" + self.command)
                    handler(Request(self.commaand, self.path, self.headers, self.rfile.read(), m.groups()))
                    return                            
                except AttributeError:
                    # Method not supported
                    self.send_error(405)
                    self.send_header("Allow", [method for method in self.methods if hasattr(h, "do_" + method)])
                    self.end_headers()        
                    self.close_when_done()
                return
        # No match found, send 404
        self.send_error(404)
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

    def request_handled(self, response):
        self.send_response(response.code)
        for k, v in response.headers.items():
            self.send_header(k, v)
        self.end_headers()
        if self.data:
            self.push(data)
        self.close_when_done()

    def request_error(self, error):
        self.send_error(error)
        self.close_when_done()

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
    def __init__(self, address, urlhandlers):
        self.trigger = Trigger()
        self.handler = AsyncHTTPRequestHandler
        self.urlhandlers = [(re.compile(r), h) for r, h in urlhandlers]
        self.address = address
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind()
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

