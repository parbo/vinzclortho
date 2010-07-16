from BaseHTTPServer import BaseHTTPRequestHandler
import asynchat
import asyncore
import socket
import cStringIO
import re
import sys

__version__ = "0.1"

class Request(object):
    def __init__(self, client_address, method, path, headers, data, groups):
        self.client_address = client_address
        self.method = method
        self.path = path
        self.headers = headers
        self.data = data
        self.groups = groups


class Response(object):
    def __init__(self, code=None, headers=None, data=None):
        self.code = code or 200
        self.headers = headers or {}
        self.data = data or ""


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
        self.urlhandlers = self.server.urlhandlers
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

    def finish_request(self, response):
        self.send_response(response.code)
        for k, v in response.headers.items():
            self.send_header(k, v)
        self.end_headers()
        if response.data:
            self.push(response.data)
        self.close_when_done()

    def handle_request(self):
        """Dispatch the request to a handler"""
        for r, cls in self.urlhandlers:
            print "requested:", self.path
            m = re.match(r, self.path)
            if m is not None:
                try:
                    h = cls(self.server.context)
                    handler = getattr(h, "do_" + self.command)
                    d = handler(Request(self.client_address, 
                                        self.command, 
                                        self.path, 
                                        self.headers, 
                                        self.rfile.read(), 
                                        m.groups()))
                    d.add_callback(self.finish_request)
                except AttributeError:
                    raise
                    # Method not supported
                    self.send_header("Allow", ", ".join([method for method in self.methods if hasattr(h, "do_" + method)]))
                    self.send_error(405)
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


class AsyncHTTPServer(asyncore.dispatcher):
    """Cobbled together from various sources, most of them state that they 
    copied from the Medusa http server.. 
    """
    def __init__(self, address, context, urlhandlers):
        self.context = context
        self.urlhandlers = [(re.compile(r), h) for r, h in urlhandlers]
        self.address = address
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(self.address)
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
        AsyncHTTPRequestHandler(conn, addr, self)
