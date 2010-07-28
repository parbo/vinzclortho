# Copyright (c) 2001-2010 Pär Bohrarper.
# See LICENSE for details.

from BaseHTTPServer import BaseHTTPRequestHandler
import asynchat
import asyncore
import socket
import cStringIO
import re
import sys
import uuid

import logging
log = logging.getLogger("tangled.server")

__version__ = "0.1"

class Request(object):
    """
    The request object that gets passed to a handler in order to respond
    to a request.

    @var client_address: tuple of address, port
    @var method: "GET", "PUT", etc.
    @var path: The uri of the request, /foo/bar
    @var data: The body of the request
    @var groups: This contains the groups (if any) from the regex used when registering the request handler
    """
    def __init__(self, client_address, method, path, headers, data, groups):
        self.client_address = client_address
        self.method = method
        self.path = path
        self.headers = headers
        self.data = data
        self.groups = groups


class Response(object):
    """
    The response object returned by a request handler.
    """
    def __init__(self, code=None, headers=None, data=None):
        """
        @param code: A numeric HTTP status
        @param headers: A dictionary containing header/content pairs
        @param data: A str (or equivalent) containing the data
        """
        self.code = code or 200
        self.headers = headers or {}
        self.data = data or ""


class AsyncHTTPRequestHandler(asynchat.async_chat, BaseHTTPRequestHandler):
    """
    An asynchronous HTTP request handler inspired somewhat by the
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
        self.protocol_version = "HTTP/1.1"
        self.code = None

    def collect_incoming_data(self,data):
        self.incoming.append(data)

    def create_rfile(self):
        # BaseHTTPRequestHandler expects a file like object
        self.rfile = cStringIO.StringIO(''.join(self.incoming))
        self.incoming = []
        self.rfile.seek(0)

    def prepare_request(self):
        """Prepare for reading the request body"""
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
        """
        Called with the request handler's response
 
        @param response: The response to the request
        @type response: L{Response}
        """
        self.send_response(response.code)
        for k, v in response.headers.items():
            self.send_header(k, v)
        if not response.data:
            if "Content-Length" not in response.headers:
                self.send_header("Content-Length", "0")
            self.end_headers()
        else:
            if isinstance(response.data, list):
                boundary = str(uuid.uuid4())
                self.send_header("Content-Type", "multipart/mixed; boundary=%s"%boundary)
                # TODO: might be a good idea to use the 'email' module to create the message..
                multidata = []
                for data in response.data:
                    multidata.append("\r\n--%s\r\n"%boundary)
                    multidata.append("Content-Type: text/plain\r\n\r\n")
                    multidata.append(data)
                multidata.append("\r\n--%s--\r\n"%boundary)
                multidata = "".join(multidata)
                self.send_header("Content-Length", len(multidata))
                self.end_headers()
                self.push(multidata)
            else:
                if "Content-Length" not in response.headers:
                    self.send_header("Content-Length", "%d"%len(response.data))
                self.end_headers()
                self.push(response.data)

        self.close_when_done()

    def handle_request(self):
        """Dispatch the request to a handler"""
        for r, cls in self.urlhandlers:
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
        log.info(format, args)

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
    """
    Cobbled together from various sources, most of them state that they
    copied from the Medusa http server..    
    """
    def __init__(self, address, context, urlhandlers):
        """
        @param address: Tuple of address, port
        @param context: Something that gets passed to the handler's constructor for each request
        @param urlhandlers: list of (regex, handler) tuples. 

        A handler needs to have a constructor that accepts the context object, 
        and a do_* method for each HTTP verb it wants to handle.
        """
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
            log.exception('server accept() threw an exception')
            return
        except TypeError:
            log.exception('server accept() threw EWOULDBLOCK')
            return
        # creates an instance of the handler class to handle the request/response
        # on the incoming connection
        AsyncHTTPRequestHandler(conn, addr, self)

if __name__=="__main__":
    import core
    class MultiPartHandler(object):
        def __init__(self, context):
            pass
        def do_GET(self, request):
            return core.succeed(Response(300, None, ["elephant", "giraffe", "lion"]))
    class NormalHandler(object):
        def __init__(self, context):
            pass
        def do_GET(self, request):
            return core.succeed(Response(200, None, "elephant\ngiraffe\nlion"))
    server = AsyncHTTPServer(("localhost", 8080), None, [("/multipart", MultiPartHandler), ("/normal", NormalHandler)])
    asyncore.loop()

