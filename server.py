import re
from BaseHTTPServer import BaseHTTPRequestHandler
import asynchat
import asyncore
import socket
import cStringIO
import vinzclortho

__version__ = "0.1"

class NOT_DONE_YET(object):
    pass

class AsyncChatDeferred(asynchat.async_chat):
    """A subclass of async_chat to allow for producers that
    will have data eventually, but not just yet"""

    # refill the outgoing buffer by calling the more() method
    # of the first producer in the queue
    def refill_buffer (self):
        while 1:
            if len(self.producer_fifo):
                p = self.producer_fifo.first()
                # a 'None' in the producer fifo is a sentinel,
                # telling us to close the channel.
                if p is None:
                    if not self.ac_out_buffer:
                        self.producer_fifo.pop()
                        self.close()
                    return
                elif isinstance(p, str):
                    self.producer_fifo.pop()
                    self.ac_out_buffer = self.ac_out_buffer + p
                    return
                data = p.more()
                if data is NOT_DONE_YET:
                    return
                if data:
                    self.ac_out_buffer = self.ac_out_buffer + data
                    return
                else:
                    self.producer_fifo.pop()
            else:
                return

class AsyncHTTPRequestHandler(AsyncChatDeferred, BaseHTTPRequestHandler):
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
        asynchat.async_chat.__init__(self,conn)
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
        # set terminator to length (will read bytesToRead bytes)
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
        finally:
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

    def do_GET(self):
        try:
            key = self.path.split("/")[-1]
            value = self.keymaster.local_get(key)
            self.send_response(200)
            self.end_headers()
            self.push(value)
        except KeyError:
            self.send_response(404)
            self.end_headers()            

    def do_PUT(self):
        key = self.path.split("/")[-1]
        self.keymaster.local_put(key, self.rfile.read())
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        self.do_PUT()

    def do_DELETE(self):
        try:
            key = self.path.split("/")[-1]
            print key
            self.keymaster.local_delete(key)
            self.send_response(200)
            self.end_headers()
            self.push(value)
        except KeyError:
            self.send_response(404)
            self.end_headers()            

class AsyncHTTPServer(asyncore.dispatcher):
    """Cobbled together from various sources, most of them state that they 
    copied from the Medusa http server.. 
    """
    def __init__(self, keymaster):
        self.keymaster = keymaster
        self.handler = AsyncHTTPRequestHandler
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(('10.0.0.100', 8080))
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
    keymaster = vinzclortho.VinzClortho(True)
    server = AsyncHTTPServer(keymaster)
    print 'Starting Vinz Clortho, use <Ctrl-C> to stop'
    asyncore.loop()
