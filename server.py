import re
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
import vinzclortho

class RequestHandler(object):
    def __init__(self, handler):
        self.handler = handler    
        self.keymaster = handler.server.keymaster
    
    def input(self):
        return self.handler.rfile.read(int(self.handler.headers['content-length']))

    def output(self, data):
        self.handler.wfile.write(data)

    def respond(self, code):
        self.handler.send_response(code)
        self.handler.end_headers()
        

class LocalStoreHandler(RequestHandler):
    def PUT(self, m):
        value = self.input()
        self.keymaster.local_put(m.group(1), value)
        self.respond(200)

    def DELETE(self, m):
        try:
            self.keymaster.local_delete(m.group(1))
            self.respond(200)
        except KeyError:
            self.respond(404)

    def GET(self, m):
        try:
            value = self.keymaster.local_get(m.group(1))
            self.respond(200)
            self.output(value)
        except KeyError:
            self.respond(404)

class StoreHandler(RequestHandler):
    def PUT(self, m):
        value = self.input()
        self.keymaster.put(m.group(1), value)
        self.respond(200)

    def DELETE(self, m):
        try:
            self.keymaster.delete(m.group(1))
            self.respond(200)
        except KeyError:
            self.respond(404)

    def GET(self, m):
        try:
            value = self.keymaster.get(m.group(1))
            self.respond(200)
            self.output(value)
        except KeyError:
            self.respond(404)

class StatsHandler(RequestHandler):
    def GET(self, m):
        self.respond(200)
        self.output(self.keymaster.stats())

class RequestDispatcher(BaseHTTPRequestHandler):
    patterns = [(re.compile(r"/_localstore/(.*)"), LocalStoreHandler),
                (re.compile(r"/store/(.*)"), StoreHandler),
                (re.compile(r"/stats"), StatsHandler)]

    def _do_request(self):
        for rexp, cls in self.patterns:
            m = rexp.match(self.path)
            if m:
                try:
                    handler = cls(self)
                    command_handler = getattr(handler, self.command)
                    try:
                        command_handler(m)
                    except Exception as e:
                        # Handle all errors as invalid requests
                        self.send_response(400)
                        self.end_headers()
                        # If the user pressed Ctrl-C, we should still quit
                        if isinstance(e, KeyboardInterrupt):
                            raise
                except AttributeError:
                    self.send_response(404)
                    self.end_headers()

    def do_GET(self):
        self._do_request()

    def do_PUT(self):
        self._do_request()

    def do_POST(self):
        self._do_request()

    def do_DELETE(self):
        self._do_request()

class VinzClorthoHTTPServer(ThreadingMixIn, HTTPServer):
    def __init__(self, keymaster):
        self.keymaster = keymaster
        HTTPServer.__init__(self, ('localhost', 8080), RequestDispatcher)

if __name__ == '__main__':
    keymaster = vinzclortho.VinzClortho(True)
    server = VinzClorthoHTTPServer(keymaster)
    print 'Starting Vinz Clortho, use <Ctrl-C> to stop'
    server.serve_forever()
