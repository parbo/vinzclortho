import re
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
from store import DictStore, SQLiteStore

class Handler(BaseHTTPRequestHandler):
    storerexp = re.compile(r"/_localstore/(.*)")

    def do_GET(self):
        m = self.storerexp.match(self.path)
        if m:
            try:
                val = self.server.store.get(m.group(1))                
                self.send_response(200)
                self.end_headers()
                self.wfile.write(val)
            except KeyError:
                self.send_response(404)
                self.end_headers()                
        else:
            self.send_response(400)
            self.end_headers()

    def do_PUT(self):
        m = self.storerexp.match(self.path)
        if m:
            value = self.rfile.read(int(self.headers['content-length']))
            self.server.store.put(m.group(1), value)
            self.send_response(200)
            self.end_headers()
        else:
            self.send_response(400)
            self.end_headers()

    def do_DELETE(self):
        m = self.storerexp.match(self.path)
        if m:
            try:
                self.server.store.delete(m.group(1))
                self.send_response(200)
                self.end_headers()
            except KeyError:
                self.send_response(404)
                self.end_headers()                
        else:
            self.send_response(400)
            self.end_headers()

class SDKVHTTPServer(ThreadingMixIn, HTTPServer):
    def __init__(self, store):
        HTTPServer.__init__(self, ('localhost', 8080), Handler)
        self.store = store

if __name__ == '__main__':
    store = SQLiteStore("foo.db")
    server = SDKVHTTPServer(store)
    print 'Starting server, use <Ctrl-C> to stop'
    server.serve_forever()
