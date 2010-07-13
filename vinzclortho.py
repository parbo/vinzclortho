import functools
import cPickle as pickle
import optparse
import store
import tangled.core as tc
import tangled.server as ts
from vectorclock import VectorClock

class Partition(tc.Worker):
    def __init__(self, partitionid, persistent):
        tc.Worker.__init__(self)
        if persistent:
            self._store = store.SQLiteStore("vc_store_" + str(partitionid) + ".db")
        else:
            self._store = store.DictStore()
        self.start()

    def get(self, key):
        return self.defer(functools.partial(self._store.get, key))

    def put(self, key, value):
        return self.defer(functools.partial(self._store.put, key, value))
    
    def delete(self, key):
        return self.defer(functools.partial(self._store.delete, key))

class LocalStoreHandler(object):
    def __init__(self, context):
        self.context = context

    def _ok(self, result):
        self.response.callback(ts.Response(200, None, result))

    def _error(self, result):
        self.response.callback(ts.Response(404))

    def do_GET(self, request):
        key = request.groups[0]
        self.data = self.context._partition.get(key)
        self.data.add_callbacks(self._ok, self._error)
        self.response = tc.Deferred()
        return self.response
            
    def do_PUT(self, request):
        key = request.groups[0]
        self.data = self.context._partition.put(key, request.data)
        self.data.add_callbacks(self._ok, self._error)
        self.response = tc.Deferred()
        return self.response
            
    def do_DELETE(self, request):
        key = request.groups[0]
        self.data = self.context._partition.delete(key)
        self.data.add_callbacks(self._ok, self._error)
        self.response = tc.Deferred()
        return self.response

    do_PUSH = do_PUT


class MetaDataHandler(object):
    def __init__(self, context):
        self.context = context

    def _ok(self, result):
        self.response.callback(ts.Response(200, None, result))

    def _error(self, result):
        self.response.callback(ts.Response(404))

    def do_GET(self, request):
        d = tc.Deferred()
        d.callback(ts.Response(200, None, pickle.dumps(self.context._metadata)))
        return d
   

class VinzClortho(object):
    def __init__(self, addr, persistent):
        self.address = addr
        self._partition = Partition(0, persistent)
        vc = VectorClock()
        vc.increment(self.address)
        self._ring = [addr]
        self._metadata = (vc, {"ring": self._ring})
        self._server = ts.AsyncHTTPServer(self.address, self,
                                          [(r"/_localstore/(.*)", LocalStoreHandler),
                                           (r"/_metadata", MetaDataHandler)])
    
        
if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("-a", "--address", dest="address", default="localhost",
                      help="Bind to ADDRESS", metavar="ADDRESS")
    parser.add_option("-p", "--port", dest="port", action="store", type="int", default=8080,
                      help="Bind to PORT", metavar="PORT")
    (options, args) = parser.parse_args()    

    print 'Starting server, use <Ctrl-C> to stop'
    vc = VinzClortho((options.address, options.port), True)
    tc.loop()
