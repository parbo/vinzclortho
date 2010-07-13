import functools
import cPickle as pickle
import optparse
import random
import platform
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
    gossip_interval=30.0
    def __init__(self, addr, join, persistent):
        self.reactor = tc.Reactor()
        self.address = split_str_addr(addr)       
        print self.address
        self._partition = Partition(0, persistent)
        vc = VectorClock()
        vc.increment(platform.node())
        self._ring = [self.address]
        if join:
            self._ring.append(split_str_addr(join))
        self._metadata = (vc, {"ring": self._ring})
        self._server = ts.AsyncHTTPServer(self.address, self,
                                          [(r"/_localstore/(.*)", LocalStoreHandler),
                                           (r"/_metadata", MetaDataHandler)])

    def random_other_node(self):
        other = [n for n in self._ring if n != self.address]
        if len(other) == 0:
            return None
        return other[random.randint(0, len(other)-1)]

    def schedule_gossip(self):
        self.reactor.call_later(self.gossip, self.gossip_interval)

    def gossip(self):
        print "gossiping"
        node = self.random_other_node()
        if node is not None:
            pass
        self.schedule_gossip()

    def run(self):
        self.schedule_gossip()
        self.reactor.loop()
    

def split_str_addr(str_addr):
    addr = str_addr.split(":")
    host = addr[0]
    try:
        port = int(addr[1])
    except IndexError:
        port = 80
    return host, port
    
    
if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("-a", "--address", dest="address", default="localhost:8080",
                      help="Bind to ADDRESS", metavar="ADDRESS")
    parser.add_option("-j", "--join", dest="join",
                      help="Bind to ADDRESS", metavar="ADDRESS")
    (options, args) = parser.parse_args()    

    print 'Starting server, use <Ctrl-C> to stop'
    vc = VinzClortho(options.address, options.join, True)
    vc.run()
