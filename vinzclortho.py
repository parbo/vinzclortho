import functools
import cPickle as pickle
import optparse
import random
import platform
import store
import tangled.core as tc
import tangled.client as client
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
        print "metadata requested"
        return tc.succeed(ts.Response(200, None, pickle.dumps(self.context._metadata)))
   
    def do_PUT(self, request):
        print "metadata submitted"
        self.context.update_meta(pickle.loads(request.data))
        return tc.succeed(ts.Response(200, None, None))

class VinzClortho(object):
    gossip_interval=30.0
    def __init__(self, addr, join, persistent):
        self.reactor = tc.Reactor()
        self.address = split_str_addr(addr)       
        print self.address
        self._partition = Partition(0, persistent)
        self._metadata = None
        if join:         
            self.join = join
            self.join_ring(join)
        else:
            self.create_ring()
        self._server = ts.AsyncHTTPServer(self.address, self,
                                          [(r"/_localstore/(.*)", LocalStoreHandler),
                                           (r"/_metadata", MetaDataHandler)])


    def create_ring(self):
        vc = VectorClock()
        vc.increment(platform.node())
        self._metadata = (vc, {"ring": [self.address]})
        print "created ring", self._metadata
        self.schedule_gossip(0.0)

    def join_ring(self, join):
        d = self.get_gossip(split_str_addr(join))
        d.add_callback(self.ring_joined)
        print d.callbacks, d.called

    def ring_joined(self, result):        
        print "joined ring", self._metadata
        self._metadata[1]["ring"].append(self.address)
        self._metadata[0].increment(platform.node())        
        self.schedule_gossip(0.0)

    def gossip_received(self, response):
        print response.data
        meta = pickle.loads(response.data)
        print "gossip received", meta
        self.update_meta(meta)

    def update_meta(self, meta):
        if self._metadata is None:
            print "no previous metadata, setting", meta
            self._metadata = meta
            return
        vc_new, meta_new = meta
        vc_curr, meta_curr = self._metadata
        if vc_new.descends_from(vc_curr) and vc_new != vc_curr:
            print "received metadata is new", meta
            # Accept new metadata
            self._metadata = meta
        else:
            print "received metadata is old"
            # Reconcile?
            pass
        
    def random_other_node(self):
        other = [n for n in self._metadata[1]["ring"] if n != self.address]
        if len(other) == 0:
            return None
        return other[random.randint(0, len(other)-1)]

    def schedule_gossip(self, timeout=None):
        if timeout is None:
            timeout = self.gossip_interval
        self.reactor.call_later(self.gossip, timeout)

    def gossip_sent(self, result):
        print "gossip sent"
        self.schedule_gossip()

    def get_gossip(self, n=None):
        node = n or self.random_other_node()
        if node is not None:
            host, port = node
            d = client.request("http://%s:%d/_metadata"%(host, port))
            d.add_callback(self.gossip_received)
            return d

    def gossip(self):
        node = self.random_other_node()
        if node is not None:
            print "gossiping with:", node, "data:", self._metadata
            host, port = node
            d = client.request("http://%s:%d/_metadata"%(host, port), command="PUT", data=pickle.dumps(self._metadata))
            d.add_both(self.gossip_sent)

    def run(self):
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
