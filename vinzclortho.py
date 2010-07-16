import functools
import cPickle as pickle
import base64
import bz2
import optparse
import random
import platform
import store
import tangled.core as tc
import tangled.client as client
import tangled.server as ts
import vectorclock

class InvalidContext(Exception):
    pass

class Storage(tc.Worker):
    def __init__(self, reactor, name, persistent):
        tc.Worker.__init__(self, reactor)
        if persistent:
            self._store = store.SQLiteStore("vc_store_" + name + ".db")
        else:
            self._store = store.DictStore()
        self.start()

    def _encode(self, vc, value):
        return bz2.compress(pickle.dumps((vc, value)))

    def _decode(self, blob):
        return pickle.loads(bz2.decompress(blob))

    def _get(self, result):
        vc, value = self._decode(result)        
        if value is None:
            raise KeyError
        return vc, value

    def get(self, key):
        d = self.defer(functools.partial(self._store.get, key))
        d.add_callback(self._get)
        return d

    def _put_new(self, vc, client, key, value, get_result):
        if vc is not None:
            raise InvalidContext
        vc = vectorclock.VectorClock()
        return self.defer(functools.partial(self._store.put, key, self._encode(vc.increment(client), value)))        

    def _put(self, vc, client, key, value, get_result):
        old_vc, old_value = self._decode(get_result)
        # if no vectorclock was provided, just use the stored one
        vc = vc or old_vc
        if vc.descends_from(old_vc):
            return self.defer(functools.partial(self._store.put, key, self._encode(vc.increment(client), value)))
        else:
            # store both
            newvc = vectorclock.merge(old_vc, vc).increment(client)
            return self.defer(functools.partial(self._store.put, key, self._encode(newvc, [old_value, value])))            
    def put(self, vc, client, key, value):
        d = self.defer(functools.partial(self._store.get, key))
        d.add_callbacks(functools.partial(self._put, vc, client, key, value),
                        functools.partial(self._put_new, vc, client, key, value))
        return d
    
    def delete(self, vc, client, key):
        d = self.get(key)
        d.add_callback(functools.partial(self._put, vc, client, key, None))
        return d

class LocalStoreHandler(object):
    def __init__(self, context):
        self.parent = context

    def _vc_to_context(self, vc):
        return base64.b64encode(bz2.compress(pickle.dumps(vc)))

    def _context_to_vc(self, context):
        return pickle.loads(bz2.decompress(base64.b64decode(context)))

    def _ok_get(self, result):
        vc, value = result
        context = self._vc_to_context(vc)        
        # TODO: fix this
        return ts.Response(200, {"X-VinzClortho-Context": context}, str(value))

    def _ok(self, result):
        return ts.Response(200)

    def _error(self, result):
        return ts.Response(404)

    def _extract(self, request):
        """This returns a tuple with the following:

          key
          vectorclock (or None if context not provided)
          client id (or address if not provided)
        """
        try:
            client = request.headers["X-VinzClortho-ClientId"]
        except KeyError:
            # Use the address as client id, if not provided
            client = request.client_address
        try:
            vc = self._context_to_vc(request.headers["X-VinzClortho-Context"])
        except KeyError:
            vc = None
        return request.groups[0], vc, client

    def do_GET(self, request):
        key = request.groups[0]
        d = self.parent._storage.get(key)
        d.add_callbacks(self._ok_get, self._error)
        return d
            
    def do_PUT(self, request):
        key, vc, client = self._extract(request)
        d = self.parent._storage.put(vc, client, key, request.data)
        d.add_callbacks(self._ok, self._error)
        return d
            
    def do_DELETE(self, request):
        key, vc, client = self._extract(request)
        d = self.parent._storage.delete(key, vc, client)
        d.add_callbacks(self._ok, self._error)
        return d

    do_PUSH = do_PUT


class MetaDataHandler(object):
    def __init__(self, context):
        self.context = context

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
        self._storage = Storage(self.reactor, "0", persistent)
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
        vc = vectorclock.VectorClock()
        vc.increment(platform.node())
        self._metadata = (vc, {"ring": [self.address]})
        print "created ring", self._metadata
        self.schedule_gossip(0.0)

    def join_ring(self, join):
        d = self.get_gossip(split_str_addr(join))
        d.add_callback(self.ring_joined)

    def ring_joined(self, result):        
        print "joined ring", self._metadata
        self._metadata[1]["ring"].append(self.address)
        self._metadata[0].increment(platform.node())        
        self.schedule_gossip(0.0)

    def gossip_received(self, response):
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
