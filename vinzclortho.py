import functools
import cPickle as pickle
import base64
import bz2
import optparse
import random
import platform
import store
import tangled.core as tc
import tangled.client
import tangled.server as ts
import vectorclock
import consistenthashing as chash


class InvalidContext(Exception):
    pass


class LocalStorage(tc.Worker):
    def __init__(self, reactor, name, partition, persistent):
        tc.Worker.__init__(self, reactor)
        self.name = name
        self.partition = partition
        if persistent:
            self._store = store.BerkeleyDBStore("vc_store_" + name + ".db")
        else:
            self._store = store.DictStore()
        self.start()

    def __str__(self):
        return "LocalStorage(%s)"%self.name

    def get(self, key):
        return self.defer(functools.partial(self._store.get, key))

    def put(self, key, value):
        return self.defer(functools.partial(self._store.put, key, value))
    
    def delete(self, key):
        return self.defer(functools.partial(self._store.delete, key))


class RemoteStorage(object):
    def __init__(self, address):
        self.address = address

    def __str__(self):
        return "RemoteStorage((%s, %d))"%self.address

    def _ok_get(self, result):
        if result.status == 200:
            return result.data
        else:
            raise KeyError

    def _ok(self, result):
        if result.code == 200:
            return
        else:
            raise KeyError
        

    def get(self, key):
        host, port = self.address
        d = tangled.client.request("http://%s:%d/_localstore/%s"%(host, port, key))
        d.add_callback(self._ok_get)
        return d
            
    def put(self, key, value):
        host, port = self.address
        d = tangled.client.request("http://%s:%d/_localstore/%s"%(host, port, key), "PUT", value)
        d.add_callback(self._ok)
        return d        
            
    def delete(self, key):
        host, port = self.address
        d = tangled.client.request("http://%s:%d/_localstore/%s"%(host, port, key), "DELETE")
        d.add_callback(self._ok)
        return d


class LocalStoreHandler(object):
    def __init__(self, context):
        self.parent = context

    def _ok_get(self, result):
        return ts.Response(200, None, result)

    def _ok(self, result):
        return ts.Response(200)

    def _error(self, result):
        return ts.Response(404)

    def do_GET(self, request):
        key = request.groups[0]
        d = self.parent.local_get(key)
        d.add_callbacks(self._ok_get, self._error)
        return d
            
    def do_PUT(self, request):
        key = request.groups[0]
        d = self.parent.local_put(key, request.data)
        d.add_callbacks(self._ok, self._error)
        return d
            
    def do_DELETE(self, request):
        key = request.groups[0]
        d = self.parent.local_delete(key)
        d.add_callbacks(self._ok, self._error)
        return d

    do_PUSH = do_PUT


class StoreHandler(object):
    W = 2
    R = 2
    def __init__(self, context):
        self.parent = context
        self.results = []
        self.failed = []

    def _encode(self, vc, value):
        return bz2.compress(pickle.dumps((vc, value)))

    def _decode(self, blob):
        return pickle.loads(bz2.decompress(blob))

    def _vc_to_context(self, vc):
        return base64.b64encode(bz2.compress(pickle.dumps(vc)))

    def _context_to_vc(self, context):
        return pickle.loads(bz2.decompress(base64.b64decode(context)))

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

    def _resolve(self):
        """Resolves the list of results to a unified result (which may be a list of concurrent versions)"""
        def joiner(a, b):
            """This way of joining concurrent versions makes it possible 
            to store concurrent versions as lists, while still being able 
            to return a single list for a request"""
            if not isinstance(a, list):
                a = [a]
            if not isinstance(b, list):
                b = [b]
            return a + b              
        return vectorclock.resolve_list([result for replica, result in self.results], joiner)        

    def _read_repair(self, result):
        if len(self.results) + len(self.failed) == len(self.replicas):
            resolved = self._resolve()
            vc_final, value_final = resolved
            for replica, result in self.results:
                vc, value = result
                if vc_final.descends_from(vc) and not vc.descends_from(vc_final):
                    print "read-repair needed for", replica
                    d = replica.put(self.key, self._encode(vc_final, value_final))
            for replica, result in self.failed:
                print "read-repair of failed node", replica
                d = replica.put(self.key, self._encode(vc_final, value_final))
        return result

    def _read_quorum_acheived(self):
        print "Read quorum?", len(self.results), self.R
        return len(self.results) >= self.R

    def _write_quorum_acheived(self):
        return len(self.results) >= self.W

    def _all_received(self):
        return len(self.results) + len(self.failed) == len(self.replicas)

    def _respond_error(self):
        if self.response.called:
            return
        # TODO: find suitable error code
        self.response.callback(ts.Response(404))

    def _respond_ok(self):
        self.response.callback(ts.Response(200))

    def _respond_get_ok(self):
        if self.response.called:
            return        
        resolved = vectorclock.resolve_list([result for replica, result in self.results])
        vc, value = resolved
        context = self._vc_to_context(vc)
        # TODO: make sure the replica object returns str
        self.response.callback(ts.Response(200, {"X-VinzClortho-Context": context}, str(value)))

    def _get_ok(self, replica, result):
        result = self._decode(result)
        # This handles deleted keys (TODO: this means concurrent deletes are lost, is this ok?)
        if result is None:
            return self._fail(replica, result)
        # There was an actual value, handle it
        self.results.append((replica, result))
        if self._read_quorum_acheived():
            self._respond_get_ok()    
        elif self._all_received():
            self._respond_error()
        return result

    def _fail(self, replica, result):
        self.failed.append((replica, result))
        if self._all_received():
            self._respond_error()
        return result

    def do_GET(self, request):
        self.response = tc.Deferred()
        self.key = request.groups[0]
        self.replicas = self.parent.get_replicas(self.key)
        print "Replicas:", self.replicas
        for r in self.replicas:
            d = r.get(self.key)
            d.add_callbacks(functools.partial(self._get_ok, r), 
                            functools.partial(self._fail, r))
            d.add_both(self._read_repair)
        return self.response
            
    def _ok(self, replica, result):
        self.results.append((replica, result))
        if self._write_quorum_acheived():
            self._respond_ok()
        elif self._all_received():
            self._respond_error()

    def do_PUT(self, request):
        self.response = tc.Deferred()
        key, vc, client = self._extract(request)
        self.replicas = self.parent.get_replicas(key)
        vc = vc or vectorclock.VectorClock()
        vc.increment(client)
        value = self._encode(vc, request.data)
        for r in self.replicas:
            d = r.put(key, value)
            d.add_callbacks(functools.partial(self._ok, r), 
                            functools.partial(self._fail, r))
        return self.response
            
    def do_DELETE(self, request):
        self.response = tc.Deferred()
        key, vc, client = self._extract(request)
        self.replicas = self.parent.get_replicas(key)
        vc = vc or vectorclock.VectorClock()
        vc.increment(client)
        value = self._encode(vc, None)
        for r in self.replicas:
            # delete is handled as a put of None
            d = r.put(key, value)
            d.add_callbacks(functools.partial(self._ok, r), 
                            functools.partial(self._fail, r))
        return self.response

    do_PUSH = do_PUT

class MetaDataHandler(object):
    def __init__(self, context):
        self.context = context

    def do_GET(self, request):
        print "metadata requested"
        return tc.succeed(ts.Response(200, None, bz2.compress(pickle.dumps(self.context._metadata))))
   
    def do_PUT(self, request):
        print "metadata submitted"
        self.context.update_meta(pickle.loads(bz2.decompress(request.data)))
        return tc.succeed(ts.Response(200, None, None))

class VinzClortho(object):
    gossip_interval=30.0
    N=3
    num_partitions=1024
    def __init__(self, addr, join, persistent):
        self.reactor = tc.Reactor()
        self.address = split_str_addr(addr)
        self.host, self.port = self.address
        self.persistent = persistent
        self._vcid = self.address
        self._storage = {}
        self._pending_shutdown_storage = []
        self._metadata = None        
        self._node = chash.Node(self.host, self.port)
        self.create_ring(join)
        self._server = ts.AsyncHTTPServer(self.address, self,
                                          [(r"/store/(.*)", StoreHandler),
                                           (r"/_localstore/(.*)", LocalStoreHandler),
                                           (r"/_metadata", MetaDataHandler)])

    def _get_replica(self, node, key):
        if node.host == self.host and node.port == self.port:
            return self.get_storage(key)
        else:
            return RemoteStorage((node.host, node.port))

    def get_replicas(self, key):
        ring = self._metadata[1]["ring"]
        preferred = ring.preferred(key, self.N)
        return [self._get_replica(n, key) for n in preferred]

    def get_storage(self, key):
        ring = self._metadata[1]["ring"]
        p = ring.key_to_partition(key)
        return self._storage.setdefault(p, LocalStorage(self.reactor, "%s:%d"%(self.host, self.port), p, self.persistent))

    def local_get(self, key):
        s = self.get_storage(key)
        return s.get(key)

    def local_put(self, key, value):
        s = self.get_storage(key)
        return s.put(key, value)

    def local_delete(self, key):
        s = self.get_storage(key)
        return s.delete(key)

    def create_ring(self, join):
        if join:
            d = self.get_gossip(split_str_addr(join))
            d.add_callback(self.ring_joined)
        else:
            vc = vectorclock.VectorClock()
            vc.increment(self._vcid)
            self._metadata = (vc, {"ring": chash.Ring(self.num_partitions, self._node)})
            self.schedule_gossip()

    def ring_joined(self, result):        
        self.schedule_gossip(0.0)

    def gossip_received(self, address, response):
        meta = pickle.loads(bz2.decompress(response.data))
        if self.update_meta(meta):
            print "update gossip", address
            url = "http://%s:%d/_metadata"%address
            d = tangled.client.request(url, command="PUT", data=bz2.compress(pickle.dumps(self._metadata)))
            d.add_both(self.gossip_sent)
        else:
            self.schedule_gossip()

    def update_meta(self, meta):
        old = False
        updated = False 

        # Update metadata as needed
        if self._metadata is None:
            self._metadata = meta
            updated = True
        else:
            vc_new, meta_new = meta
            vc_curr, meta_curr = self._metadata
            if vc_new.descends_from(vc_curr):
                if vc_new != vc_curr:
                    print "received metadata is new", meta
                    # Accept new metadata
                    self._metadata = meta
                    updated = True
                else:
                    print "received metadata is the same"
            else:
                print "received metadata is old"
                old = True
                # Reconcile?

        # Add myself if needed
        ring = self._metadata[1]["ring"]
        if self._node not in ring.nodes:
            ring.add_node(self._node)
            self._metadata[0].increment(self._vcid)
            updated = True
            old = True

        if updated:
            self.reactor.call_later(self.check_handoff, 0.0)            

        return old
        
    def random_other_node_address(self):
        ring = self._metadata[1]["ring"]
        other = [n for n in ring.nodes if n != self._node]
        if len(other) == 0:
            return None
        n = other[random.randint(0, len(other)-1)]
        return n.host, n.port

    def schedule_gossip(self, timeout=None):
        if timeout is None:
            timeout = self.gossip_interval
        self.reactor.call_later(self.get_gossip, timeout)

    def gossip_sent(self, result):
        print "gossip sent"
        self.schedule_gossip()

    def gossip_error(self, result):
        print "gossip error"
        self.schedule_gossip()

    def get_gossip(self, a=None):
        address = a or self.random_other_node_address()
        if address is not None:
            print "gossip with", address
            d = tangled.client.request("http://%s:%d/_metadata"%address)
            d.add_callbacks(functools.partial(self.gossip_received, address), self.gossip_error)
            return d

    def do_handoff(self, node, storage, result):
        print "TODO: handoff partition", storage.partition, "to node", node
        return result

    def check_handoff(self):
        """Checks if any partitions that aren't claimed or replicated can be handed off"""
        ring = self._metadata[1]["ring"]
        # TODO: this is wrong, this hands off replicated data...
        unclaimed_and_stored = set(self._storage.keys()) - set(self._node.claim) 
        for p in unclaimed_and_stored:
            n = ring.partition_to_node(p)
            # request the metadata to see if it's alive
            d = tangled.client.request("http://%s:%d/_metadata"%(n.host, n.port))
            d.add_callback(functools.partial(self.do_handoff, n, self._storage[p]))

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
