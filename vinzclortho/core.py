# Copyright (c) 2001-2010 Pär Bohrarper.
# See LICENSE for details.

import functools
import cPickle as pickle
import base64
import bz2
import optparse
import random
import platform
import collections
import sys
import store
import tangled.core as tc
import tangled.client
import tangled.server as ts
import vectorclock
import consistenthashing as chash

import logging
log = logging.getLogger("vinzclortho.core")

class InvalidContext(Exception):
    pass


class LocalStorage(object):
    """
    A wrapper that makes calls to a L{store.Store} be executed by a worker, and return L{tangled.core.Deferred}'s
    """
    def __init__(self, worker, name, partition, persistent):
        self.worker = worker
        self.name = name
        self.partition = partition
        if persistent:
            self._store = store.BerkeleyDBStore("vc_store_" + name + ".db")
        else:
            self._store = store.DictStore()

    def __str__(self):
        return "LocalStorage(%s)"%self.name

    def get(self, key):
        return self.worker.defer(functools.partial(self._store.get, key))

    def put(self, key, value):
        return self.worker.defer(functools.partial(self._store.put, key, value))

    def multi_put(self, kvlist, resolver):
        return self.worker.defer(functools.partial(self._store.multi_put, kvlist, resolver))

    def delete(self, key):
        return self.worker.defer(functools.partial(self._store.delete, key))

    def _iterate_result(self, first, threshold, callback, result):
        kvlist, iterator = result
        if kvlist:
            callback(kvlist)
            d = self.worker.defer(functools.partial(self._store.iterate, iterator, threshold))
            d.add_callbacks(functools.partial(self._iterate_result, False, threshold, callback), self._iterate_error)
        else:
            if first:
                callback(kvlist)

    def _iterate_error(self, failure):
        failure.raise_exception()

    def _iterator_ready(self, threshold, callback, iterator):
        d = self.worker.defer(functools.partial(self._store.iterate, iterator, threshold))
        d.add_callbacks(functools.partial(self._iterate_result, True, threshold, callback), self._iterate_error)

    def get_all(self, threshold, callback):
        """This will call callback multiple times with a list of key/val tuples.
        The callback will be called whenever threshold bytes is accumulated
        (and also when all key/val tuples have been gathered). If the storage
        is empty, the callback will be called with an empty list.

        This does *not* return a Deferred!
        """
        d = self.worker.defer(self._store.get_iterator)
        d.add_callbacks(functools.partial(self._iterator_ready, threshold, callback), self._iterate_error)


class RemoteStorage(object):
    """A wrapper object that makes remote stores accessible just like local ones"""
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
        if result.status == 200:
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
    """The request handler for requests to /_localstore/somekey"""
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
    """
    The request handler for requests to /store/somekey. Implements the state 
    machines for quorum reads and writes. It also handles read-repair.
    """
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
        return vectorclock.resolve_list_extend([result for replica, result in self.results])

    def _read_repair(self, result):
        if len(self.results) + len(self.failed) == len(self.replicas):
            resolved = self._resolve()
            vc_final, value_final = resolved
            for replica, result in self.results:
                vc, value = result
                if vc_final.descends_from(vc) and not vc.descends_from(vc_final):
                    log.info("Read-repair needed for %s", replica)
                    d = replica.put(self.key, self._encode(vc_final, value_final))
            for replica, result in self.failed:
                log.info("Read-repair of failed node %s", replica)
                d = replica.put(self.key, self._encode(vc_final, value_final))
        return result

    def _read_quorum_acheived(self):
        return len(self.results) >= self.R

    def _write_quorum_acheived(self):
        return len(self.results) >= self.W

    def _all_received(self):
        return len(self.results) + len(self.failed) == len(self.replicas)

    def _respond_error(self):
        if self.response.called:
            return
        self.response.callback(ts.Response(404))

    def _respond_ok(self):
        self.response.callback(ts.Response(200))

    def _respond_get_ok(self):
        if self.response.called:
            return
        resolved = vectorclock.resolve_list([result for replica, result in self.results])
        vc, value = resolved
        context = self._vc_to_context(vc)
        code = 200
        if isinstance(value, list):
            code = 300
        self.response.callback(ts.Response(code, {"X-VinzClortho-Context": context}, value))

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
    """The request handler for requests to /_metadata. Used when gossiping."""
    def __init__(self, context):
        self.context = context

    def do_GET(self, request):
        log.info("Metadata requested by %s", request.client_address)
        return tc.succeed(ts.Response(200, None, bz2.compress(pickle.dumps(self.context._metadata))))

    def do_PUT(self, request):
        log.info("Metadata submitted by %s", request.client_address)
        self.context.update_meta(pickle.loads(bz2.decompress(request.data)))
        return tc.succeed(ts.Response(200, None, None))

class HandoffHandler(object):
    """
    The request handler for requests to /_handoff. This is used to send 
    a partition to its new owner.
    """
    def __init__(self, context):
        self.context = context

    def _put_complete(self, result):
        return ts.Response(200, None, None)

    def do_PUT(self, request):
        kvlist = pickle.loads(bz2.decompress(request.data))
        if not kvlist:
            return tc.succeed(ts.Response(200, None, None))
        d = self.context.local_multi_put(kvlist)
        d.add_both(self._put_complete)
        return d

class AdminHandler(object):
    """
    The request handler for requests to /admin. Currently, only /admin/claim is available.
    The number of partitions claimed by a node can be read/written using this.
    """
    def __init__(self, context):
        self.context = context

    def do_GET(self, request):
        service = request.groups[0]
        if service == "claim":
            return tc.succeed(ts.Response(200, None, str(self.context.get_claim())))
        return tc.succeed(ts.Response(404))

    def do_PUT(self, request):
        service = request.groups[0]
        if service == "claim":
            try:
                claim = int(request.data)
                self.context.update_claim(claim)
                return tc.succeed(ts.Response(200))
            except ValueError:
                return tc.succeed(ts.Response(400))
        elif service == "balance":
            self.context.balance()
            return tc.succeed(ts.Response(200))
        return tc.succeed(ts.Response(404))

    do_PUSH = do_PUT

class VinzClortho(object):
    """
    The main object that contains the HTTP server and handles gossiping
    of the consistent hash ring metadata.
    """
    gossip_interval=30.0
    N=3
    num_partitions=1024
    worker_pool_size=10
    def __init__(self, addr, join, claim, partitions, logfile, persistent):
        # Setup logging
        logfile = logfile or "vc_log_" + addr + ".log"
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%m-%d %H:%M',
                            filename=logfile,
                            filemode='a')
        # define a Handler which writes WARNING messages or higher to the sys.stderr
        console = logging.StreamHandler()
        console.setLevel(logging.WARNING)
        formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)

        log.info("Starting VinzClortho")

        self.reactor = tc.Reactor()
        self.workers = [tc.Worker(self.reactor, True) for i in range(self.worker_pool_size)]
        self.address = split_str_addr(addr)
        self.host, self.port = self.address
        self.num_partitions = partitions or self.num_partitions
        self.persistent = persistent
        self._vcid = self.address
        self._storage = {}
        self._pending_shutdown_storage = {}
        self._metadata = None
        self._node = chash.Node(self.host, self.port)
        self._claim = claim
        self.create_ring(join)
        self._server = ts.AsyncHTTPServer(self.address, self,
                                          [(r"/store/(.*)", StoreHandler),
                                           (r"/_localstore/(.*)", LocalStoreHandler),
                                           (r"/_handoff", HandoffHandler),
                                           (r"/_metadata", MetaDataHandler),
                                           (r"/admin/(.*)", AdminHandler)])
        self.reactor.call_later(self.check_shutdown, 30.0)

    @property
    def ring(self):
        return self._metadata[1]["ring"]

    def _get_worker(self, num):
        # The idea is to get the same worker for a partition, to avoid 
        # threading issues
        return self.workers[num % len(self.workers)]

    def _get_replica(self, node, key):
        if node.host == self.host and node.port == self.port:
            return self.get_storage(key)
        else:
            return RemoteStorage((node.host, node.port))

    def check_shutdown(self):
        if not self._storage and not self._pending_shutdown_storage:
            for w in self.workers:
                w.stop()
                w.join()
            # fugly way, but it works
            sys.exit(0)
        self.reactor.call_later(self.check_shutdown, 5.0)

    def get_claim(self):
        return len(self._node.claim)

    def balance(self):
        self.ring.update_claim()
        self._metadata[0].increment(self._vcid)
        self.reactor.call_later(self.update_storage, 0.0)
        self.reactor.call_later(self.check_handoff, 0.0)
        self.schedule_gossip(0.0)

    def update_claim(self, claim):
        force = (claim == 0)
        self.ring.update_node(self._node, claim, force)
        self.balance()

    def get_replicas(self, key):
        preferred, fallbacks = self.ring.preferred(key)
        return [self._get_replica(n, key) for n in preferred]

    def get_storage(self, key):
        p = self.ring.key_to_partition(key)
        return self._storage.setdefault(p, LocalStorage(self._get_worker(p), "%d@%s:%d"%(p, self.host, self.port), p, self.persistent))

    def local_get(self, key):
        s = self.get_storage(key)
        return s.get(key)

    def local_put(self, key, value):
        s = self.get_storage(key)
        return s.put(key, value)

    def local_multi_put(self, kvlist):
        def resolve(a, b):
            return vectorclock.resolve_list_extend([a, b])
        s = self.get_storage(kvlist[0][0])
        return s.multi_put(kvlist, resolve)

    def local_delete(self, key):
        s = self.get_storage(key)
        return s.delete(key)

    def create_ring(self, join):
        if join:
            self.get_gossip(split_str_addr(join))
        else:
            vc = vectorclock.VectorClock()
            vc.increment(self._vcid)
            self._metadata = (vc, {"ring": chash.Ring(self.num_partitions, self._node, self.N)})
            self.reactor.call_later(self.update_storage, 0.0)
            self.schedule_gossip()

    def gossip_received(self, address, response):
        meta = pickle.loads(bz2.decompress(response.data))
        log.info("Gossip received from %s", address)
        if self.update_meta(meta):
            log.info("Update gossip @ %s", address)
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
                    log.debug("Received metadata is new %s", meta)
                    # Accept new metadata
                    self._metadata = meta
                    updated = True
                else:
                    log.debug("Received metadata is the same")
            else:
                log.debug("Received metadata is old")
                old = True
                # Reconcile?

        # Add myself if needed
        # this just compares host and port, not the claim..
        if self._node not in self.ring.nodes:
            self.ring.add_node(self._node, self._claim)
            self._metadata[0].increment(self._vcid)
            updated = True
            old = True

        if updated:
            # Grab the node since it might have been updated
            self._node = self.ring.get_node(self._node.name)
            self.reactor.call_later(self.update_storage, 0.0)
            self.reactor.call_later(self.check_handoff, 0.0)
        return old

    def random_other_node_address(self):
        other = [n for n in self.ring.nodes if n != self._node]
        if len(other) == 0:
            return None
        n = other[random.randint(0, len(other)-1)]
        return n.host, n.port

    def schedule_gossip(self, timeout=None):
        log.debug("Gossip scheduled, %s", timeout)
        if timeout is None:
            timeout = self.gossip_interval
        self.reactor.call_later(self.get_gossip, timeout)

    def gossip_sent(self, result):
        log.debug("Gossip sent")
        self.schedule_gossip()

    def gossip_error(self, result):
        log.error("Gossip error: %s", result)
        self.schedule_gossip()

    def get_gossip(self, a=None):
        address = a or self.random_other_node_address()
        if address is not None:
            log.debug("Gossip with", address)
            d = tangled.client.request("http://%s:%d/_metadata"%address)
            d.add_callbacks(functools.partial(self.gossip_received, address), self.gossip_error)
            return d

    def update_storage(self):
        """Creates storages (if necessary) for all claimed partitions"""
        for p in self._node.claim:
            if p not in self._storage:
                self._storage[p] = LocalStorage(self._get_worker(p), "%d@%s:%d"%(p, self.host, self.port), p, self.persistent)

    def _partial_handoff(self, node, partition, kvlist):
        if not kvlist:
            # TODO: remove the db file etc
            log.debug("Shutdown partition %s", partition)
            del self._pending_shutdown_storage[partition]
        else:
            url = "http://%s:%d/_handoff"%(node.host, node.port)
            d = tangled.client.request(url, command="PUT", data=bz2.compress(pickle.dumps(kvlist)))
            log.info("Handoff %d items from %s to %s", len(kvlist), partition, node)

    def do_handoff(self, node, partitions, result):
        log.debug("Handing off %s", partitions)
        for p in partitions:
            s = self._storage[p]
            self._pending_shutdown_storage[p] = s
            del self._storage[p]
            # Send the partitions in 1MB chunks
            s.get_all(1048576, functools.partial(self._partial_handoff, node, p))
        return result

    def _handoff_error(self, failure):
        failure.raise_exception()

    def check_handoff(self):
        """Checks if any partitions that aren't claimed or replicated can be handed off"""
        to_handoff = set(self._storage.keys()) - set(self._node.claim) - self.ring.replicated(self._node)
        handoff_per_node = collections.defaultdict(list)
        for p in to_handoff:
            n = self.ring.partition_to_node(p)
            handoff_per_node[n].append(p)
        for n, plist in handoff_per_node.items():
            # request the metadata to see if it's alive
            d = tangled.client.request("http://%s:%d/_metadata"%(n.host, n.port))
            d.add_callbacks(functools.partial(self.do_handoff, n, plist), self._handoff_error)

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

def main():
    parser = optparse.OptionParser()
    parser.add_option("-a", "--address", dest="address", default="localhost:8080",
                      help="Bind to ADDRESS", metavar="ADDRESS")
    parser.add_option("-j", "--join", dest="join",
                      help="Bind to ADDRESS", metavar="ADDRESS")
    parser.add_option("-c", "--claim", dest="claim",
                      help="Number of partitions to claim")
    parser.add_option("-p", "--partitions", dest="partitions",
                      help="Number of partitions in the hash ring")
    parser.add_option("-l", "--logfile", dest="logfile", metavar="FILE"
                      help="Use FILE as logfile")
    (options, args) = parser.parse_args()

    vc = VinzClortho(options.address, options.join, options.claim, options.partition, options.logfile, True)
    vc.run()

if __name__ == '__main__':
    main()
