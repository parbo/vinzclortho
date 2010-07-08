import store
import functools
import executor

class Partition(object):
    def __init__(self, partitionid, persistent):
        if persistent:
            self._store = store.SQLiteStore("vc_store_" + str(partitionid) + ".db")
        else:
            self._store = store.DictStore()
        self._executor = executor.Executor()

    def get(self, key, oncomplete):
        self._executor.defer(functools.partial(self._store.get, key), oncomplete)

    def put(self, key, value, oncomplete):
        self._executor.defer(functools.partial(self._store.put, key, value), oncomplete)
    
    def delete(self, key, oncomplete):
        self._executor.defer(functools.partial(self._store.delete, key), oncomplete)

class VinzClortho(object):
    def __init__(self, addr, persistent):
        self.address = addr
        self._partition = Partition(0, persistent)
        self._members = [addr]

    def do_GET(self, path, rfile, oncomplete):
        try:
            p = path.split("/")
            if p[1] == "_localstore":
                assert(len(p) == 3)
                key = p[2]
                self._partition.get(key, oncomplete)
            elif p[1] == "_members":
                assert(len(p) == 2)
                oncomplete((str(self._members), None))
            else:
                oncomplete((None, KeyError(path)))
        except Exception as e:
            oncomplete((None, e))
            

    def do_PUT(self, path, rfile, oncomplete):
        try:
            p = path.split("/")
            if p[1] == "_localstore":
                assert(len(p) == 3)
                key = p[2]
                self._partition.put(key, rfile.read(), oncomplete)
            elif p[1] == "_members":
                assert(len(p) == 2)
                err = None
                try:
                    # this is stupid, I know!
                    self._members = eval(rfile.read())
                except Exception as e:
                    err = e
                oncomplete((None, err))
            else:
                oncomplete((None, KeyError(path)))
        except Exception as e:
            oncomplete((None, e))


    def do_DELETE(self, path, rfile, oncomplete):
        try:
            p = path.split("/")
            if p[1] == "_localstore":
                assert(len(p) == 3)
                key = p[2]
                self._partition.delete(key, oncomplete)
            else:
                oncomplete((None, KeyError(path)))
        except Exception as e:
            oncomplete((None, e))

    do_PUSH = do_PUT
    
        
