import store


class VinzClortho(object):
    def __init__(self, persistent):
        if persistent:
            self._localstore = store.SQLiteStore("keymaster.db")
        else:
            self._localstore = store.DictStore()
            
    def local_get(self, key):
        return self._localstore.get(key)

    def local_put(self, key, value):
        return self._localstore.put(key, value)
    
    def local_delete(self, key):
        return self._localstore.delete(key)
        
