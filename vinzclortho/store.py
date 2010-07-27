import sqlite3
import bsddb
import unittest

class Store(object):
    """Base class for stores."""
    def put(self, key, value):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def delete(self, key):
        raise NotImplementedError

    def get_iterator(self):
        raise NotImplementedError

    def iterate(self, iterator, threshold):
        raise NotImplementedError

    def multi_put(self, kvlist, resolver):
        for k, v in kvlist:
            try:
                v_curr = self.get(k)
                v = resolver(v, v_curr)
            except KeyError:
                # This store doesn't have the key, no need to resolve
                pass
            # TODO: probably should check if the value was changed...
            self.put(k, v)
    

class DictStore(Store):
    """Basic in-memory store."""
    def __init__(self):
        self._store = {}

    def put(self, key, value):
        self._store[key] = value

    def get(self, key):
        return self._store[key]

    def delete(self, key):
        del self._store[key]

    def get_iterator(self):
        return self._store.iteritems()

    def iterate(self, iterator, threshold):
        tot = 0
        ret = []
        try:            
            while tot < threshold:
                k, v = iterator.next()
                tot = tot + len(k) + len(v)
                ret.append((k, v))
            return ret, iterator
        except StopIteration:
            return ret, iterator

class BerkeleyDBStore(Store):
    """Store using BerkeleyDB, specifically the B-Tree version"""
    def __init__(self, filename):
        self._store = bsddb.btopen(filename)

    def put(self, key, value):
        self._store[key] = value
        self._store.sync()

    def get(self, key):
        return self._store[key]

    def delete(self, key):
        del self._store[key]
        self._store.sync()

    def get_iterator(self):
        try:
            k, v = self._store.first()
            return k
        except bsddb.error as e:
            return None

    def iterate(self, iterator, threshold):
        if iterator is None:
            return [], None
        iterator, v = self._store.set_location(iterator)
        tot = 0
        ret = [(iterator, v)]        
        try:            
            while tot < threshold:
                iterator, v = self._store.next()
                tot = tot + len(iterator) + len(v)
                ret.append((iterator, v))
            return ret, iterator
        except bsddb.error as e:
            return ret, None
        

class SQLiteStore(Store):
    """Store that uses SQLite for storage."""
    def __init__(self, filename):
        self._db = filename
        self.conn = sqlite3.connect(self._db)
        c = self.conn.cursor()
        # Create table
        c.execute("CREATE TABLE IF NOT EXISTS blobkey(k BLOB PRIMARY KEY, v BLOB)")
        self.conn.commit()
        c.close()

    def put(self, key, value):
        c = self.conn.cursor()
        c.execute("INSERT OR REPLACE INTO blobkey(k, v) VALUES(?, ?)", (key, sqlite3.Binary(value)))
        self.conn.commit()
        c.close()

    def get(self, key):
        c = self.conn.cursor()
        c.execute("SELECT v FROM blobkey WHERE k = ?", (key,))
        value = c.fetchone()
        c.close()
        if value is None:
            raise KeyError(key)
        return value[0]

    def delete(self, key):
        c = self.conn.cursor()
        c.execute("DELETE FROM blobkey WHERE k = ?", (key,))
        self.conn.commit()
        rows = c.rowcount
        c.close()
        if rows == 0:
            raise KeyError

    def get_iterator(self):
        c = self.conn.cursor()
        c.execute("SELECT k, v FROM blobkey")
        return c

    def iterate(self, iterator, threshold):
        tot = 0
        ret = []        
        try:            
            while tot < threshold:
                k, v = iterator.next()
                tot = tot + len(k) + len(v)
                ret.append((k, v))
            return ret, iterator
        except StopIteration:
            return ret, iterator

class TestStores(unittest.TestCase):
    def _test_iterate(self, d):
        contents = [("Key_%d"%i, "Val_%d"%i) for i in range(100)]
        for k, v in contents:
            d.put(k, v)
        iterator = d.get_iterator()
        kvlist = []
        while True:
            kv, iterator = d.iterate(iterator, 100)
            if not kv:
                break
            kvlist.extend(kv)
        self.assertEqual(set(contents), set([(str(k), str(v)) for k, v in kvlist]))

    def test_iterate_dict(self):
        d = DictStore()
        self._test_iterate(d)

    def test_iterate_bdb(self):
        d = BerkeleyDBStore("bdb")
        self._test_iterate(d)

    def test_iterate_sqlite(self):
        d = SQLiteStore("sqlite")
        self._test_iterate(d)

if __name__=="__main__":
    unittest.main()

