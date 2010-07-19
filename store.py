import sqlite3
import bsddb

class DictStore(object):
    def __init__(self):
        self._store = {}

    def put(self, key, value):
        self._store[key] = value

    def get(self, key):
        return self._store[key]

    def delete(self, key):
        del self._store[key]

class BerkeleyDBStore(object):
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

class SQLiteStore(object):
    def __init__(self, filename):
        self._db = filename
        conn = sqlite3.connect(self._db)
        c = conn.cursor()
        # Create table
        c.execute("CREATE TABLE IF NOT EXISTS blobkey(k BLOB PRIMARY KEY, v BLOB)")
        conn.commit()
        c.close()
        conn.close()

    def put(self, key, value):
        conn = sqlite3.connect(self._db)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO blobkey(k, v) VALUES(?, ?)", (key, sqlite3.Binary(value)))
        conn.commit()
        c.close()
        conn.close()

    def get(self, key):
        conn = sqlite3.connect(self._db)
        c = conn.cursor()
        c.execute("SELECT v FROM blobkey WHERE k = ?", (key,))
        value = c.fetchone()
        c.close()
        conn.close()
        if value is None:
            raise KeyError(key)
        return value[0]

    def delete(self, key):
        conn = sqlite3.connect(self._db)
        c = conn.cursor()
        c.execute("DELETE FROM blobkey WHERE k = ?", (key,))
        conn.commit()
        rows = c.rowcount
        c.close()
        conn.close()
        if rows == 0:
            raise KeyError
