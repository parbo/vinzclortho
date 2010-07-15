import hashlib

class Node(object):
    def __init__(self, physical, virtual):
        self.physical = physical
        self.virtual = virtual
        self.claimed = []

class Ring(object):
    _num_partitions = 1024
    def __init__(self):
        self._nodes = []

    def _hash(self, key):
        return hashlib.sha1(key).digest()
        
    def preferred(self, key):
        pass

    
