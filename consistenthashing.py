import hashlib
import itertools
import unittest
import bisect

def hashval(s):
    return int(hashlib.md5(s).hexdigest(), 16)

class Node(object):
    def __init__(self, host, port, num_vnodes):
        self.host = host
        self.port = port
        self.nodes = [VirtualNode(self, i) for i in range(num_vnodes)]
        self._hash = hashval(self.name)

    def __eq__(self, rhs):
        return self._hash == rhs._hash

    def __ne__(self, rhs):
        return not self.__eq__(rhs)    

    @property
    def name(self):
        return str(self.host) + ":" + str(self.port)

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Node(%s, %s, %s)"%(self.host, self.port, len(self.nodes))

class VirtualNode(object):
    def __init__(self, node, vnode):
        self.node = node
        self.vnode = vnode
        self._hash = hashval(self.name)

    def __eq__(self, rhs):
        return self._hash == rhs._hash

    def __ne__(self, rhs):
        return not self.__eq__(rhs)

    def __cmp__(self, rhs):
        try:
            return cmp(self._hash, rhs._hash)
        except AttributeError:
            # compare with just a hash
            return cmp(self._hash, rhs)

    @property
    def name(self):
        return "node_%d@%s"%(self.vnode, self.node.name)

    def __str__(self):
        return self.name

    def __repr__(self):
        return "VirtualNode(%s, %d)"%(repr(self.node), self.vnode)

class Ring(object):
    def __init__(self, nodes=None):
        self.vnodes = []
        nodes = nodes or []
        for n in nodes:
            self.add_node(n)

    def _walk_cw(self, vnode_or_hash):
        """A generator that iterates all nodes, starting at the hash value provided"""
        start = bisect.bisect_left(self.vnodes, vnode_or_hash)
        n = 0
        while n < len(self.vnodes):
            if start >= len(self.vnodes):
                start = 0
            yield self.vnodes[start]
            n = n + 1
            start = start + 1 

    def add_node(self, node):
        for vnode in node.nodes:
            self.add_vnode(vnode)

    def add_vnode(self, vnode):
        bisect.insort(self.vnodes, vnode)

    def remove_node(self, node):
        for vnode in node.nodes:
            self.remove_vnode(vnode)

    def remove_vnode(self, vnode):
        self.vnodes.remove(vnode)

    def key_to_vnode(self, key):
        return self.vnodes[bisect.bisect_left(self.vnodes, hashval(key))]       

    def preferred(self, vnode, n):
        # walk clockwise from node, and return n vnodes that don't have the same parent
        physnodes = set()
        def seen(vnode):
            if vnode.node in physnodes:
                return True
            physnodes.add(vnode.node)
            return False
        return list(itertools.islice(itertools.ifilterfalse(seen, self._walk_cw(vnode)), n))

    def preferred_from_key(self, key, n):
        vnode = self.key_to_vnode(key)
        return self.preferred(vnode, n)
    
class TestConsistentHashing(unittest.TestCase):
    def test_add_node(self):
        n = Node("localhost", 8080, 1)
        r = Ring([n])

        for vnode in n.nodes:
            self.assertEqual(1, len(r.preferred(vnode, 3)))

    def test_add_10_nodes(self):
        nodes = [Node("node_%d"%i, 8080, 1) for i in range(10)]
        r = Ring(nodes)

        for vnode in r.vnodes:
            preferred = r.preferred(vnode, 3)
            self.assertEqual(3, len(preferred))
            self.assertEqual(vnode, preferred[0])
            for p in preferred[1:]:
                self.assertTrue(p.node != vnode.node)            

    def test_add_10_nodes_with_100_vnodes(self):
        nodes = [Node("node_%d"%i, 8080, 100) for i in range(10)]
        r = Ring(nodes)

        for vnode in r.vnodes:
            preferred = r.preferred(vnode, 3)
            self.assertEqual(3, len(preferred))
            self.assertEqual(vnode, preferred[0])
            for p in preferred[1:]:
                self.assertTrue(p.node != vnode.node)            

    def test_walk(self):
        nodes = [Node("node_%d"%i, 8080, 1) for i in range(10)]
        r = Ring(nodes)

        n = r.vnodes[3]
        g = r._walk_cw(n._hash)
        next = g.next()
        self.assertEqual(next, r.vnodes[3])
        g = r._walk_cw(n._hash-1)
        next = g.next()
        self.assertEqual(next, r.vnodes[3])
        g = r._walk_cw(n._hash+1)
        next = g.next()
        self.assertEqual(next, r.vnodes[4])
        g = r._walk_cw(0)
        next = g.next()
        self.assertEqual(next, r.vnodes[0])
        g = r._walk_cw(r.vnodes[-1]._hash+1)
        next = g.next()
        self.assertEqual(next, r.vnodes[0])

        for vnode in r.vnodes:
            preferred = r.preferred(vnode, 3)
            self.assertEqual(3, len(preferred))
            self.assertEqual(vnode, preferred[0])
            for p in preferred[1:]:
                self.assertTrue(p.node != vnode.node)            

if __name__=="__main__":
    unittest.main()
