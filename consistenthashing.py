import hashlib
import itertools
import unittest
import random

def hashval(s):
    """The hash value is a 160 bit integer"""
    return int(hashlib.sha1(s).hexdigest(), 16)

MAXHASH=((2**160)-1)

def random_elem(list_):
    return list_[random.randint(0, len(list_)-1)]

class Node(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.claim = []

    def __eq__(self, rhs):
        return self.name == rhs.name

    def __ne__(self, rhs):
        return not self.__eq__(rhs)    

    @property
    def name(self):
        return str(self.host) + ":" + str(self.port)

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Node(%s, %s, %s)"%(self.host, self.port, repr(self.claim))


class Ring(object):
    def __init__(self, partitions, node):
        self.nodes = [node]
        node.claim = range(partitions)
        self.partitions = [node] * partitions

    def _walk_cw(self, key):
        """A generator that iterates all nodes, starting at the hash value provided"""
        start = self.key_to_partition(key)
        n = 0
        while n < len(self.partitions):
            if start >= len(self.partitions):
                start = 0
            yield self.partitions[start]
            n += 1
            start += 1 

    def update_node(self, node, claim):
        """This will set the number of claimed partitions to 'claim'
        by stealing/giving partitions at random
        """
        if claim > len(node.claim):
            unclaimed = set(range(len(self.partitions))) - set(node.claim)
            while len(node.claim) != claim:
                p = random_elem(list(unclaimed))
                n = self.partitions[p]
                del n.claim[n.claim.index(p)]
                n.claim.sort()
                node.claim.append(p)
                self.partitions[p] = node
                unclaimed.remove(p)
        elif claim < len(node.claim):
            others = [n for n in self.nodes if n != node]
            while len(node.claim) != claim:
                p = node.claim.pop(0)
                n = random_elem(others)
                n.claim.append(p)
                n.claim.sort()
                self.partitions[p] = n
        node.claim.sort()

    def add_node(self, node, claim=None):
        assert node not in self.nodes
        self.nodes.append(node)
        claim = claim or (len(self.partitions) // len(self.nodes))
        self.update_node(node, claim)

    def remove_node(self, node):
        self.update_node(node, 0)
        del self.nodes[self.nodes.index(node)]

    def key_to_partition(self, key):
        keys_per_partition = MAXHASH // len(self.partitions)
        return hashval(key) // keys_per_partition

    def partition_to_node(self, partition):
        return self.partitions[partition]

    def preferred(self, key, n):
        nodes = set()
        def seen(node):
            if node in nodes:
                return True
            nodes.add(node)
            return False
        return list(itertools.islice(itertools.ifilterfalse(seen, self._walk_cw(key)), n))


class TestConsistentHashing(unittest.TestCase):
    def test_new(self):
        n = Node("localhost", 8080)
        r = Ring(8, n)
        self.assertEqual(n.claim, range(8))

    def test_add_node(self):
        n1 = Node("localhost", 8080)
        n2 = Node("apansson", 8080)
        r = Ring(8, n1)
        r.add_node(n2)
        self.assertEqual(len(n1.claim), 4)
        self.assertEqual(len(n2.claim), 4)
        self.assertEqual(set(n1.claim) & set(n2.claim), set())

    def test_increase_node(self):
        n1 = Node("localhost", 8080)
        n2 = Node("apansson", 8080)
        r = Ring(8, n1)
        r.add_node(n2)
        r.update_node(n2, 6)
        self.assertEqual(len(n1.claim), 2)
        self.assertEqual(len(n2.claim), 6)
        self.assertEqual(set(n1.claim) & set(n2.claim), set())

    def test_decrease_node(self):
        n1 = Node("localhost", 8080)
        n2 = Node("apansson", 8080)
        r = Ring(8, n1)
        r.add_node(n2)
        r.update_node(n2, 2)
        self.assertEqual(len(n1.claim), 6)
        self.assertEqual(len(n2.claim), 2)
        self.assertEqual(set(n1.claim) & set(n2.claim), set())

    def test_remove_node(self):
        n1 = Node("localhost", 8080)
        n2 = Node("apansson", 8080)
        r = Ring(8, n1)
        r.add_node(n2)
        r.remove_node(n1)
        self.assertEqual(len(n1.claim), 0)
        self.assertEqual(len(n2.claim), 8)
        self.assertTrue(n1 not in r.nodes)

    def test_preferred(self):
        n = Node("localhost", 8080)
        r = Ring(1024, n)
        for i in range(16):
            r.add_node(Node("node_%d"%i, 8080))
        p = r.key_to_partition("foo")
        preferred = r.preferred("foo", 3)
        self.assertEqual(len(preferred), 3)
        self.assertTrue(p in preferred[0].claim)        

if __name__=="__main__":
    unittest.main()
