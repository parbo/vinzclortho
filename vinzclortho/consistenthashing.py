# Copyright (c) 2001-2010 Pär Bohrarper.
# See LICENSE for details.

import hashlib
import itertools
import unittest
import random

import logging
log = logging.getLogger("vinzclortho.consistenthashing")

def hashval(s):
    """The hash value is a 160 bit integer"""
    return int(hashlib.sha1(s).hexdigest(), 16)

MAXHASH=((2**160)-1)

def pop_random_elem(list_):
    ix = random.randint(0, len(list_)-1)
    val = list_[ix]
    del list_[ix]
    return val

def random_elem(list_):
    return list_[random.randint(0, len(list_)-1)]

class Node(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.wanted = None
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
    def __init__(self, partitions, node, N):
        self.nodes = [node]
        node.claim = range(partitions)
        self.partitions = [node] * partitions
        self.num_partitions = partitions
        self._partition_set = set(range(partitions))
        self._wanted_N = N
        self.N = len(self.nodes)

    def _walk_cw(self, start):
        """A generator that iterates all partitions, starting at the partition provided"""
        n = 0
        sz = self.num_partitions
        while n < sz:
            if start >= sz:
                start = 0
            yield start
            n += 1
            start += 1

    def _walk_ccw(self, start):
        """A generator that iterates all partitions backwards, starting at the partition provided"""
        n = 0
        sz = self.num_partitions
        while n < sz:
            if start < 0:
                start = sz - 1
            yield start
            n += 1
            start -= 1

    def get_node(self, name):
        for n in self.nodes:
            if n.name == name:
                return n

    def replicated(self, node):
        rep = set()
        for p in node.claim:
            for p_ in itertools.islice(self._walk_ccw(p), 1, self.N):
                rep.add(p_)
        return rep

    def _neighbours(self, p):
        sz = self.num_partitions
        return [n%sz for n in range(p-self.N+1, p+self.N)]

    def _replicated_in(self, p):
        sz = self.num_partitions
        return [n%sz for n in range(p-self.N+1, p)]

    def unwanted(self, claim):
        r = set()
        for p in claim:
            r.update(self._neighbours(p))
        return r

    def _swap(self, p1, p2):
        """This swaps owner of p1 and p2"""
        n1 = self.partitions[p1]
        n2 = self.partitions[p2]
        n1.claim.remove(p1)
        n1.claim.append(p2)
        n1.claim.sort()
        n2.claim.remove(p2)
        n2.claim.append(p1)
        n2.claim.sort()
        self.partitions[p2] = n1
        self.partitions[p1] = n2

    def fix_constraint(self):
        # Check that replicas are on separate nodes
        for p in range(self.num_partitions):
            node = self.partitions[p]
            rep = [self.partitions[p_] for p_ in self._replicated_in(p)]
            if node in rep:
                g = self._walk_cw(p)
                g.next()
                for p_ in g:
                    if self.partitions[p_] not in rep:
                        self._swap(p, p_)
                        break

    def update_claim(self):
        # Check that all nodes have roughly the claim they wanted..
        for n in self.nodes:
            want = n.wanted or (self.num_partitions // len(self.nodes))
            if abs(len(n.claim)-want) > 3: # arbitrary thresholds ftw!
                self.update_node(n, n.wanted)

    def ok(self):
        for n in self.nodes:
            for i, p in enumerate(n.claim):
                d = abs(p-n.claim[i-1])
                if d < self.N - 1:
                    return False
        return True

    def update_node(self, node, claim, force=False):
        """This will set the number of claimed partitions to 'claim'
        by stealing/giving partitions at random
        """
        log.info("Updating node %s %s %s", node, claim, force)
        node.wanted = claim
        claim = claim or (self.num_partitions // len(self.nodes))
        unwanted = self.unwanted(node.claim)
        while claim > len(node.claim):
            available = self._partition_set - unwanted
            try:
                p = random_elem(list(available))
            except ValueError:
                # No partitions left to grab
                break
            n = self.partitions[p]
            n.claim.remove(p)
            n.claim.sort()
            node.claim.append(p)
            node.claim.sort()
            self.partitions[p] = node
            for p_ in self._neighbours(p):
                unwanted.add(p_)

        while claim < len(node.claim):
            p_from = random_elem(node.claim)
            available_nodes = set(self.nodes) - set([self.partitions[p] for p in self._neighbours(p_from)])
            try:
                n = random_elem(list(available_nodes))
            except ValueError:
                # no node could take it without breaking the replication constraint
                if force:
                    # hand it to one anyway
                    n = random_elem(list(set(self.nodes) - set([node])))
                else:
                    log.info("Could not handover.. %s %d", claim, len(node.claim))
                    break
            n.claim.append(p_from)
            n.claim.sort()
            node.claim.remove(p_from)
            node.claim.sort()
            self.partitions[p_from] = n

    def add_node(self, node, claim=None):
        assert node not in self.nodes
        self.nodes.append(node)
        self.N = min(len(self.nodes), self._wanted_N)
        self.update_node(node, claim)
        if not self.ok():
            self.fix_constraint()

    def remove_node(self, node):
        self.update_node(node, 0, True)
        del self.nodes[self.nodes.index(node)]
        self.N = min(len(self.nodes), self._wanted_N)
        if not self.ok():
            self.fix_constraint()

    def key_to_partition(self, key):
        keys_per_partition = MAXHASH // self.num_partitions
        return hashval(key) // keys_per_partition

    def partition_to_node(self, partition):
        return self.partitions[partition]

    def preferred(self, key):
        """Returns tuple of (preferred, fallbacks)"""
        cwnodelist = [self.partitions[p] for p in self._walk_cw(self.key_to_partition(key))]
        return cwnodelist[:self.N], cwnodelist[self.N:]

class TestConsistentHashing(unittest.TestCase):
    def test_new(self):
        n = Node("localhost", 8080)
        r = Ring(8, n, 3)
        self.assertEqual(n.claim, range(8))
        self

    def test_add_node(self):
        n1 = Node("localhost", 8080)
        n2 = Node("apansson", 8080)
        r = Ring(8, n1, 3)
        r.add_node(n2)
        self.assertEqual(set(n1.claim) & set(n2.claim), set())

    def test_add_many_nodes(self):
        n = Node("localhost", 8080)
        r = Ring(1024, n, 3)
        for i in range(64):
            r.add_node(Node("node_%d"%i, 8080))
        self.assertTrue(r.ok())

    def test_increase_node(self):
        n1 = Node("localhost", 8080)
        n2 = Node("apansson", 8080)
        r = Ring(8, n1, 3)
        r.add_node(n2)
        r.update_node(n2, 6)
        self.assertEqual(set(n1.claim) & set(n2.claim), set())

    def test_decrease_node(self):
        n1 = Node("localhost", 8080)
        n2 = Node("apansson", 8080)
        r = Ring(8, n1, 3)
        r.add_node(n2)
        r.update_node(n2, 2)
        self.assertEqual(set(n1.claim) & set(n2.claim), set())

    def test_remove_node_less_than_N(self):
        n1 = Node("localhost", 8080)
        n2 = Node("apansson", 8080)
        r = Ring(8, n1, 3)
        r.add_node(n2)
        r.remove_node(n1)
        self.assertEqual(len(n1.claim), 0)
        self.assertEqual(len(n2.claim), 8)
        self.assertTrue(n1 not in r.nodes)

    def test_remove_node(self):
        n = Node("localhost", 8080)
        r = Ring(64, n, 3)
        for i in range(8):
            r.add_node(Node("node_%d"%i, 8080))
        r.remove_node(n)
        self.assertEqual(len(n.claim), 0)
        self.assertTrue(n not in r.nodes)

    def test_preferred(self):
        n = Node("localhost", 8080)
        r = Ring(64, n, 3)
        for i in range(8):
            r.add_node(Node("node_%d"%i, 8080))
        p = r.key_to_partition("foo")
        preferred, fallbacks = r.preferred("foo")
        self.assertEqual(len(preferred), 3)
        self.assertTrue(p in preferred[0].claim)

    def test_replicated(self):
        n = Node("localhost", 8080)
        r = Ring(128, n, 3)
        for i in range(8):
            r.add_node(Node("node_%d"%i, 8080))
        for i, n in enumerate(r.nodes):
            rep = r.replicated(n)
            self.assertEqual(rep & set(n.claim), set())


if __name__=="__main__":
    unittest.main()
