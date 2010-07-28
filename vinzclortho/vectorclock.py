# Copyright (c) 2001-2010 Pär Bohrarper.
# See LICENSE for details.

import time
import unittest

class VectorClock(object):
    """A vector clock implementation. Each node's clock is stored as a
    tuple: (clock, timestamp) in a dict with name as key. This means
    that the key needs to be hashable.

    Inspired by: http://github.com/cliffmoon/dynomite/blob/master/elibs/vector_clock.erl
    """
    prune_size = 10
    prune_age = 3600.0

    def __init__(self, clocks=None):
        if clocks is not None:
            self._clocks = clocks
        else:
            self._clocks = {}

    def __repr__(self):
        return "VectorClock(%s)"%repr(self._clocks)

    def __str__(self):
        s = []
        for k, v in self._clocks:
            t, c = v
            s.append("%s, %s, (%f)" % (k, v, t))
        return "\n".join(s)

    def clone(self):
        """Returns a copy of the vector clock"""
        return VectorClock(dict(self._clocks))

    def prune(self):
        """
        This remove items so that it contains no items older than L{prune_age} 
        and a maximum of L{prune_size} items.
        """
        t = time.time()
        newclocks = dict(((k, v) for v, k in
                          sorted(((v, k) for k, v in self._clocks.items()
                                  if t - v[0] > self.prune_age))[:self.prune_size]))
        self._clocks = newclocks
        return self

    def increment(self, name):
        """
        Increments the vector clock for name.

        @param name: A unique identifier
        @type name: hashable
        """
        try:
            timestamp, clock = self._clocks[name]
        except KeyError:
            clock = 0
        self._clocks[name] = (time.time(), clock+1)
        return self

    def __eq__(self, rhs):      
        for name, v1 in rhs._clocks.items():
            try:
                v2 = self._clocks[name]
            except KeyError:
                return False
            t1, clock1 = v1
            t2, clock2 = v2
            if clock1 != clock2:
                return False
        # Everything from rhs was in self, check length to see if self has more
        return len(self._clocks) == len(rhs._clocks)

    def __ne__(self, rhs):
        return not self.__eq__(rhs)

    def descends_from(self, rhs):
        """Determines if rhs is an ancestor of self. Note that vc.descends_from(vc) returns True"""
        for name, v_r in rhs._clocks.items():
            try:
                v_s = self._clocks[name]
            except KeyError:
                return False
            t_r, clock_r = v_r
            t_s, clock_s = v_s
            if not (clock_s >= clock_r):
                return False
        # if rhs is shorter than self, then self also has versions > than rhs
        return (len(rhs._clocks) <= len(self._clocks))


def merge(a, b):
    """Merges the two vector clocks, using the latest version for each client"""
    newclocks = {}
    c_a = a._clocks
    c_b = b._clocks
    for k_a, v_a in c_a.items():
        try:
            v_b = c_b[k_a]
            t_a, clock_a = v_a
            t_b, clock_b = v_b
            if v_a > v_b:
                newclocks[k_a] = (t_a, clock_a)
            elif v_a < v_b:
                newclocks[k_a] = (t_b, clock_b)
            else:
                # use latest timestamp if equal versions
                newclocks[k_a] = (max(t_a, t_b), clock_a)
        except KeyError:
            newclocks[k_a] = v_a

    k_only_in_b = set(c_b.keys()) - set(c_a.keys())
    for k in k_only_in_b:
        newclocks[k] = c_b[k]

    return VectorClock(newclocks)

def _joiner(a, b):
    return [a, b]

def resolve(a, b, joiner=_joiner):
    """Resolves the latest value for a and b,
    which should be a tuple of (VectorClock, value).

    @param a: Tuple of L{VectorClock}, value
    @param b: Tuple of L{VectorClock}, value
    @param joiner: A function that takes the two values and produces a new value. The default joiner produces a list of the two values
    @return: A tuple of the merged vector clock and the latest (possibly joined) value.
    """
    c_a, val_a = a
    c_b, val_b = b
    if c_a == c_b:
        return (c_a, val_a)
    elif c_a.descends_from(c_b):
        return (c_a, val_a)
    elif c_b.descends_from(c_a):
        return (c_b, val_b)
    else:
        # concurrent
        newclock = merge(c_a, c_b)
        return (newclock, joiner(val_a, val_b))

def resolve_list(c, joiner=_joiner):
    """Returns the latest/merged value from a list of L{VectorClock}, value tuples"""
    def _resolve(curr, rest):
        if not rest:
            return curr
        curr = resolve(curr, rest[0], joiner)
        return _resolve(curr, rest[1:])
    return _resolve(c[0], c[1:])

def resolve_list_extend(list_):
    """Resolves the list of results to a unified result (which may be a list of concurrent versions)"""
    def joiner(a, b):
        """This way of joining concurrent versions makes it possible
        to store concurrent versions as lists, while still being able
        to return a single list for a request
        """
        if not isinstance(a, list):
            a = [a]
        if not isinstance(b, list):
            b = [b]
        return a + b
    return resolve_list(list_, joiner)

class TestVectorClock(unittest.TestCase):
    def test_empty_equals_empty(self):
        a = VectorClock()
        b = VectorClock()
        self.assertEquals(a, b)

    def test_empty_descends_from_self(self):
        a = VectorClock()
        self.assertTrue(a.descends_from(a))

    def test_incremented_descends_from_empty(self):
        a = VectorClock()
        b = VectorClock()
        a.increment("foo")
        self.assertTrue(a.descends_from(b))

    def test_merge(self):
        a = VectorClock()
        a.increment("foo")
        a.increment("bar")
        a.increment("bar")
        a.increment("foo")
        a.increment("foo")
        b = a.clone()
        b.increment("foo")
        b.increment("baz")
        c = a.clone()
        c.increment("bar")
        m = merge(b, c)
        self.assertEquals(m._clocks["foo"][1], 4)
        self.assertEquals(m._clocks["bar"][1], 3)
        self.assertEquals(m._clocks["baz"][1], 1)

    def test_resolve_not_concurrent(self):
        a = VectorClock()
        a.increment("foo")
        a.increment("bar")
        b = a.clone()
        b.increment("foo")
        c = resolve((a, "a"), (b, "b"))
        self.assertEquals(c[0], b)
        self.assertEquals(c[1], "b")

    def test_resolve_concurrent(self):
        a = VectorClock()
        a.increment("foo")
        a.increment("bar")
        b = a.clone()
        b.increment("foo")
        a.increment("baz")
        c = resolve((a, "a"), (b, "b"))
        self.assertEquals(c[0], merge(a, b))
        self.assertEquals(sorted(c[1]), sorted(["a", "b"]))

if __name__=="__main__":
    unittest.main()

