#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>
################################################################################

import fileinput
import math
import random
import sys
from collections import defaultdict

int_size = len(bin(sys.maxint)) - 1
int_mask = (1 << int_size) - 1

def log2(x):
    return math.log(x) / math.log(2.0)

def multiply_shift(m, a, x):
    s = int(((a * x)&int_mask) >> (int_size - m))
    #print >>sys.stderr, 'multiply_shift:', m, a, x, s
    return s

def random_odd_int():
    n = int(random.getrandbits(int_size-2))
    return n<<1|1

class Sketch:
    def __init__(self, depth, width):
        self.N = 0
        self.width = width
        self.m = int(math.ceil(log2(float(width))))
        self.rounded_width = 1 << self.m
        self.depth = depth
        self.counters = [ [0] * self.rounded_width for x in range(depth) ]
        self.hash_fns = [ random_odd_int() for x in range(depth) ]

    def _get_all(self, i):
        ix = abs(hash(i))
        values = [ multiply_shift(self.m, hf, ix) for hf in self.hash_fns ]
        counts = [ (self.counters[d][w],d,w) for d,w in enumerate(values) ]
        return counts

    def _get_min(self, i):
        return min(self._get_all(i))

    def update(self, i, c=1):
        self.N += c
        counts = self._get_all(i)
        for c_,d,w in counts:
            self.counters[d][w] += c

    def update_min(self, i, c=1):
        self.N += c
        min_c, min_d, min_w = min(self._get_all(i))
        self.counters[min_d][min_w] += c

    def estimate(self, i):
        min_c, min_d, min_w = self._get_min(i)
        return min_c

    def estimate_error(self):
        error = 2.0 * self.N / self.rounded_width
        confidence = 0.5**self.depth
        return error, confidence

if __name__ == '__main__':
    d = 10
    w = 2000000
    sketch = Sketch(d, w)
    sketch_min = Sketch(d, w)
    count = defaultdict(int)
    N = 0
    for line in fileinput.input():
        for x in line.strip().split():
            N += 1
            sketch.update(x)
            sketch_min.update(x)
            count[x] += 1
    for x in sorted(count.keys())[:100]:
        print x, count[x], sketch.estimate(x), sketch_min.estimate(x)
    print 'Error estimation:', sketch.N, sketch.depth, sketch.rounded_width, sketch.estimate_error()
    #print >>sys.stderr, sketch.counters
