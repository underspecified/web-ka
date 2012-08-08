#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>
################################################################################

import fileinput
import math
import sys
from collections import defaultdict
from countmin import Sketch
from functools import partial
from heapq import heappush, heappushpop

class OnlinePMI:
    def __init__(self, d, depth, width):
        self.d = d
        self.sketch = Sketch(depth, width)
        self.S = set()
        self.V = defaultdict(list)

    def pmi(self, x, y):
        c = self.sketch.estimate
        log2 = lambda x: math.log(x,2)
        try:
            return log2( c((x,y)) / (c(x) * c(y)) )
        except ValueError:
            return 0.0

    def update(self, B):
        for z,y in B:
            self.S.add((z,y))
            self.sketch.update(z)
            self.sketch.update(y)
            self.sketch.update((z,y))
        # recompute vectors V(x) using current contexts in
        # priority queue and {y|S(<z,y>)=1}
        xs = set([x for x,y in self.S])
        for x in xs:
            ys = set.union( set([y for x_,y in self.S if x_ == x]),
                            set([y for pmi,y in self.V[x]]) )
            for y in ys: 
                if len(self.V[x]) < self.d:
                    heappush(self.V[x], (self.pmi(x,y), y))
                else:
                    heappushpop(self.V[x], (self.pmi(x,y), y))

def line2rel_args(line):
    arg1,rel,arg2 = line.strip().split('\t')
    return rel, (arg1, arg2)

if __name__ == '__main__':
    d = 10
    depth = 5
    width = 2000
    opmi = OnlinePMI(d, depth, width)
    B = ( line2rel_args(line)
          for line in fileinput.input() )
    opmi.update(B)
    for x in opmi.V:
        for pmi,y in opmi.V[x]:
            print >>sys.stderr, x, y, pmi
