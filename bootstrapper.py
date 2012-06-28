#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>
################################################################################

import pymongo
import sys

import mongodb
from matrix2pmi import PMI


class Bootstrapper:
    def __init__(self, host, port, db, matrix, rel,
                 seeds, n, keep, reset, scorer, it=1):
        self.connection = pymongo.Connection(host, port)
        self.db = self.connection[db]
        self.matrix = matrix
        self.rel = rel
        self.n = n
        self.keep = keep
        if reset: self.reset()
        self.add_seeds(seeds)
        self.args = self.get_args()
        self.scorer = scorer(self.db, self.matrix, self.boot_i, self.boot_p)
        self.it = it

    def get_args(self):
        '''returns a lists of argument names in <matrix>'''
        x = self.db[self.matrix].find_one()
        return sorted([k 
                       for k in x.keys()
                       if k.startswith('arg')])

    def has_run(self, db, coll, i=0):
        '''determines if db.coll has iteration it'''
        if db[coll].find_one({'it':i}):
            return True
        else:
            return False

    def has_seeds(self):
        '''determines if db.coll has seed iteration'''
        return has_run(self.db, self.boot_i, 0)

    def add_seeds(self, seeds):
        '''adds seeds to db.coll with reliability score of 1.0'''
        for s in seeds:
            #print >>sys.stderr, 's:', s
            args = s.split('\t')
            doc = {'arg%d'%n:v
                   for n,v in enumerate(args, 1)}
            doc['it'] = 0
            doc['score'] = 1.0
            mongodb.cache(self.db, self.boot_i, doc)

    def get_I(self, it, query={}):
        '''retrieves instances that match query from iteration it'''
        if self.keep:
            query['it'] = {'$lte':it}
        else:
            query['it'] = it
        return [[v
                 for k,v in sorted(r.items()) 
                 if k.startswith('arg')]
                for r in mongodb.fast_find(
                self.db, self.boot_i, query, fields=self.args
                ) ]

    def get_P(self, it, query={}):
        '''retrieves patterns that match query from iteration it'''
        if self.keep:
            query['it'] = {'$lte':it}
        else:
            query['it'] = it
        return [r['rel'] 
                for r in mongodb.fast_find(
                self.db, self.boot_p, query, fields=['rel']
                ) ]

    def I2P(self, I):
        '''retrieve patterns that match promoted instances in I and
        have not been retrieved in past iteration'''
        P = [r['rel']
             for i in I
             for r in mongodb.fast_find(
                self.db, self.matrix, 
                mongodb.make_query(i=i,p=None), fields=['rel']
                )
             if not self.db[self.boot_p].find_one({'rel':r['rel']}) ]
        P_ = sorted(set(P))
        print >>sys.stderr, 'P: %d => %d' % (len(P), len(P_))
        return P_

    def P2I(self, P):
        '''retrieve instances that match promoted patterns in P and
        have not been retrieved in past iteration'''
        I = [tuple( [v
                     for k,v in sorted(r.items())
                     if k.startswith('arg')] )
             for p in P
             for r in mongodb.fast_find(
                self.db, self.matrix, 
                mongodb.make_query(i=None,p=p), fields=self.args
                )
             if not self.db[self.boot_i].find_one(
                {k:v 
                 for k,v in sorted(r.items())
                 if k.startswith('arg')} ) ]
        I_ = sorted(set(I))
        print >>sys.stderr, 'I: %d => %d' % (len(I), len(I_))
        return I_

    def iterate_p(self):
        '''perform an iteration of bootstrapping saving n patterns with the 
        highest reliability score'''
        print >>sys.stderr, 'pattern bootstrapping iter: %d' % self.it

        # read promoted instances of last bootstrpping iteration
        print >>sys.stderr, 'getting promoted instances...'''
        I = self.get_I(self.it-1)
        print >>sys.stderr, 'I:', len(I)
        print >>sys.stderr, 'getting promoted instances: done.'''

        # find matching patterns
        print >>sys.stderr, 'getting matching patterns...'
        P = self.I2P(I)
        print >>sys.stderr, 'getting matching patterns: done.'

        # rank patterns by reliability score
        print >>sys.stderr, 'ranking patterns ...'
        rs = self.scorer.rank_patterns(I, P, self.it)
        print >>sys.stderr, 'ranking patterns: done.'

        # save top n to <matrix>_boot_p
        print >>sys.stderr, 'saving top %d patterns...' % self.n
        for r in rs[:self.n]:
            print >>sys.stderr, 'r:', r
            mongodb.cache(self.db, self.boot_p, r)
        print >>sys.stderr, 'saving top %d patterns: done.' % self.n

        print >>sys.stderr, 'ensuring indices ...'
        # index for iteration number
        self.db[self.boot_p].ensure_index( [('it', pymongo.DESCENDING), ] )
        # index for <REL>
        self.db[self.boot_p].ensure_index( [('rel', pymongo.ASCENDING), ] )
        print >>sys.stderr, 'ensuring indices: done.'

    def iterate_i(self):
        '''perform an iteration of bootstrapping saving n instances with the 
        highest reliability score'''
        print >>sys.stderr, 'instance bootstrapping iter: %d' % self.it

        # read promoted patterns of last bootstrpping iteration
        print >>sys.stderr, 'getting promoted patterns...'''
        P = self.get_P(self.it)
        print >>sys.stderr, 'P:', len(P)
        print >>sys.stderr, 'getting promoted patterns: done.'''

        # find matching instances
        print >>sys.stderr, 'getting matching instances...'
        I = self.P2I(P)
        print >>sys.stderr, 'getting matching instances: done.'

        # rank instances by reliability score
        print >>sys.stderr, 'ranking instances ...'
        rs = self.scorer.rank_instances(I, P, self.it)
        print >>sys.stderr, 'ranking instances: done.'

        # save top n to <matrix>_boot_p
        print >>sys.stderr, 'saving top %d instances...' % self.n
        for r in rs[:self.n]:
            print >>sys.stderr, 'r:', r
            mongodb.cache(self.db, self.boot_i, r)
        print >>sys.stderr, 'saving top %d instances: done.' % self.n

        print >>sys.stderr, 'ensuring indices ...'
        # index for iteration number
        self.db[self.boot_i].ensure_index( [('it', pymongo.DESCENDING), ] )
        # index for <ARGJ,...,ARGN>
        self.db[self.boot_i].ensure_index(
            [(arg, pymongo.ASCENDING)
             for arg in self.args]
            )
        print >>sys.stderr, 'ensuring indices: done.'

    def iterate(self):
        self.iterate_p()
        self.iterate_i()

    def bootstrap(self, start, stop):
        '''apply espresso bootstrapping algorithm for rel from
        iteration start to stop'''
        for it in xrange(start, stop+1):
            self.iterate_p()
            self.iterate_i()

    def reset(self):
        '''reset bootstrapping by deleting collections of bootstraped
        args and rels'''
        self.db.drop_collection(self.boot_i)
        self.db.drop_collection(self.boot_p)
