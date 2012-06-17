#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>
################################################################################

import pymongo
import sys

import mongodb
from matrix2pmi import PMI

def get_args(db, matrix):
    '''returns a lists of argument names in <matrix>'''
    x = db[matrix].find_one()
    return sorted([k 
                   for k in x.keys()
                   if k.startswith('arg')])


class Bootstrapper:
    def __init__(self, db, matrix, rel, n):
        self.db = db
        self.matrix = matrix
        self.rel = rel
        self.n = n
        self.args = get_args(self.db, self.matrix)
        self.esp_i = '%s_%s_esp_i' % (self.matrix, self.rel)
        self.esp_p = '%s_%s_esp_p' % (self.matrix, self.rel)

    def get_I(self, it, query={}):
        '''retrieves instances that match query from iteration it'''
        query['it'] = it
        return [[v
                 for k,v in sorted(r.items()) 
                 if k.startswith('arg')]
                for r in mongodb.fast_find(
                self.db, self.esp_i, query, fields=self.args
                ) ]

    def get_P(self, it, query={}):
        '''retrieves patterns that match query from iteration it'''
        query['it'] = it
        return [r['rel'] 
                for r in mongodb.fast_find(
                self.db, self.esp_p, query, fields=['rel']
                ) ]

    def I2P(self, I):
        '''retrieve patterns that match promoted instances in I and have not been
        retrieved in past iteration'''
        P = [r['rel']
             for i in I
             for r in mongodb.fast_find(
                self.db, self.matrix, mongodb.make_query(i=i,p=None), fields=['rel']
                )
             if not self.db[self.esp_p].find_one({'rel':r['rel']}) ]
        P_ = sorted(set(P))
        print >>sys.stderr, 'P: %d => %d' % (len(P), len(P_))
        return P_

    def P2I(self, P):
        '''retrieve instances that match promoted patterns in P and have not been
        retrieved in past iteration'''
        I = [tuple( [v
                     for k,v in sorted(r.items())
                     if k.startswith('arg')] )
             for p in P
             for r in mongodb.fast_find(
                self.db, self.matrix, mongodb.make_query(i=None,p=p), fields=self.args
                )
             if not self.db[self.esp_i].find_one(
                {k:v 
                 for k,v in sorted(r.items())
                 if k.startswith('arg')} ) ]
        I_ = sorted(set(I))
        print >>sys.stderr, 'I: %d => %d' % (len(I), len(I_))
        return I_

    def bootstrap_p(self, it):
        '''perform an iteration of bootstrapping saving n patterns with the 
        highest reliability score'''
        # read promoted instances of last bootstrpping iteration
        print >>sys.stderr, 'getting promoted instances...'''
        I = self.get_I(it-1)
        print >>sys.stderr, 'I:', len(I)
        print >>sys.stderr, 'getting promoted instances: done.'''

        # find matching patterns
        print >>sys.stderr, 'getting matching patterns...'
        P = self.I2P(I)
        print >>sys.stderr, 'getting matching patterns: done.'

        # rank patterns by reliability score
        print >>sys.stderr, 'ranking patterns by reliability score...'
        rs = self.rank_patterns(I, P, it)
        print >>sys.stderr, 'ranking patterns by reliability score: done.'

        # save top n to <matrix>_esp_p
        print >>sys.stderr, 'saving top %d patterns...' % self.n
        for r in rs[:self.n]:
            print >>sys.stderr, 'r:', r
            mongodb.cache(self.db, self.esp_p, r)
        print >>sys.stderr, 'saving top %d patterns: done.' % self.n

        print >>sys.stderr, 'ensuring indices ...'
        # index for iteration number
        self.db[self.esp_p].ensure_index( [('it', pymongo.DESCENDING), ] )
        # index for <REL>
        self.db[self.esp_p].ensure_index( [('rel', pymongo.ASCENDING), ] )
        print >>sys.stderr, 'ensuring indices: done.'

    def bootstrap_i(self, it):
        '''perform an iteration of bootstrapping saving n instances with the 
        highest reliability score'''
        # read promoted patterns of last bootstrpping iteration
        print >>sys.stderr, 'getting promoted patterns...'''
        P = self.get_P(it)
        print >>sys.stderr, 'P:', len(P)
        print >>sys.stderr, 'getting promoted patterns: done.'''

        # find matching instances
        print >>sys.stderr, 'getting matching instances...'
        I = self.P2I(P)
        print >>sys.stderr, 'getting matching instances: done.'

        # rank instances by reliability score
        print >>sys.stderr, 'ranking instances by reliability score...'
        rs = self.rank_instances(I, P, it)
        print >>sys.stderr, 'ranking instances by reliability score: done.'

        # save top n to <matrix>_esp_p
        print >>sys.stderr, 'saving top %d instances...' % self.n
        for r in rs[:self.n]:
            print >>sys.stderr, 'r:', r
            mongodb.cache(self.db, self.esp_i, r)
        print >>sys.stderr, 'saving top %d instances: done.' % self.n

        print >>sys.stderr, 'ensuring indices ...'
        # index for iteration number
        self.db[self.esp_i].ensure_index( [('it', pymongo.DESCENDING), ] )
        # index for <ARGJ,...,ARGN>
        self.db[self.esp_i].ensure_index(
            [(arg, pymongo.ASCENDING)
             for arg in self.args]
            )
        print >>sys.stderr, 'ensuring indices: done.'

    def bootstrap(self, start, stop):
        '''apply espresso bootstrapping algorithm for rel from iteration start to
        stop'''
        for it in xrange(start, stop):
            print >>sys.stderr, 'pattern bootstrapping iter: %d' % it
            self.bootstrap_p(it)
            print >>sys.stderr, 'instance bootstrapping iter: %d' % it
            self.bootstrap_i(it)
