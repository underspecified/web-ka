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
        self.host = host
        self.port = port
        self.db = db
        self.matrix = matrix
        self.rel = rel
        self.seeds = seeds
        self.n = n
        self.keep = keep
        self.reset = reset
        self.scorer_class = scorer
        self.it = it
        self.set_collection_names()
        self.init_connection()

    def set_collection_names(self):
        boot_i_args = [self.matrix, self.rel, self.__short__, 'i',
                       self.scorer_class.__short__]
        boot_p_args = [self.matrix, self.rel, self.__short__, 'p',
                       self.scorer_class.__short__]
        if self.keep:
            boot_i_args.append('keep')
            boot_p_args.append('keep')
        else:
            boot_i_args.append('nokeep')
            boot_p_args.append('nokeep')
        self.boot_i = '_'.join(boot_i_args)
        self.logger.info('boot_i: %s' % self.boot_i)
        self.boot_p = '_'.join(boot_p_args)
        self.logger.info('boot_p: %s' % self.boot_p)

    def init_connection(self):
        self.logger.info('initializing mongodb connection ...')
        self.connection = pymongo.Connection(self.host, self.port)
        self.db = self.connection[self.db]
        self.logger.info('initializing mongodb connection: done')
        self.args = self.get_args()
        self.scorer = self.scorer_class(
            self.db, self.matrix, self.boot_i, self.boot_p, self.logger
            )
        if self.reset: self.do_reset()
        if not self.has_seeds(): self.add_seeds()

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
        return self.has_run(self.db, self.boot_i, 0)

    def add_seeds(self):
        '''adds seeds to db.coll with reliability score of 1.0'''
        self.logger.debug('add_seeds: %d %s' % 
                          (len(self.seeds), self.seeds))
        for s in self.seeds:
            self.logger.debug('seed: %s' % s)
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
        return [tuple( [v
                       for k,v in sorted(r.items()) 
                       if k.startswith('arg')] )
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
        P_ = tuple(sorted(set(P)))
        self.logger.info('P: %d => %d' % (len(P), len(P_)))
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
        I_ = tuple(sorted(set(I)))
        self.logger.info('I: %d => %d' % (len(I), len(I_)))
        return I_

    def iterate_p(self):
        '''perform an iteration of bootstrapping saving n patterns with the 
        highest reliability score'''
        if not getattr(self, 'connection', None):
            self.init_connection()

        self.logger.info('### BOOTSTRAPPING PATTERN ITERATION: %d ###' %
                         self.it)

        # read promoted instances of last bootstrpping iteration
        self.logger.info('getting promoted instances...''')
        I = self.get_I(self.it-1)
        self.logger.info('I: %d' % len(I))
        self.logger.info('getting promoted instances: done.''')

        # find matching patterns
        self.logger.info('getting matching patterns...')
        P = self.I2P(I)
        self.logger.info('getting matching patterns: done.')

        # rank patterns by reliability score
        self.logger.info('ranking patterns ...')
        rs = self.scorer.rank_patterns(I, P, self.it)
        self.logger.info('ranking patterns: done.')

        # save top n to <matrix>_boot_p
        self.logger.info('saving top %d patterns...' % self.n)
        for r in rs[:self.n]:
            self.logger.info('r: %s' % r)
            mongodb.cache(self.db, self.boot_p, r)
        self.logger.info('saving top %d patterns: done.' % self.n)

        self.logger.info('ensuring indices ...')
        # index for iteration number
        self.db[self.boot_p].ensure_index( [('it', pymongo.DESCENDING), ] )
        # index for <REL>
        self.db[self.boot_p].ensure_index( [('rel', pymongo.ASCENDING), ] )
        self.logger.info('ensuring indices: done.')

    def iterate_i(self):
        '''perform an iteration of bootstrapping saving n instances with the 
        highest reliability score'''
        if not getattr(self, 'connection', None):
            self.init_connection()

        self.logger.info('### BOOTSTRAPPING INSTANCE ITERATION: %d ###' % 
                         self.it)

        # read promoted patterns of last bootstrpping iteration
        self.logger.info('getting promoted patterns...''')
        P = self.get_P(self.it)
        self.logger.info('P: %d' % len(P))
        self.logger.info('getting promoted patterns: done.''')

        # find matching instances
        self.logger.info('getting matching instances...')
        I = self.P2I(P)
        self.logger.info('getting matching instances: done.')

        # rank instances by reliability score
        self.logger.info('ranking instances ...')
        rs = self.scorer.rank_instances(I, P, self.it)
        self.logger.info('ranking instances: done.')

        # save top n to <matrix>_boot_p
        self.logger.info('saving top %d instances...' % self.n)
        for r in rs[:self.n]:
            self.logger.info('r:', r)
            mongodb.cache(self.db, self.boot_i, r)
        self.logger.info('saving top %d instances: done.' % self.n)

        self.logger.info('ensuring indices ...')
        # index for iteration number
        self.db[self.boot_i].ensure_index( [('it', pymongo.DESCENDING), ] )
        # index for <ARGJ,...,ARGN>
        self.db[self.boot_i].ensure_index(
            [(arg, pymongo.ASCENDING)
             for arg in self.args]
            )
        self.logger.info('ensuring indices: done.')

    def iterate(self):
        self.iterate_p()
        self.iterate_i()
        self.it += 1

    def bootstrap(self, start, stop):
        '''apply espresso bootstrapping algorithm for rel from
        iteration start to stop'''
        for it in xrange(start, stop+1):
            self.iterate()

    def do_reset(self):
        '''reset bootstrapping by deleting collections of bootstraped
        args and rels'''
        
        if self.it <= 1:
            self.logger.info('resetting %s and %s ...' % 
                             (self.boot_i, self.boot_p))
            self.db.drop_collection(self.boot_i)
            self.db.drop_collection(self.boot_p)
            self.logger.info('resetting %s and %s: done.' % 
                             (self.boot_i, self.boot_p))
