#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>
################################################################################

'''
`cpl.py`: an implemenatation of the Coupled Pattern Learning
bootstrapping algorithm

### Usage

        Usage: cpl.py [options] [database] [collection] [rel] [seeds]

        Options:
          -h, --help            show this help message and exit
          -o HOST, --host=HOST  mongodb host machine name. default: localhost
          -p PORT, --port=PORT  mongodb host machine port number. default: 27017
          -s START, --start=START
                                iteration to start with. default: 1
          -t STOP, --stop=STOP  iteration to stop at. default: 2

### Caches Created

Creates 2 caches of bootstrapped instances and patterns for the target 
relation:

1. `<matrix>_<rel>_cpl_i`: bootstrapped instances for <rel>
2. `<matrix>_<rel>_cpl_p`: bootstrapped patterns for <rel>

### Bootstrapping

...
'''

import fileinput
import inspect
import multiprocessing
import pymongo
import sys
from ConfigParser import ConfigParser
import logging

import mongodb
import scorers
from bootstrapper import Bootstrapper

class CPLWorker(Bootstrapper):
    __short__ = 'cpl'
    def __init__(self, host, port, db, matrix, rel,
                 seeds, n, keep, reset, scorer, it=1):
        self.logger = multiprocessing.get_logger()
        #self.logger.setLevel(logging.DEBUG)
        self.logger.setLevel(logging.INFO)
        #self.logger.setLevel(logging.WARNING)
        if len(self.logger.handlers) == 0:
            handler = logging.StreamHandler()
            handler.setLevel(logging.INFO)
            fmt = '%(asctime)s [%(levelname)s/CPL:' + rel + '] %(message)s'
            formatter = logging.Formatter(fmt)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.boot_i = '%s_%s_cpl_i' % (matrix, rel)
        self.boot_p = '%s_%s_cpl_p' % (matrix, rel)
        Bootstrapper.__init__(
            self, host, port, db, matrix, rel, 
            seeds, n, keep, reset, scorer, it
            )

    def mutex_pred2patterns(self, pred):
        return 

    def mutex_filter_i(self, I, P, mutexes):
        '''filter out all candidate instances that have a co-occurence
        count less than three times the co-occurence count with
        mutually exclusive predicates'''
        self.logger.info('mutex_filter_i: %s' % 
                         set.intersection(set(I), set(mutexes)))
        def cooc(i, P):
            return sum( [self.scorer.pmi.F_ip(i,p) for p in P] )
        I_ = [i for i in I
              if cooc(i, P) > 3.0*cooc(i, mutexes)]
        self.logger.info('mutex_filter_i: %d => %d' % (len(I), len(I_)))
        return I_

    def mutex_filter_p(self, I, P, mutexes):
        '''filter out all candidate instances that have a co-occurence
        count less than three times the co-occurence count with
        mutually exclusive predicates'''
        self.logger.info('mutex_filter_P: %s' % 
                         set.intersection(set(P), set(mutexes)))
        def cooc(I, p):
            return sum( [self.scorer.pmi.F_ip(i,p) for i in I] )
        P_ = [p for p in P
              if cooc(I, p) > 3.0*cooc(p, mutexes)]
        self.logger.info('mutex_filter_p: %d => %d' % (len(P), len(P_)))
        return P_

    def iterate_p(self, mutexes=[]):
        '''perform an iteration of bootstrapping saving n patterns with the 
        highest reliability score'''
        if not getattr(self, 'connection', None):
            self.init_connection()

        self.logger.info(' ### BOOTSTRAPPING PATTERN ITERATION: %d ###' % 
                         self.it)

        # read promoted instances of last bootstrpping iteration
        self.logger.info('getting promoted instances...''')
        I = self.get_I(self.it-1)
        self.logger.info('I: %d' % len(I))
        self.logger.info('getting promoted instances: done.''')

        # find matching patterns
        self.logger.info('getting matching patterns...')
        P_ = self.I2P(I)
        P = self.mutex_filter_p(I, P_, mutexes)
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

    def iterate_i(self, mutexes=[]):
        '''perform an iteration of bootstrapping saving n instances with the 
        highest reliability score'''
        if not getattr(self, 'connection', None):
            self.init_connection()

        self.logger.info(' ### BOOTSTRAPPING INSTANCE ITERATION: %d ###' % 
                         self.it)

        # read promoted patterns of last bootstrpping iteration
        self.logger.info('getting promoted patterns...''')
        P = self.get_P(self.it)
        self.logger.info('P: %d' % len(P))
        self.logger.info('getting promoted patterns: done.''')

        # find matching instances
        self.logger.info('getting matching instances...')
        I_ = self.P2I(P)
        I = self.mutex_filter_i(I_, P, mutexes)
        self.logger.info('getting matching instances: done.')

        # rank instances by reliability score
        self.logger.info('ranking instances ...')
        rs = self.scorer.rank_instances(I, P, self.it)
        self.logger.info('ranking instances: done.')

        # save top n to <matrix>_boot_p
        self.logger.info('saving top %d instances...' % self.n)
        for r in rs[:self.n]:
            self.logger.info('r: %s' % r)
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

def get_scorer(scorer):
    scorers_ = dict(inspect.getmembers(scorers, inspect.isclass))
    return scorers_[scorer]

def iterate_i(kwargs):
    mutexes = kwargs.pop('mutexes', [])
    #print >>sys.stderr, 'iterate_i:', len(kwargs), kwargs
    cpl = CPLWorker(**kwargs)
    cpl.iterate_i(mutexes)

def iterate_p(kwargs):
    mutexes = kwargs.pop('mutexes', [])
    #print >>sys.stderr, 'iterate_p:', len(kwargs), kwargs
    cpl = CPLWorker(**kwargs)
    cpl.iterate_p(mutexes)

def get_I(kwargs):
    mutexes = kwargs.pop('mutexes', [])
    #print >>sys.stderr, 'get_I:', len(kwargs), kwargs
    cpl = CPLWorker(**kwargs)
    I = cpl.get_I(cpl.it)
    #print >>sys.stderr, 'get_I:', cpl.it, I
    return I

def get_P(kwargs):
    mutexes = kwargs.pop('mutexes', [])
    #print >>sys.stderr, 'get_P:', len(kwargs), kwargs
    cpl = CPLWorker(**kwargs)
    P = cpl.get_P(cpl.it)
    #print >>sys.stderr, 'get_P:', cpl.it, P
    return P

class CPLManager:
    def __init__(self, config):
        self.host = config.get('mongo', 'host')
        self.port = config.getint('mongo', 'port')
        self.db = config.get('mongo', 'db')
        self.matrix = config.get('mongo', 'matrix')
        self.scorer = get_scorer(config.get('boot', 'scorer'))
        self.keep = config.getboolean('boot', 'keep')
        self.reset = config.getboolean('boot', 'reset')
        self.n = config.getint('boot', 'n')
        self.rels = config._sections['general']['rels'].split(',')
        self.mutex = {rel:mutex.split(',')
                      for rel, mutex in config._sections['mutex'].items()
                      if rel != '__name__'}
        self.seeds = {rel:[line.strip() for line in open(seed)]
                      for rel, seed in config._sections['seeds'].items()
                      if rel != '__name__'}
        self.logger = multiprocessing.get_logger()
        #self.logger = multiprocessing.log_to_stderr()
        #self.logger.setLevel(logging.DEBUG)
        self.logger.setLevel(logging.INFO)
        #self.logger.setLevel(logging.WARNING)

    def make_mutexes(self, rel, mutex_dict):
        mutexes = set()
        for ms in self.mutex[rel]:
            for m in mutex_dict[ms]:
                mutexes.add(m)
        return sorted(mutexes)

    def make_cpl_args(self, it, mutexes=None):
        def make_args(rel, it, mutexes=None):
            args = {
                'host': self.host,
                'port': self.port,
                'db': self.db,
                'matrix': self.matrix,
                'rel': rel,
                'seeds': self.seeds[rel],
                'n': self.n,
                'keep': self.keep,
                'scorer': self.scorer,
                'it': it,
             }
            if it == 0:
                args['reset'] = self.reset
            else:
                args['reset'] = False
            if mutexes: args['mutexes'] = mutexes
            return args
        cpl_args = [make_args(rel, it, mutexes)
                    for rel in self.rels]
        return cpl_args

    def bootstrap(self, start, stop):
        pool = multiprocessing.Pool(processes=len(self.rels))
        cpl_args = self.make_cpl_args(0)
        self.logger.debug('cpl_args: %s' % cpl_args)
        Is = {rel:I
              for rel,I in zip(self.rels, 
                               pool.map(get_I, cpl_args))}
        self.logger.debug('pool_map_Is %s:' % Is)
        mutex_Is = {rel:self.make_mutexes(rel, Is)
                    for rel in self.rels}
        self.logger.debug('mutex_Is: %s' % mutex_Is)
        for it in xrange(start, stop+1):
            self.logger.debug('ITERATION %d:' % it)
            cpl_args = self.make_cpl_args(it, mutex_Is)
            pool.map(iterate_p, cpl_args)
            Ps = {rel:P 
                  for rel,P in zip(self.rels, 
                                   pool.map(get_P, cpl_args))}
            self.logger.debug('pool_map_Ps: %s' % Ps)
            mutex_Ps = {rel:self.make_mutexes(rel, Ps)
                        for rel in self.rels}
            self.logger.debug('mutex_Ps: %s' % mutex_Ps)
            cpl_args = self.make_cpl_args(it, mutex_Ps)
            pool.map(iterate_i, cpl_args)
            Is = {rel:I
                  for rel,I in zip(self.rels, 
                                   pool.map(get_I, cpl_args))}
            self.logger.debug('pool_map_Is: %s' % Is)
            mutex_Is = {rel:self.make_mutexes(rel, Is)
                        for rel in self.rels}
            self.logger.debug('mutex_Is: %s' % mutex_Is)

def main():
    from optparse import OptionParser
    usage = '''%prog [config.ini]'''
    parser = OptionParser(usage=usage)
    parser.add_option('-s', '--start', dest='start', type=int, default=1,
                      help='''iteration to start with. default: 1''')
    parser.add_option('-t', '--stop', dest='stop', type=int, default=10,
                      help='''iteration to stop at. default: 10''')
    options, args = parser.parse_args()
    if len(args) != 1:
        parser.print_help()
        exit(1)
    ini = args[0]
    config = ConfigParser()
    config.read(ini)
    cpl = CPLManager(config)
    cpl.bootstrap(options.start, options.stop)

if __name__ == '__main__':
    main()
