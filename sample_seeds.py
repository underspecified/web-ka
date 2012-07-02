#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>
################################################################################

import fileinput
import random
import sys

def main():
    dev_num = 10
    test_num = 100
    total_num = dev_num + test_num
    seeds = [line.strip() for line in fileinput.input()]
    seed_sample = random.sample(seeds, total_num)
    for line in seed_sample[:dev_num]:
        print >>sys.stdout, line
    for line in seed_sample[dev_num:]:
        print >>sys.stderr, line

if __name__ == '__main__':
    main()
