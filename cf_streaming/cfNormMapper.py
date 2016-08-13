#!/usr/bin/env python
import sys
import math


for line in sys.stdin:
    try:
        ele = line.strip().split('#')
        n = len(ele)
        for i in xrange(n):
            ee = ele[i].strip().split('_')
            print ('%s\t%s' % (ee[0], ee[1])) 
    except Exception:
        pass
