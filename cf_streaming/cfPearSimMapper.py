#!/usr/bin/env python
import sys
import math

for line in sys.stdin:
    try:
        ele = line.strip().split('#')
        n = len(ele)
        for i in xrange(n - 1):
            for j in xrange(i+1, n):
                ee1 = ele[i].split('_')
                ee2 = ele[j].split('_')
                if int(ee1[0]) < int(ee2[0]):
                    print ('%s\t%s' % (ee1[0] + '#' + ee2[0], ee1[1] + '#' + ee2[1])) 
                else:
                    print ('%s\t%s' % (ee2[0] + '#' + ee1[0], ee2[1] + '#' + ee1[1])) 
    except Exception:
        pass
