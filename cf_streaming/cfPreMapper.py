#!/usr/bin/env python
import sys
import math
import os

kv_dep = '_'
ele_dep = '#'
try:
    kv_dep = os.getenv('KV_DEP')
    ele_dep = os.getenv('ELE_DEP')
    if kv_dep == None or ele_dep == None:
        kv_dep = '_'
        ele_dep = '#'
except Exception:
    pass
    
class Preprocess:
    def bin(self):
        for line in sys.stdin:
            try:
                ele = line.strip().split(ele_dep)
                n = len(ele)
                s = ''
                for i in xrange(n):
                    ee = ele[i].strip().split(kv_dep)
                    s = s + ee[0] + kv_dep + str(1) + ele_dep
                print ('%s' % (s[0: -1]))
            except Exception:
                pass
            
if __name__ == "__main__":
    pre = Preprocess()
    pre.bin()
    