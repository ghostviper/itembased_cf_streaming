#!/usr/bin/env python
import sys
import math

method = sys.argv[1]
 
(last_key, val) = (None, 0)
val_link = []
for line in sys.stdin:
    try:
        (key, val) = line.strip().split()
    except Exception:
        continue
    if last_key and last_key != key:
        norm_2(last_key, val_link, method)

        val_link = []
        val_link.append(val)
        last_key = key
    else:
        last_key = key
        val_link.append(val)
if last_key:
    norm_2(last_key, val_link, method)


def norm_2(key, val_link, method='euro'):
    v = 0
    if method == 'eurl':
        for val in val_link:
            v = v + float(val) * float(val)
        print ('%s\t%s' % (key, v))
    else:
        for val in val_link:
            v = v + abs(float(val))
        print ('%s\t%s' % (key, v))
    
           
            
    