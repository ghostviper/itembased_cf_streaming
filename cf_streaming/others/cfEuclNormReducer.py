#!/usr/bin/env python
import sys
import math

    
def norm(key, val_link, method='euro'):
    v = 0
    if method == 'euro':
        for val in val_link:
            v = v + float(val) * float(val)
        print ('%s\t%s' % (key, v))
        
(last_key, val) = (None, 0)
val_link = []
for line in sys.stdin:
    try:
        (key, val) = line.strip().split()
    except Exception:
        continue
    if last_key and last_key != key:
        norm(last_key, val_link)

        val_link = []
        val_link.append(val)
        last_key = key
    else:
        last_key = key
        val_link.append(val)
if last_key:
    norm(last_key, val_link)
  
