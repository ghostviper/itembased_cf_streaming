#!/usr/bin/env python
import sys
import math

    
def norm(key, val_link):
    sum_ = 0
    squa_sum = 0
    for val in val_link:
        sum_ = sum_ + float(val)
        squa_sum = squa_sum + float(val) * float(val)
    print ('%s\t%s\t%s\t%s' % (key, str(sum_),str(sum_ * sum_), str(squa_sum)))

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
  
