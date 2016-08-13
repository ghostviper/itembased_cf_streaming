#!/usr/bin/env python
import sys
import math

def paser(val_link):
    v = []
    for line in val_link:
        ele = line.strip().split('#')
        v.append([float(ele[0]), float(ele[1])])
    return v
    
def dot(v):
    d = 0.0
    for vi in v:
        vii = vi.split('#')
        d = d + float(vii[0]) * float(vii[1])
    return d
    
def simCal(key, val_link, method='euro'):
    dot_val = dot(val_link)
    e = key.strip().split('#')
    sku1 = e[0]
    sku2 = e[1]
    print ('%s\t%s\t%s' % (sku1, sku2, str(dot_val)))
    print ('%s\t%s\t%s' % (sku2, sku1, str(dot_val)))

        
(last_key, val) = (None, 0)
sku_link = []
val_link = []
#for line in sys.stdin:
for line in sys.stdin:
    try:
        (key, val) = line.strip().split()
    except Exception:
        continue
    if last_key and last_key != key:
        simCal(last_key, val_link)

        sku_link = []
        val_link = []
        val_link.append(val)
        last_key = key
    else:
        last_key = key
        val_link.append(val)
if last_key:
    simCal(last_key, val_link)
  
