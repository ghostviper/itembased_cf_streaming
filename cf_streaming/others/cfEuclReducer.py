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
    if method == 'euro':
        dot_val = dot(val_link)
        e = key.strip().split('#')
        sku1 = e[0]
        sku2 = e[1]
        try:
            n1_val = norm_map[int(sku1)][1]
            n2_val = norm_map[int(sku2)][1]
        except Exception:
            sys.stderr.write('cat not get norm info in [norm_map]\n')
            exit(-1)
        sim = math.sqrt(n1_val + n2_val - 2.0 * dot_val)
        print ('%s\t%s\t%s' % (sku1, sku2, str(sim)))
        print ('%s\t%s\t%s' % (sku2, sku1, str(sim)))


norm_map = []
#norm_map.append([0,0])
for line in file('norm_sort.txt','r'):
    ele = line.strip().split()
    norm_map.append([int(ele[0]),float(ele[1])])

#if len(norm_map) != norm_map[len(norm_map) - 1][0] + 1:
#    sys.stderr.write('norm data index not equel size')
#    exit(-1)
        
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
  
