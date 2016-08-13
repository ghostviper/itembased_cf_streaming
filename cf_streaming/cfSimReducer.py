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
def eurl(n1_val, n2_val, dot_val):
    sim = math.sqrt(n1_val + n2_val - 2.0 * dot_val)
    return 1.0 / (sim + 1)
def loglikelihood_rating(dotij, ni, nj, N):
    p1 = 1.0 * dotij / N
    p2 = 1.0 * (ni - dotij) / N
    p3 = 1.0 * (nj - dotij) / N
    p4 = 1.0 * (N - ni - nj + dotij ) / N
    try:
		t1 = -p1 * math.log(p1, 2)
	except Exception:
		t1 = 0
	try:
		t2 = -p2 * math.log(p2, 2)
	except Exception:
		t2 = 0
	try:
		t3 = -p3 * math.log(p3, 2)
	except Exception:
		t3 = 0	
	try:
		t4 = -p4 * math.log(p4, 2)
	except Exception:
		t4 = 0
    H = t1 + t2 + t3 + t4
   
    #-------------------------------------------------------
    p1 = 1.0 * nj / N
    p2 = 1.0 * (N - nj) / N
    try:
		t1 = -p1 * math.log(p1, 2)
	except:
		t1 = 0
	try:
		t2 = -p2 * math.log(p2, 2)
	except Exception:
		t2 = 0 
    H1 = t1 + t2
    
    p1 = 1.0 * ni / N
    p2 = 1.0 * (N - ni) / N
    try:
		t1 = -p1 * math.log(p1, 2)
	except:
		t1 = 0
	try:
		t2 = -p2 * math.log(p2, 2)
	except Exception:
		t2 = 0 
    H2 = t1 + t2
    if - H + H1 + H2 > 0:
        return 2.0*(- H + H1 + H2)
    else:
        return 0
def jacc(n1_val, n2_val, dot_val):
    return 1.0 * dot_val / (n1_val + n2_val - dot_val)
    
def manh(n1_val, n2_val, dot_val):
    return n1_val + n2_val - 2.0 * dot_val

def simCal(key, val_link, method='eurl'):
    
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
        if method == 'eurl':
            sim = eurl(n1_val, n2_val, dot_val)
        elif method == 'logl':
            sim = loglikelihood_rating(dot_val, n1_val, n2_val, N)
        elif method == 'jacc':
            sim = jacc(n1_val, n2_val, dot_val)
        elif method == 'manh':
            sim = manh(n1_val, n2_val, dot_val)
        if sim <= 0: return   
        print ('%s\t%s\t%s' % (sku1, sku2, str(sim)))
        print ('%s\t%s\t%s' % (sku2, sku1, str(sim)))
 
try:
    method = sys.argv[1]
    N = int(sys.argv[2])
    f = sys.argv[3]
except Exception:
    sys.stderr.write('can not get parameters infos.')
    exit(-1)

norm_map = []
#norm_map.append([0,0])
for line in file(f,'r'):
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
        simCal(last_key, val_link, method)

        sku_link = []
        val_link = []
        val_link.append(val)
        last_key = key
    else:
        last_key = key
        val_link.append(val)
if last_key:
    simCal(last_key, val_link, method)
  
