"""helper functions for system evals"""

import numpy as np
import pandas as pd
from index_helpers import (Query)

index_wd= '/Users/nikhilarora/data/latimes/index_dir_baseline'
query = Query(index_wd)
docno = 'LA030690-0168'
print(query.docno_to_metadata(docno).doc_len)

def get_rank(ind):
    return ind + 1

def get_rel_vect(ind, topic_df):
    if len(topic_df) == 0:
        return 0
    rel_vect = list(topic_df[:ind+1]['relevance'])
    return rel_vect

def get_doc_len_vect(docno_vect):
    # searchup each document no and return workcount
    doc_len_vect = []
    for docno in docno_vect:
        doc_len_vect.append(query.docno_to_metadata(docno).doc_len)
    return doc_len_vect

def get_precision_at_k(rel_vect, k=10):
    rel_vect = rel_vect[:k]
    return sum(rel_vect)/k

def get_avg_precision_k(rel_vect, R, k=1000):
    rel_vect = rel_vect[:k]
    summed_rel = []
    prev_sum = 0
    rank_vect = list(range(1,len(rel_vect)+1))
    for i, rel in enumerate(rel_vect):
        if i == 0:
            summed_rel.append(rel)
            prev_sum += rel
            continue
        elif rel == 1:
            prev_sum += rel
            summed_rel.append(prev_sum)
        elif rel == 0:
            summed_rel.append(0)
    ap_at_k = (1/R)*sum(np.array(summed_rel)/np.array(rank_vect))
    return ap_at_k

def get_ndcg_k(rel_vect, k):
    if len(rel_vect) == 0:
        return 0
    rel_vect = rel_vect[:k]
    i_rel_vect = list(reversed(sorted(rel_vect)))
    dcg = 0
    idcg = 0
    for i in range(1,len(rel_vect)+1):
        dcg += rel_vect[i-1]/np.log2(i+1)
        idcg += i_rel_vect[i-1]/np.log2(i+1)
    return dcg/idcg

def get_tbg_k(rel_vect, doc_len_vect, k):
    rel_vect = rel_vect[:k]
    doc_len_vect = doc_len_vect[:k]
    h = 224
    TBG = 0
    for i in range(1, len(rel_vect)):
        g_k = rel_vect[i-1]*0.64*0.77
        t = 0
        for j in range(1,i):
            if rel_vect[j] == 1:
                p=0.64
            else:
                p=0.39
            t += 4.4 + (0.018*doc_len_vect[j] + 7.8)*p

        d_t = np.exp(-t*(np.log(2)/h))

        TBG += g_k*d_t
    return TBG


#print(get_tbg_k( [1,1,0,1,0,0,0], [50, 75, 45, 50, 30], 5 ))
assert(get_ndcg_k([1,1,0,1,0,0,0], 5) < 1)
assert(get_precision_at_k([1,1,0,1,0], 5) == 3/5)
assert(get_avg_precision_k([0,1,0,0,1,1,1,0,0,0], 5, 10) == 69/175)
assert(get_avg_precision_k([0,1,0,0,1,1,1], 5, 10) == 69/175)
assert(get_avg_precision_k([0,1,0,0,1,1,1,0,0,0,0,0,0,0,0], 5, 10) == 69/175)
