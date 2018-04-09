"""
Code used to compute BM25 scores on 45 topics.

example output:
```
405 cosmic events
Tokens: ['cosmic']
matching ids: [223387]
Counter({223387: 1})
405 q0 LA070390-0077 1 18.50442154738377 n5aroraBM25STEM
405 q0 LA063089-0071 2 15.947429482094632 n5aroraBM25STEM
405 q0 LA040789-0015 3 14.908228579449307 n5aroraBM25STEM
405 q0 LA082890-0097 4 14.643373243003502 n5aroraBM25STEM
405 q0 LA090290-0038 5 14.63106927876425 n5aroraBM25STEM
...
405 q0 LA072589-0047 18 12.680364234357512 n5aroraBM25STEM
405 q0 LA031190-0041 19 12.668435223943128 n5aroraBM25STEM
405 q0 LA040190-0034 20 12.636454085251295 n5aroraBM25STEM
405 q0 LA050589-0170 21 12.596161809184565 n5aroraBM25STEM
405 q0 LA042089-0179 22 12.583571188723035 n5aroraBM25STEM
405 q0 LA111089-0103 23 12.44462738084027 n5aroraBM25STEM
405 q0 LA012289-0002 24 12.425103989025217 n5aroraBM25STEM
405 q0 LA090690-0093 25 12.413067741233803 n5aroraBM25STEM
405 q0 LA112990-0105 26 12.344603038299962 n5aroraBM25STEM
405 q0 LA010889-0109 27 12.286903063049714 n5aroraBM25STEM
...
```
"""
import os
import sys
import pickle
import numpy as np
import gzip
from index_helpers import MetaData, timing, Query
import operator
from collections import Counter

# setup code:
index_wd = '/Users/nikhilarora/data/latimes/index_dir_baseline'
queries_file = '/Users/nikhilarora/data/latimes/queries.txt'
output_file = '/Users/nikhilarora/data/latimes/n5arora-hw4-bm25-stem.txt'


def run_query(index_wd, queries_file, output_file):
    #parse the queries file:
    with open(queries_file) as f:
        lines = f.read().splitlines()
    queries = {}
    for i, elem in enumerate(lines):
        if i % 2 == 0:
            num = elem
        else:
            queries[num] = elem

    query = Query(index_wd) # create a query object to hold query tools

    run_tag = 'n5aroraBM25STEM'
    main_result= []
    doc_scores = {}
    # loop through queries:
    for topid, query_str in queries.items():
        print(str(topid), query_str)
        tokens = query.tokenize(query_str) # tokenize

        print("Tokens: {}".format(str(tokens)))
        term_ids = query.conv_tokens_to_ids(tokens) # lexicon used term->termid
        print("matching ids: {}".format(str(term_ids)))
        tkn_cts = Counter(term_ids)
        print(tkn_cts)
        postings_lists = query.general_retrieval(term_ids)


        for termid, posting_ls in postings_lists.items():
            r_i = 0 # no rel info known
            R = 0 # no rel info known
            N = query.invIndex.coll_len
            coll_token_sum = query.invIndex.coll_token_sum
            n_i = len(posting_ls)# num of docs containing current term
            for docid, count in posting_ls:
                # compute score...
                k_1 = 1.2
                k_2 = 7
                b = 0.75
                dl = int(query.docid_to_metadata(docid).doc_len)
                avg_dl = coll_token_sum/N
                qf = tkn_cts[termid] # gets to freq of term in query
                df = count # gets freq of term in doc
                K = k_1*((1-b)+(b*(dl/avg_dl)))
                qf_term = ( (k_2+1)*qf) / (k_2+qf)
                df_term = ( (k_1+1)*df) / (K+df)
                idf_term = np.log( ((r_i + 0.5)/(R - r_i + 0.5)) \
                    / ((n_i - r_i + 0.5)/(N - n_i - R - r_i - 0.5)) )
                score = qf_term * df_term * idf_term
                if docid in doc_scores:
                    doc_scores[docid] += score
                else:
                    doc_scores[docid] = score
        sorted_doc_scores = sorted(doc_scores.items(), \
            key=operator.itemgetter(1), reverse=True)
        sorted_doc_scores = sorted_doc_scores[0:1000]
        rank = 0
        for docid, score in sorted_doc_scores:
            docno = query.docid_to_metadata(docid).docno.strip()
            rank += 1
            res_doc_str = "{topid} q0 {docno} {rank} {score} {run_tag}"\
                            .format(
                                topid=topid,
                                docno=docno,
                                rank=rank,
                                score=score,
                                run_tag=run_tag
                            )
            print(res_doc_str)
            main_result.append(res_doc_str)

    with open(output_file, 'w') as wfile:
        for line in main_result:
            wfile.write("{}\n".format(line))
    print("Query complete.")

# kick of ranking:
run_query(index_wd, queries_file, output_file)
