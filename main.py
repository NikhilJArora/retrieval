""" Author: Nikhil Arora
Usage:
 >> python main.py
 The prompt will provide remaining usage instructions.
Program starts up loading the inverted index (baseline) and provides an
interactive retrieval experience to the user.

To Submit: (info on how to screen cap on assign handout)
1. screen capture of running retrieval on topic 427 (UV damage, eyes)
2. Demonstrate program meets requirements below using multiple queries and
showing results.  Also include how the program can handle a N and Q at the end.
3. Written explanation of how query-biased summaries are computed...
"""
import os
import sys
import pickle
import numpy as np
import gzip
from index_helpers import MetaData, timing, Query, LightDocParser
import operator
from collections import Counter

@timing
def run_query(query_str, query):
    """Runs a query against previously built index.
    """
    doc_scores = {}
    print("Processing your Query: {}".format(str(query_str)))
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
    sorted_doc_scores = sorted_doc_scores[0:10]
    rank = 0
    for docid, score in sorted_doc_scores:
        doc_meta = query.docid_to_metadata(docid)
        dom_parser = LightDocParser(doc_meta.raw_doc)
        rank += 1
        query_snippet = dom_parser.cont_dict['TEXT'].strip()[0:200]
        query_snippet = query_snippet + '...'
        headline = doc_meta.hl
        if headline == "" or headline == None:
            headline = query_snippet[0:50]
        date = doc_meta.date
        docno = doc_meta.docno.strip()
        print(query_res_str.format(
                rank=rank,
                headline=headline,
                date=date,
                query_snippet=query_snippet,
                docno=docno
            ))

query_res_str=\
'''
{rank}. {headline}; {date}
{query_snippet}
({docno})
'''


"""Main function runs on program exectution."""
def main():
    # Index dir path points to a previously generated (baseline) index:
    index_wd = '/Users/nikhilarora/data/latimes/index_dir_baseline'

    query = Query(index_wd) # query object to hold ref to required tools

    while True:
        query_str = input("Please enter a query: ")
        query_str = query_str.strip()
        run_query(query_str, query)
        while True:
            u_in = input("Press N to enter a new query or Q to quit the program.")
            u_in = u_in.lower().strip()
            if u_in == 'n':
                break
            elif u_in == 'q':
                sys.exit()
            else:
                continue



if __name__ == '__main__':
    main()
