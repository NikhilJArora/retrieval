"""Program used to perform boolean AND retrieval on a set of queries given:
    - index_wd: path where the index is stored
    - queries_file: path to file filled with queries
    - output_file: location to store result of the set of queries

    Example
    -------
    >> python3 BooleanAND.py /Users/nikhilarora/data/latimes/index_dir_test /Users/nikhilarora/data/latimes/queries.txt /Users/nikhilarora/data/latimes/n5arora-hw2-results.txt

    run for A2Q2 part A:
    >> python3 BooleanAND.py /Users/nikhilarora/data/latimes/index_dir_sample/ /Users/nikhilarora/data/latimes/queries_sample.txt /Users/nikhilarora/data/latimes/n5arora-hw2-res_samples.txt
"""
import os
import sys
import pickle
import gzip
from index_helpers import MetaData, timing, Query

@timing
def BooleanAND(index_wd, queries_file, output_file):
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

    run_tag = 'n5aroraAND'
    main_result= []

    # loop through queries:
    for topid, query_str in queries.items():
        print(str(topid), query_str)
        tokens = query.tokenize(query_str) # tokenize
        print("Tokens: {}".format(str(tokens)))
        term_ids = query.conv_tokens_to_ids(tokens) # lexicon used term->termid
        print("matching ids: {}".format(str(term_ids)))
        res_docids, res_counts = query.BooleanAND(term_ids) # find intersection of termid's
        print("AND result: {}, {}".format(str(res_docids), res_counts))
        for inx, docid in enumerate(res_docids):
            docno = query.docid_to_metadata(docid).docno.strip()
            rank = inx + 1
            score = 10000 - inx*10
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

if __name__ == '__main__':
    BooleanAND(sys.argv[1], sys.argv[2], sys.argv[3])
