"""
System evaluation.

example usage:
```
python system_eval.py --qrel "/Users/nikhilarora/data/latimes/a3/upload-to-learn
        /qrels/LA-only.trec8-401.450.minus416-423-437-444-447.txt"
        --results "/Users/nikhilarora/data/latimes/a3/upload-to-learn/
                    results-files/student1.results"
        --max-results 1000
```

evaluating my systems: baseline
```
python system_eval.py --qrel "/Users/nikhilarora/data/latimes/a3/upload-to-learn\
/qrels/LA-only.trec8-401.450.minus416-423-437-444-447.txt" \
--results "/Users/nikhilarora/data/latimes/n5arora-hw4-bm25-baseline.txt" \
--max-results 1000
```
evaluating my systems: stemmed index
```
python system_eval.py --qrel "/Users/nikhilarora/data/latimes/a3/upload-to-learn\
/qrels/LA-only.trec8-401.450.minus416-423-437-444-447.txt" \
--results "/Users/nikhilarora/data/latimes/n5arora-hw4-bm25-stem.txt" \
--max-results 1000
```
"""
import os
import argparse
import numpy as np
import pandas as pd
from eval_helpers import *

parser = argparse.ArgumentParser(description='todo: insert description')
parser.add_argument('--qrel', required=True, help='Path to qrel')
parser.add_argument('--results', required=True, help='Path to file containing results')
parser.add_argument('--max-results', type=int, required=True, help='')


def main():

    cli = parser.parse_args()
    qrels_f = cli.qrel
    res_f = cli.results
    max_results = cli.max_results
    output_dir = '/Users/nikhilarora/data/latimes/a4/outputs/'


    qrels_headings = ['topic_id', '_', 'docno', 'relevance']
    res_headings = ['topic_id', '_', 'docno', 'rank', 'score', 'run_id']
    qrels_df = pd.read_csv(qrels_f, delimiter=' ', names = qrels_headings)
    r_res_df = pd.read_csv(res_f, delimiter=' ', names = res_headings)
    run_id = r_res_df.loc[0,'run_id']
    r_res_df = r_res_df.sort_values(['topic_id', 'score'], ascending=[True, False])
    topic_ids = list(set(qrels_df['topic_id']))

    # label each result as rel=`1` or non-rel=`0` in `relevance` col
    res_df = pd.merge(
        r_res_df,
        qrels_df,
        how='left',
        on=['topic_id', 'docno'],
        sort=False,
        copy=False
    )\
    .fillna(0)\
    .sort_values(['topic_id', 'score'], ascending=[True, False])\
    .drop(['__x', '__y'], axis=1)

    print(res_df.head())
    print(len(res_df['topic_id']))
    # compute the rel_count dict --> store mapping from topic_id to relevance count
    rel_counts = dict()
    for topic_id, topic_df in qrels_df.groupby('topic_id'):
        rel_counts[topic_id] = len(topic_df[topic_df['relevance'] == 1])


    # what will be done for each topic:
    measures = {}
    measures['topic_id'] = []
    measures['ap'] = []
    measures['p@10'] = []
    measures['ndcg@10'] = []
    measures['ndcg@1000'] = []
    measures['tbg'] = []


    for topic_id, topic_df in res_df.groupby('topic_id'):
        print('Computing measures for topic_id: {}'.format(topic_id))
        topic_df = topic_df.sort_values('score', ascending=False)

        measures['topic_id'].append(topic_id)

        # calc precision at k=10
        measures['p@10'].append(get_precision_at_k(list(topic_df['relevance']),
            10))

        # calc avg precision with k=1000
        measures['ap'].append(
            get_avg_precision_k(list(topic_df['relevance']),
            rel_counts[topic_id], 1000))

        # calc ndcg with k=10
        measures['ndcg@10'].append(get_ndcg_k(list(topic_df['relevance']), 10))

        # calc ndcg with k=1000
        measures['ndcg@1000'].append(get_ndcg_k(list(topic_df['relevance']),
            1000))

        # calc tbg
        doc_len_vect = get_doc_len_vect(list(topic_df['docno']))
        measures['tbg'].append(get_tbg_k(list(topic_df['relevance']),
            doc_len_vect, 1000))

    measures_df = pd.DataFrame(measures).sort_values('topic_id').fillna(0)
    col_order = ['topic_id', 'ap', 'p@10', 'ndcg@10', 'ndcg@1000', 'tbg']
    measures_df = measures_df[col_order]
    print(measures_df)
    measures_df.to_csv(os.path.join(output_dir,
        '{}_metrics.csv'.format(run_id)))



if __name__ == '__main__':
    main()
