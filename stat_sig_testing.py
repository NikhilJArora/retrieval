"""
Code used to compute T-test
"""
from scipy import stats
import pandas as pd
metrics_input_dir = '/Users/nikhilarora/data/latimes/a3/outputs/'
# ap - student1 vs student5
st1_df = pd.read_csv(os.path.join(metrics_input_dir, 'student1_metrics.csv'))
st5_df = pd.read_csv(os.path.join(metrics_input_dir, 'student5_metrics.csv'))

ap_pval = stats.ttest_rel(st1_df['ap'],st5_df['ap']).pvalue
print(ap_pval)

# p@10 - student1 vs student8
st1_df = pd.read_csv(os.path.join(metrics_input_dir, 'student1_metrics.csv'))
st8_df = pd.read_csv(os.path.join(metrics_input_dir, 'student8_metrics.csv'))

p_10_pval = stats.ttest_rel(st1_df['p@10'],st8_df['p@10']).pvalue
print(p_10_pval)

# ndcg@10 - student8 vs student1

ndcg_10_pval = stats.ttest_rel(st8_df['ndcg@10'], st1_df['ndcg@10']).pvalue
print(ndcg_10_pval)

# ndcg@1000 - student1 vs student8
st1_df = pd.read_csv(os.path.join(metrics_input_dir, 'student1_metrics.csv'))
st8_df = pd.read_csv(os.path.join(metrics_input_dir, 'student8_metrics.csv'))

ndcg_1000_pval = stats.ttest_rel(st1_df['ndcg@10'], st8_df['ndcg@10']).pvalue
print(ndcg_1000_pval)

# ap - student1 vs student5
st1_df = pd.read_csv(os.path.join(metrics_input_dir, 'student1_metrics.csv'))
st5_df = pd.read_csv(os.path.join(metrics_input_dir, 'student5_metrics.csv'))

tbg_pval = stats.ttest_rel(st1_df['tbg'],st5_df['tbg']).pvalue
print(tbg_pval)
