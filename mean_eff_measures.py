
"""
compute mean measure values
"""
metric_dict = {'run_name':[], 'mean_ap':[], 'mean_p@10':[], 'mean_ndcg@10':[],
    'mean_ndcg@1000':[], 'mean_tbg':[]}


for i in range(1,15):

    run_name = 'student{}'.format(str(i))
    fname = 'student{}_metrics.csv'.format(str(i))
    try:
        run_metric_df = pd.read_csv(os.path.join(metrics_input_dir, fname))
        metric_dict['run_name'].append(run_name)
        metric_dict['mean_ap'].append(round(run_metric_df['ap'].sum()/\
                                        len(run_metric_df['ap']), 3))
        metric_dict['mean_p@10'].append(round(run_metric_df['p@10'].sum()/\
            len(run_metric_df['p@10']),3))
        metric_dict['mean_ndcg@10'].append(round(run_metric_df['ndcg@10'].sum()/\
            len(run_metric_df['ndcg@10']), 3))
        metric_dict['mean_ndcg@1000'].append(round(run_metric_df['ndcg@1000']\
            .sum()/len(run_metric_df['ndcg@1000']), 3))
        metric_dict['mean_tbg'].append(round(run_metric_df['tbg'].sum()/\
            len(run_metric_df['tbg']), 3))
    except:
        bad_data_msg = "bad format"
        metric_dict['run_name'].append(run_name)
        metric_dict['mean_ap'].append(bad_data_msg)
        metric_dict['mean_p@10'].append(bad_data_msg)
        metric_dict['mean_ndcg@10'].append(bad_data_msg)
        metric_dict['mean_ndcg@1000'].append(bad_data_msg)
        metric_dict['mean_tbg'].append(bad_data_msg)

metrics_df = pd.DataFrame(metric_dict)[['run_name', 'mean_ap', 'mean_p@10', \
    'mean_ndcg@10', 'mean_ndcg@1000','mean_tbg']]
