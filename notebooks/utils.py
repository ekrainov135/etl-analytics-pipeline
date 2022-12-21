import numpy as np
import pandas as pd
from scipy import stats


def get_smoothed_ctr(clicks, impressions, global_ctr, alpha=5):
    smoothed_ctr = (clicks+alpha*global_ctr) / (impressions+alpha)
    return smoothed_ctr


def get_linearized_likes(clicks, impressions, control_ctr):
    return clicks - control_ctr * impressions


def ab_test_report(a, b, metrics, tests=['ttest', 'mannwhitneyu'], ttest=dict(), mannwhitneyu=dict()):
    ttest_result, mannwhitneyu_result = [], []
    statistic, pvalue = [], []
    
    is_ttest = 'ttest' in tests
    is_mannwhitneyu = 'mannwhitneyu' in tests
    
    if is_ttest:
        if ttest.get('type') is None:
            ttest['type'] = 'ind'
        curr_test_type = ttest.pop('type')
    
    for metric in metrics:
        if is_ttest:
            if curr_test_type == 'ind':
                curr_test = stats.ttest_ind
            elif curr_test_type == 'rel':
                curr_test = stats.ttest_rel
            else:
                pass
            ttest_result.append(curr_test(a[metric], b[metric], **ttest))
            statistic.append(ttest_result[-1].statistic)
            pvalue.append(ttest_result[-1].pvalue)
            
        if is_mannwhitneyu:
            mannwhitneyu_result.append(stats.mannwhitneyu(a[metric], b[metric], **mannwhitneyu))
            statistic.append(mannwhitneyu_result[-1].statistic)
            pvalue.append(mannwhitneyu_result[-1].pvalue)
    
    index = pd.MultiIndex.from_product([metrics, tests])
    ab_test_result = pd.DataFrame({}, columns=['statistic', 'pvalue'], index=index)
    ab_test_result['statistic'] = statistic
    ab_test_result['pvalue'] = pvalue

    return ab_test_result.T


def bootstrap(likes_a, views_a, likes_b, views_b, n=1000):

    poisson_bootstraps_a = stats.poisson(1).rvs(
        (n, len(likes_a))).astype(np.int64)
    poisson_bootstraps_b = stats.poisson(1).rvs(
            (n, len(likes_b))).astype(np.int64)
    
    global_ctr_a = (poisson_bootstraps_a*likes_a).sum(axis=1)/(poisson_bootstraps_a*views_a).sum(axis=1)
    global_ctr_b = (poisson_bootstraps_b*likes_b).sum(axis=1)/(poisson_bootstraps_b*views_b).sum(axis=1)

    return global_ctr_a, global_ctr_b
