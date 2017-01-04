import numpy as np
import pandas as pd

test = pd.read_csv('../input/clicks_test.csv')      
output = pd.read_csv('sub_proba.csv')

test = test.merge(output,how='left')
test['clicked'].fillna(test['clicked'].median(),inplace=True)
test = test.sort_values(['display_id','clicked'],ascending=False)
test = test.groupby('display_id')['ad_id'].apply(lambda x:' '.join(map(str,x))).reset_index()
test.to_csv('FTRL_submission_median_imputed.csv',index=False)
