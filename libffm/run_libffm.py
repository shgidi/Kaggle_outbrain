
# coding: utf-8

# In[46]:

import pandas as pd
import time
import subprocess
model_name='/datadrive/model'
files_path='/datadrive/libffm_data/'
files_name='libffm_format_full_'
test_file_index=1
train_file=files_path+files_name+'0'
test_file=files_path+files_name+str(test_file_index)


# In[9]:

#split the validation set from training...

df_train = pd.read_csv("clicks_train.csv") 
df_train.head()

df_train_1=df_train[df_train.display_id % 10 ==test_file_index]
df_train_1=df_train_1.reset_index()


# In[13]:

#run the file
subprocess.check_output(['sudo','pypy','libffm_v2.py'])


# In[47]:

#train model

model_param_tags={'regularization':'-l','model_depth':'-k','epcohs':'-t','learn_rate':'-r','cores':'-s',                 'validation_file':'-p'}
model_params={'regularization':'0.002','model_depth':'16','epcohs':'13','learn_rate':'0.1','cores':'12'}

t=time.time()
#train
results=subprocess.check_output(['sudo','../../libffm/ffm-train','-l','0.002','-k','16','-t','15','-r','0.1','-s','12'                        ,'-p',test_file,'--auto-stop',train_file,model_name])


print 'model trained in',t-time.time()
print results
#sudo ../ libffm_format_feats_0 /datadrive/model_3 /datadrive/output
#../../../libffm/ffm-train -l 0.002 -k 16 -t 13 -r 0.1 -s 12 -p libffm_format_feats_0 libffm_format_feats_1 /datadrive/model


# In[ ]:

#grid search
k_s=[8,16,24]
r_s=[0.05,0.1,0.2]


# In[ ]:


#predict
t=time.time()
results=subprocess.check_output(['sudo','../../libffm/ffm-predict','/datadrive/libffm_data/libffm_format_test_0',model_name,'/datadrive/output'])
print 'model predicted in',time.time()-t
print results


# In[ ]:

#for submission
df_train_1=pd.read_csv('clicks_test.csv')


# In[ ]:

#read output

#ndont forget to reset index if necesary!


output_list=pd.read_csv('/datadrive/output',header=None)

print len(output_list),len(df_train_1)

test_set=df_train_1[['display_id','ad_id','clicked']]#
test_set['prediction']=output_list[0]
#join to test

# sort test set
#test_set=test_set.sort(['display_id','prediction'],ascending=[True,False])
test_set['prediction'].fillna(0)
test_set=test_set.rename(columns = {'prediction':'likelihood'})
#test_set.head()


# In[30]:

#test_set['likelihood']=output_list
test_set.tail(2) #verify its not Nan :{}


# In[45]:

import util
#calc mapk on validation set
util.fast_mapk(test_set) #columns: display id, likelihood,clicked
#0.483445906214
#0.576126738888
# 2feats 0.604136968694  ^24
# 3 feats logloss 0.45885 ^23 map ?
# 4 feats 0.44571 0.634
# 6 feats 0.43375,train_time=7min,predict_time=5min map=0.6534
#6,full data, 0.40, 15min train,


# In[ ]:

#make a submission
first=1
f = open('submission','w')
for item in test_set:
    if first or item['display_id']<>pred_dis_id:
        f.write('\n')
        f.write(str(item['display_id'])+',')
    first=0
    pred_dis_id=item['display_id']    
    f.write(str(item['ad_id'])+' ') 
f.close()
