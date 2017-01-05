#hashing trick
#libffm_v1
#../../libffm/ffm-train -l 0.002 -k 16 -t 30 -r 0.1 -s 12 -p small_test_frame.csv small_train_frame.csv model
#../../libffm/ffm-predict filename model output
import csv
from datetime import datetime
from csv import DictReader
from math import exp, log, sqrt


k=1
D=2**27
x = {}
for i in range(10):
    x[i]=[]
    
for t, row in enumerate(DictReader(open("clicks_train.csv"))):
    #print t,row
    k+=1
    y = 0

    if 'clicked' in row:
        if row['clicked'] == '1':
            y = 1
        del row['clicked']
    st = str(y)
    
    for i,key in enumerate(row):
        st = st+ ' '+str(i)+':'+str(abs(hash(key + '_' + row[key])) % D)+':1'
    
    i=int(row['display_id']) % 10
    
    x[i].append(st)
    if k % 100000==0:
        print k

for i in range(10):
    with open('libffm_data/libffm_format_'+str(i), 'w') as outfile:
        for row in x[i]:
            outfile.write('%s\n' % row)
        
   
