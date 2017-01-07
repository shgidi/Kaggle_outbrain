
data_path = ""

import csv
from datetime import datetime
from csv import DictReader
from math import exp, log, sqrt
import pickle
import time

def read_content_dict():
    print("Content..")
    with open(data_path + "promoted_content.csv") as infile:
        prcont = csv.reader(infile)
        #prcont_header = (prcont.next())[1:]
        prcont_header = next(prcont)[1:]
        prcont_dict = {}
        for ind,row in enumerate(prcont):
            prcont_dict[int(row[0])] = row[1:]
            if ind%100000 == 0:
                print(ind)

        print(len(prcont_dict))
    del prcont
    return prcont_header,prcont_dict

def read_events_to_dict():
    print("Events..")
    with open(data_path + "events_uuid_count.csv") as infile:
        events = csv.reader(infile)
        #events.next()
        next(events)
        event_dict={}
        event_header = ['uuid', 'document_id', 'platform', 'geo_location', 'loc_country','uid_count', 'loc_state', 'loc_dma']
        
        for ind,row in enumerate(events):
           
            tlist = row[1:3] + row[4:7]
            loc = row[5].split('>')
            if len(loc) == 3:
                tlist.extend(loc[:])
            elif len(loc) == 2:
                tlist.extend( loc[:]+[''])
            elif len(loc) == 1:
                tlist.extend( loc[:]+['',''])
            else:
                tlist.append(['','',''])	
            event_dict[int(row[0])] = tlist[:] 
            if ind%500000 == 0:
                print("Events : ", ind)
            if early_break:
                if ind==10000:
                    break
        print(len(event_dict))
    del events
    
    #save dict to disk
        
    return event_header,event_dict


def build_libffm_format(event_header,event_dict,prcont_header,prcont_dict):
    row_counter=1
    D=2**23
    field_id=0 #init

    x = {}
    for i in range(10):
        x[i]=[]

    for t, row in enumerate(DictReader(open("clicks_train.csv"))):
        #print t,row
        row_counter+=1
        y = 0
        disp_id=int(row['display_id'])
        i=int(disp_id) % 10
        if i<files_count:
            ad_id=int(row['ad_id'])
            field_list=['uuid','ad_id','documnet_id','platform']
            field_id=0       
            if 'clicked' in row:
                if row['clicked'] == '1':
                    y = 1
                del row['clicked']

            st = str(y)

            #clicks
            for ind,key in enumerate(row):
                if key in field_list:
                    st += ' '+str(field_id)+':'+str(abs(hash(key + '_' + row[key])) % D)+':1'
                    field_id+=1

            #events
            row = event_dict.get(disp_id, [])
            disp_doc_id = -1
            #print row,enumerate(row)
            for ind, val in enumerate(row):
                if ind in [0,1,2,6]: #uuid #,document_id (from), platform
                    '''if row[4]=='1':
                        val='0' #code all 1 time users with 1# no uuid experiment -not a good idea'''

                    #print ind,row,val,event_header[ind]
                    st+=' '+str(field_id)+':'+str(abs(hash(event_header[ind] + '_' + val)) % D)+':1'
                    #print val,row
                    field_id+=1

            #contnet
            row = prcont_dict.get(ad_id, [])
            for ind, val in enumerate(row):
                #print ind,val
                if ind in [0]: #document to
                    st+=' '+str(field_id)+':'+str(abs(hash(prcont_header[ind] + '_' + val)) % D)+':1'
                    field_id+=1

            #print st


            x[i].append(st)

            if row_counter % 100000==0:
                print 'rows',row_counter,st
                if early_break:
                    break
    return x

def output_to_files(x):
    for i in range(files_count):
        with open('/datadrive/libffm_data/'+output_name+str(i), 'w') as outfile:
            for row in x[i]:
                outfile.write('%s\n' % row)
            
###if '__name'==__main__
#globals
early_break=0
read_events_from_disk=0
output_name='libffm_format_4feats_'
files_count=2

prcont_header,prcont_dict=read_content_dict()
event_header,event_dict=read_events_to_dict()
    
data=build_libffm_format(event_header,event_dict,prcont_header,prcont_dict)
output_to_files(data)
