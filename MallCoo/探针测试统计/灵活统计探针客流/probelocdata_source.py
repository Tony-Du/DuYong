# -*- coding: utf-8 -*- 

import pandas as pd
import os
import sys
import datetime
import traceback
import time

# hive -e "select mac, mallid, shopid, area, activity, filterrssi, bindtype, floorid from probe.probeinfo" > /apps/tony_test/probeinfo/probeinfo.txt

probeinfo_dir = "/apps/tony_test/probeinfo"
file_name1 = "probeinfo.txt"


imoobox_data_dir = "/apps/probe/imoobox/"
file_name2 = "qmonitor.csv"


save_dir = "/apps/tony_test/probe/probeclean"
save_file_name = "probelocdata.csv"


BASE_DIR = '/qmonitor/data/'

u2l = lambda x: x.replace(':', '').lower()
col_cut = lambda x: str(x).split(' ')[0]
neg = lambda x: -x

def join_csv(partition_dir, rssi_thre, start_time, end_time):

    probeinfo_file = os.path.join(probeinfo_dir, file_name1)    
    #print 'open probeinfo_file: %s' % probeinfo_file
    
    if not os.path.isdir(os.path.join(save_dir, partition_dir)):
            os.mkdir(os.path.join(save_dir, partition_dir))        
    save_file = os.path.join(os.path.join(save_dir, partition_dir), save_file_name)
    print 'result file path : %s' % save_file
    
    try:
        probeinfo = pd.read_csv(
            probeinfo_file,
            names=['probemac', 'mallid', 'shopid', 'area', 'activity', 'filterrssi', 'bindtype', 'floorid'],
            header=None,
            sep='\t')
        
        probeinfo['probemac'] = probeinfo['probemac'].apply(u2l) 
        
        probeinfo.loc[probeinfo['area']=='八吉岛','filterrssi'] = rssi_thre
        
        if partition_dir == datetime.date.today().strftime('%Y%m%d'):       
            print 'open origin dir : %s' % os.path.join(BASE_DIR, partition_dir)
            
            listfile = os.listdir(os.path.join(BASE_DIR, partition_dir))
            if listfile.count('test_records.csv'):
                listfile.remove('test_records.csv')
            if listfile.count('qmonitor.csv'):
                listfile.remove('qmonitor.csv')
                
            imoobox = None
            for file_name in listfile:
                csv_file = os.path.join(os.path.join(BASE_DIR, partition_dir), file_name)
                # print 'open csv file: "%s"'%csv_file

                ori_data = pd.read_csv(
                    csv_file,
                    names=[
                        'ID', 'mac', 'Mode', 'rssi', 'Channel', 'PktType', 'FMgt',
                        'Fctl', 'Fdata', 'FMagic', 'time', 'etime', 'slocal',
                        'elocal', 'probemac', 'scan'
                    ],
                    header=None)

                #Mode>256 delete
                #lower mac
                ori_data['mac'] = ori_data['mac'].apply(u2l)
                ori_data['probemac'] = ori_data['probemac'].apply(u2l)
                #cut scan
                ori_data['scan'] = ori_data['scan'].apply(col_cut)
                #neg data
                ori_data['rssi'] = ori_data['rssi'].apply(neg)
                # csv
                #select "stime","dmac","mac","RSSI","scan" from data where data["Mode"]<256
                ori_data = ori_data.loc[(ori_data["Mode"] < 256),["time", "probemac", "mac", "rssi", "scan"]]
                imoobox = pd.concat([imoobox, ori_data], ignore_index=True)
        else:
            scan_data_file = os.path.join(os.path.join(imoobox_data_dir, partition_dir), file_name2)
            #print 'open scan_data_file: %s' % scan_data_file
            
            imoobox = pd.read_csv(
            scan_data_file,
            names=['time', 'probemac', 'mac', 'rssi', 'scan'],
            header=None)

            
        data = pd.merge(imoobox, probeinfo, on=['probemac'])


        out_data = data.loc[(data["rssi"] != 0) & (data["rssi"] > data["filterrssi"]) & 
                            (data["time"] >= time.mktime(time.strptime(start_time, '%Y%m%d %H:%M:%S'))) & 
                            (data["time"] <= time.mktime(time.strptime(end_time, '%Y%m%d %H:%M:%S'))), 
                            ["time", "mallid","shopid","probemac","mac","rssi","area","activity","bindtype","floorid"]].fillna('\\N')

        out_data.to_csv(
            save_file,
            encoding='utf8',
            index=False,
            header=False,
            index_label=None,
            mode='w+')

        print 'save data to csv file: %s success !' % save_file

    except:
        traceback.print_exc()
        print 'save data to csv file: %s  error!' % save_file
            

if __name__ == '__main__':
    if len(sys.argv) == 5:
        partition_dir = sys.argv[1]
        rssi_thre = int(sys.argv[2])
        s_time = sys.argv[3]
        e_time = sys.argv[4]
        # start_time = "{0} {1}".format(partition_dir, s_time)
        # end_time = "{0} {1}".format(partition_dir, e_time)
        if(len(partition_dir) <> 8) or ('-' in partition_dir):
            print "$s is not a vaild dir, please input date format as 'yyyymmdd'."
            exit
        join_csv(partition_dir,rssi_thre,s_time, e_time)
    else:
        print "you should input 4 parameters: date_partition format as 'yyyymmdd', rssi_threshold, start_time and end_time;"
        print "For example, 20171208 -82 \'20171208 17:25:00\' \'20171208 18:57:00\'"
