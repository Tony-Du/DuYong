# -*- coding: utf-8 -*-  

import pandas as pd
import os
import sys
import datetime
import traceback
import time


probeinfo_dir = "/apps/tony_test/probeinfo"
file_name1 = "probeinfo.txt"

imoobox_data_dir = "/apps/probe/imoobox/"
file_name2 = "qmonitor.csv"

save_dir = "/apps/tony_test/probe/probeclean"
save_file_name = "probelocdata111.csv"

u2l = lambda x: x.replace(':', '').lower()

def join_csv(partition_dir, rssi_thre):
    probeinfo_file = os.path.join(probeinfo_dir, file_name1)
    #print 'open probeinfo_file: %s' % probeinfo_file

    scan_data_file = os.path.join(os.path.join(imoobox_data_dir, partition_dir), file_name2)
    #print 'open scan_data_file: %s' % scan_data_file

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
        
        imoobox = pd.read_csv(
            scan_data_file,
            names=['time', 'probemac', 'mac', 'rssi', 'scan'],
            header=None)

        data = pd.merge(imoobox, probeinfo, on=['probemac'])

        out_data = data.loc[(data["rssi"] != 0) & (data["rssi"] > data['filterrssi']),
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
    if len(sys.argv) == 3:
        dir_name = sys.argv[1]
        rssi_thre = int(sys.argv[2])
        if(len(dir_name) <> 8) or ('-' in dir_name):
            print "$s is not a vaild dir, please input date format as 'yyyymmdd'."
            exit
        join_csv(dir_name,rssi_thre)
    else:
        print "you should input 2 parameters: date_partition format as 'yyyymmdd' and rssi_threshold;"
        print "For example, 20171208 -82"
