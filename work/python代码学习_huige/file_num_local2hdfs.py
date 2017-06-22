 #!/usr/bin/env python
#encoding=utf8

import subprocess, os, re, sys, datetime, shutil	#使用import语句导入模块，import语句语法如下：import module 关键字 模块名
import utils

runlogger = None	#属于utils
putlogger = None	#属于utils

EXIT_SUCCESS = 0   #当前数据周期校验成功返回值
EXIT_FAIL    = 1   #当前数据周期检验失败返回值
VERF_SUCCESS = 10  #某个数据文件校验成功标识

#获取对账文件
def get_files(local_path, file_mode):	#定义函数
    files  = []							#{}表示字典，[]是列表，()是元组
    try:
        files= filter(lambda file: re.match(file_mode, file) and os.path.isfile(local_path+file), os.listdir(local_path))
						  
    except Exception as ex:
        raise Exception("从路径'{0}'获取文件'{1}'列表异常:{2}".format(local_path, file_mode, str(ex)))		#Python2.6和Python3.0中字符串格式化的方法
    
    return files
	
#filter()函数接收一个函数 f 和一个list，返回list
#re.match(pattern, string[, flags]);match() 函数只在字符串的开始位置尝试匹配正则表达式，也就是只报告从位置 0 开始的匹配情况
#os.listdir(local_path):列出指定目录下所有的文件和目录名
#re.match(file_mode, file)：传进来的文件名(file_mode)和指定路径下的文件名(file)是否匹配，并且file是否为文件 	
# str(ex) 把ex转换为字符串格式
	
	
	
# 获取hdfs_path上的文件列表,文件大小
def get_hdfs_files(hdfs_path):
    files = {} 		#字典
    hdfs_cmd = "hadoop dfs -ls {0} | grep ^- ".format(hdfs_path) + " | awk '{print $5,$8}'"
    subp = subprocess.Popen(hdfs_cmd, shell=True ,stdout=subprocess.PIPE)
    stdout, stderr = subp.communicate()
    subp.wait()	#等待子进程结束。设置并返回returncode属性
    
    lines = stdout.split('\n')
    for line in lines[:-1]:
        file_attr = line.split()
        file_name = file_attr[1].split('/')[-1]
        files[file_name] = int(file_attr[0])
    return files

# grep ^- ： 由hadoop dfs -ls 命令列出来的信息如 drwxr-xr-x - hbase hbase 0 2017-05-18 17:33 /hbase	，-代表文件，d代表目录
# (-rwxrwx--x+  3 hive hive   0 2017-03-20 14:41 /user/hive/warehouse/app.db/cpa_h5_event_daily/src_file_day=20170320/000000_0)
# $5 代表文件大小，$8 代表文件的hdfs全路径 	
# hdfs_cmd = "hadoop dfs -ls {0} | grep ^- ".format("/user/hive/warehouse/app.db/cpa_h5_event_daily/src_file_day=20170320") + " | awk '{print $5,$8}'"
# subp = subprocess.Popen(hdfs_cmd, shell=True ,stdout=subprocess.PIPE)
# stdout, stderr = subp.communicate()
# >>> stdout	（执行结果）
#'0 /user/hive/warehouse/app.db/cpa_h5_event_daily/src_file_day=20170320/000000_0\n'
# >>> lines
# ['0 /user/hive/warehouse/app.db/cpa_h5_event_daily/src_file_day=20170320/000000_0', '']
# >>> file_attr
# ['0', '/user/hive/warehouse/app.db/cpa_h5_event_daily/src_file_day=20170320/000000_0']
# >>> file_name = fileaaaa[1].split('/')[-1]
# >>> file_name
# '000000_0'
# >>> files[file_name] = int(fileaaaa[0])
# >>> files
# {'000000_0': 0}

	
def create_hdfs_path(hdfs_path):
    hdfs_cmd = ''
    try:
        hdfs_cmd = "hadoop dfs -mkdir {0}".format(hdfs_path)
        runlogger.info("创建hdfs路径:{0}".format(hdfs_cmd))
        subp = subprocess.Popen(hdfs_cmd, shell=True ,stdout=subprocess.PIPE)
        stdout, stderr = subp.communicate()
        subp.wait()
    except Exception as ex:
        runlogger.warn("创建hdfs路径结果:{0}".format(str(ex)))
 
# 上传文件到hdfs
def put_file2hdfs(local_path, hdfs_path, correct_files):
    put_files = []
    
    hdfs_files = get_hdfs_files(hdfs_path)	#返回的是一个字典（文件名：文件大小）
    # 清理hdfs上的旧数据文件,生成待上传文件列表
    hdfs_cmd = 'hadoop dfs -rm '
    rm_flg   = False
    for file in correct_files:
        if file in hdfs_files.keys() \
           and hdfs_files[file] == os.lstat(local_path + file).st_size:	#如果hdfs上的文件名和大小与本地的一致，则无需上传；
            runlogger.info("hdfs路径'{0}'已经存在相同文件'{1}',无需上传hdfs".format(hdfs_path,file))            
        elif file in hdfs_files.keys():									#如果文件名一致，大小不一致，则逻辑删除hdfs上的原文件,把文件名放在put_files列表中
            hdfs_cmd += " {0}/{1}".format(hdfs_path, file)
            put_files.append(file)
            rm_flg = True
        else:
            put_files.append(file)
            
    if rm_flg:
        try:
            runlogger.info("清理旧数据文件:{0}".format( hdfs_cmd))
            subp = subprocess.Popen(hdfs_cmd, shell=True,stdout=subprocess.PIPE)
            subp.wait()            
        except Exception as ex:
            raise Exception("从hdfs路径'{0}'处理旧文件'{1}'失败:{2}".format(hdfs_path, file, str(ex)))
    
    # 上传文件到hdfs    
    for i in range(1,4):
        if len(put_files) == 0:
            continue
        
        hdfs_cmd = 'hadoop dfs -put '
        for file in put_files:
            hdfs_cmd += "{0}/{1} ".format(local_path, file)
            runlogger.info("准备第{0}次上传数据文件到hdfs:{1}".format(i, file))
        
        hdfs_cmd += hdfs_path
        subp = subprocess.Popen(hdfs_cmd, shell=True,stdout=subprocess.PIPE)
        subp.wait()
        
        hdfs_files = get_hdfs_files(hdfs_path)		#上传操作执行后，再执行一次对比，确保准确上传
        __tmp_put_files = put_files[:]
        for file in __tmp_put_files:
            if os.path.isfile(local_path + file) \
                and os.lstat(local_path + file).st_size == hdfs_files[file]:
                runlogger.info("数据文件{0}已成功上传数hdfs:{1}".format(file, hdfs_path))
                put_files.remove(file)
     
    return put_files
	
# 当我们创建一个以"__"两个下划线开始的方法时，这意味着这个方法不能被重写，它只允许在该类的内部中使用
# "双下划线" 开始的是私有成员，意思是只有类对象自己能访问，连子类对象也不能访问到这个数据

def insert_verf_result(if_code, data_cycle, verf_file, data_file, result_code, result_desc):
    putlogger.info("{0}".format([if_code, data_cycle, verf_file, data_file, result_code, result_desc]))
    
    
def main(run_cfg, file_time, if_code):
    __put_cfg = {x:(utils.replace_datetime2fmt(y, file_time)).strip() for x,y in run_cfg.items()}
	
# 字典的items(), keys(), values()都返回一个list
# 使用 os.sep 的话，os.sep 根据你所处的平台，自动地采用相应的分割符号。
# 举例：
# Linux下一个路径,  /usr/share/python，那么上面的 os.sep 就是 ‘/’
# Windows下一个路径, C:\Users\Public\Desktop, 那么上面的 os.sep 就是 '\'。
    
    runlogger.info("本次校验上传hdfs配置为:{0}".format(__put_cfg))
    
    __local_path      = __put_cfg["local_path"] + os.sep
    __hdfs_path       = __put_cfg["hdfs_path"] + '/'    
    __data_file_mode  = __put_cfg["data_file_mode"]
    __file_num_min    = int(__put_cfg["file_num_min"])
    __file_num_max    = int(__put_cfg["file_num_max"])

    runlogger.info("准备从'{0}'获取模式为'{1}'的文件列表".format(__local_path, __data_file_mode))
    data_files = get_files(__local_path, __data_file_mode)	#获取对账文件
    runlogger.info("从'{0}'获取到'{1}'个文件:{2}".format(__local_path, len(data_files), data_files))
    
    verf_info = "要求最少{0}个,最多{1}个文件;实际{2}个文件".format(__file_num_min, __file_num_max, len(data_files))
    runlogger.info("当前周期'{0}'文件校验结果:{1}".format(file_time, verf_info))
    if not __file_num_min <= len(data_files) <=  __file_num_max:
        insert_verf_result(if_code, file_time, '', '', '40', verf_info)
        return EXIT_FAIL

    #数据文件全部校验成功,上传到hdfs
    runlogger.info("准备上传校验成功的数据文件到hdfs路径'{0}'".format(__hdfs_path))
    create_hdfs_path(__hdfs_path)	#创建hdfs路径
    put_fail_files = put_file2hdfs(__local_path, __hdfs_path, data_files)	#上传文件到hdfs
    runlogger.info("有{0}个校验成功的数据文件上传hdfs失败:{1}".format(len(put_fail_files), put_fail_files))
    if len(put_fail_files) > 0:
        return EXIT_FAIL
    else:
        return EXIT_SUCCESS
    
if "__main__" == __name__:
    try:   
        # 获取传入参数:<配置文件名> <配置块名称section> <文件数据周期>
        # cfg_file, section_name, file_time = '.\cfg\local2hdfs.cfg', 'CMS-20103', '20161021'
        cfg_file, section_name, file_time = sys.argv[1:4]
        
        # 初始化写日志对象
        cfg_file_name = cfg_file[cfg_file.rfind(os.sep)+1:]		# find()返回的是匹配的第一个字符串的位置，而rfind()返回的是匹配的最后一个字符串的位置
        log_name  = cfg_file_name[:cfg_file_name.rfind('.')] if cfg_file_name.rfind('.') > 0 else cfg_file_name		# python三元表达式：为真时的结果 if 判定条件 else 为假时的结果 	
        runlogger = utils.get_logger('run_' + log_name + '_' + section_name)
        putlogger = utils.get_logger('put_' + log_name + '_' + section_name)
        
        runlogger.info("从配置文件'{0}'获取配置'{1}',数据周期为:{2}".format(cfg_file, section_name, file_time))
        __run_cfg = utils.parse_ini_cfgs(cfg_file, section_name)
        
        exit_flg = main(__run_cfg, file_time, section_name)
        
        runlogger.info("配置文件'{0}'的配置'{1}'处理完成,数据周期为:{2}".format(cfg_file, section_name, file_time))
        sys.exit(exit_flg)
    except Exception as ex:
        err_info = "脚本执行失败:{0}".format(str(ex))
        if runlogger is None:
            print "Exception:{0}".format(err_info)
        else:
            runlogger.error(err_info)
        sys.exit(1)

# 在python中，当一个module作为整体被执行时,moduel.__name__的值将是"__main__";而当一个 module被其它module引用时，module.__name__将是module自己的名字		