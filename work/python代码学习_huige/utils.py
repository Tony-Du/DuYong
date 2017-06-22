#!/usr/bin/env python
#encoding=utf8

'''
##############################################################################
#版权信息：Copyright (c) 2016 Aspire.Co.Ltd. All rights reserved.
#文件名称：utils.py
#版 本 号：v0.9.0
#创 建 者：gongjianhui
#创建日期：2016-08-05
#内容摘要：在python2.7版本开发测试,在python3.X运行可能存在兼容问题
#############################################################################
'''

import sys, os, re, time, logging, datetime, base64, binascii, ConfigParser, urllib, hashlib
from logging.handlers import TimedRotatingFileHandler	# TimedRotatingFileHandler这个模块是满足文件名按时间自动更换的需求，这样就可以保证日志单个文件不会太大
from xml.etree import ElementTree
from Crypto.Cipher import AES

# 日志级别大小关系为：CRITICAL > ERROR > WARNING > INFO > DEBUG > NOTSET，当然也可以自己定义日志级别
# os.path.join(path1[, path2[, ...]])  #把目录和文件名合成一个路径
# 默然情况下python导入文件或者模块的话，他会先在sys.path里找模块的路径。如果没有的话，程序就会报错。
# 
# 



# 获取日志记录对象
def get_logger(log_name, log_level=logging.DEBUG, logfile_path=os.path.join(sys.path[0], 'log')): 
    #logFormatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d][%(levelname)s]%(message)s')
    if not os.path.isdir(logfile_path):		# os.path.isdir(path)  判断路径是否为目录
        os.mkdir(logfile_path)				#只能创建一层目录
		
    log_file = os.path.join(logfile_path, log_name + '.log.' + time.strftime('%Y%m%d'))	
    logFormatter = logging.Formatter('%(asctime)s:[%(levelname)s]%(message)s')
	
    rtHandler = TimedRotatingFileHandler(log_file, when='D', interval=365)	# 创建 TimedRotatingFileHandler 的一个对象
    rtHandler.setLevel(log_level)
    rtHandler.setFormatter(logFormatter)
    rtHandler.suffix = ".%Y%m%d"		# 后缀
	
    consoleHandler = logging.StreamHandler()	# 创建 StreamHandler 的一个对象
    consoleHandler.setLevel(log_level)
    consoleHandler.setFormatter(logFormatter)
    
    logger = logging.getLogger(log_name)
    logger.setLevel(log_level)
    logger.addHandler(rtHandler)
    logger.addHandler(consoleHandler)
    return logger

# 用日期时间字符串str_datetime替换istring中的日期格式字符串 
def replace_datetime2fmt(istring, str_datetime):

    if len(str_datetime) < 1 or str_datetime is None:
        return istring
    try:
        src_str_datetime = str_datetime
        while len(str_datetime) < 8:
            str_datetime = str_datetime + '01'
        str_datetime = str_datetime.ljust(14,'0')		# 用法：string.ljust(s,width[,fillchar]) 填充
        datetime.datetime.strptime(str_datetime, '%Y%m%d%H%M%S')

    except ValueError as ve:
        raise Exception("动态日期时间字符串错误,无法转换!:{0}:{1}".format(src_str_datetime,ve))
 
    def repl(m):
        pstr = str.lower(m.group(1))
        if not pstr in ['%yyyy%','%mm%','%dd%','%hh%','%mi%', '%ss%']:
            raise Exception('动态日期模式字符串错误:{0}'.format(pstr))
        pstr = pstr.replace('%yyyy%', str_datetime[0:4])
        pstr = pstr.replace('%mm%', str_datetime[4:6])
        pstr = pstr.replace('%dd%', str_datetime[6:8])
        pstr = pstr.replace('%hh%', str_datetime[8:10])
        pstr = pstr.replace('%mi%', str_datetime[10:12])
        pstr = pstr.replace('%ss%', str_datetime[12:14])

        return pstr
    return re.sub(r"(%[ymdhis]+%)", repl, istring, flags=re.IGNORECASE)

# 编码字符串为base64
def encrypt_base64(src_str):
    return base64.encodestring(src_str)	

# 解码base64编码的字符串
def decrypt_base64(encrypt_str):
    # 解码的字符必需为4的倍数,去掉
    encode_str = encrypt_str[:int(len(encrypt_str)/4)*4]	
    return urllib.unquote(base64.decodestring(encode_str))

# 以CBC模式进行的AES加密，并转换为16进制的字符串
def encrypt_aes(key, src_str):
    cryptor = AES.new(key, AES.MODE_CBC, key)
    # 密钥key 长度必须为16（AES-128）、24（AES-192）、或32（AES-256）Bytes 长度
    key_len = 16
    # 加密文本text必须为16的倍数,不是则用'\0'补足
    src_str = src_str + ('\0'*(key_len - len(src_str)%key_len))
    ciphertext = cryptor.encrypt(src_str)
    return binascii.b2a_hex(ciphertext)

# 解密以CBC模式进行的AES加密，并转换为16进制的字符串
def decrypt_aes(key, encrypt_str):
    cryptor = AES.new(key, AES.MODE_CBC, key)
    src_str = cryptor.decrypt(binascii.a2b_hex(encrypt_str))
    #解密后去掉补足的'\0'
    return src_str.rstrip('\0')

#解析三层类似结构为"<root><server><task></task><task></task></server></root>"的xml
def parse_xml_cfgs(xml_cfg_file):
    xml_cfgs = []
    try:
        xml_root = ElementTree.parse(xml_cfg_file)
        
        for server in xml_root.getroot():
            server_cfg = server.attrib
            task_list = []
            for task in server.getchildren():
                task_cfg = dict(task.attrib, **{x.tag:(x.text if x.text else '' ).strip() for x in task.getchildren()})
                task_list.append(task_cfg)
            server_cfg["task"] = task_list
            xml_cfgs.append(server_cfg)

        return xml_cfgs

    except Exception as ex:
        raise Exception("读取配置文件'{0}'异常:{1}".format(xml_cfg_file,ex))

#读取ini类文件cfg_file中section_name片段的键值对
def parse_ini_cfgs(cfg_file, section_name):
    try:
        cfg = ConfigParser.ConfigParser()
        cfg.read(cfg_file)
        return dict(cfg.items(section_name))
    except Exception as ex:
        raise Exception("从文件'{0}'读取配置块'{1}'出错:{2}".format(cfg_file, section_name, str(ex)))

def get_file_hash(file):
    hash =hashlib.md5()
    try:
        with open(file, 'rb') as file_handler:
            for line in file_handler:
                hash.update(line)
        return hash.hexdigest()
    except Exception as ex:
        raise Exception("计算文件'{0}'的MD5值异常:{1}".format(file, str(ex)))


if "__main__" == __name__:
    try:
        src_string = 'xuser/xuser@local'
        #enc_str = encrypt_aes('useruseruseruser', src_string)
        enc_str = encrypt_base64(src_string)
        print enc_str
    
        #print decrypt_aes('useruseruseruser', enc_str)
        print decrypt_base64('Z1UlMjM2ZV9nRTBD')
         
        #print get_file_hash('D:\datafile\\temp\\20161021\\am_W_USER_GROUP_20160926x.verf')
        #ffb7def8f7a58f8610e47ed8099c714a
        print replace_datetime2fmt('%YYYY%%MM%%DD%%HH%', '20161' )
    finally:
        None
					
		
# TimedRotatingFileHandler的构造函数定义如下（2.5版本API为例）：
# TimedRotatingFileHandler(filename [,when [,interval [,backupCount]]])
# filename 是输出日志文件名的前缀
# when 是一个字符串的定义如下：
# “S”: Seconds
# “M”: Minutes
# “H”: Hours
# “D”: Days
# “W”: Week day (0=Monday)
# “midnight”: Roll over at midnight
# interval 是指等待多少个单位when的时间后，Logger会自动重建文件，当然，这个文件的创建
# 取决于filename+suffix，若这个文件跟之前的文件有重名，则会自动覆盖掉以前的文件，所以
# 有些情况suffix要定义的不能因为when而重复。
# backupCount 是保留日志个数。默认的0是不会自动删除掉日志。若设10，则在文件的创建过程中
# 库会判断是否有超过这个10，若超过，则会从最先创建的开始删除。