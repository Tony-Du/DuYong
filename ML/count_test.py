

#逻辑：遍历时区的过程中将计数值保存在字典中
def get_counts(sequence):
    counts = {}
    for x in sequence:
        if x in counts:
            counts[x] += 1
        else:
            counts[x] = 1
    return counts
 
 
#版本2 
from collections import defaultdict

def get_counts2(sequence):
    counts = defaultdict(int) #所有的值均会被初始化为0
    for x in sequence:
        counts[x] += 1
    return counts

    

    
#得到前10名的时区及其计数值, 传进去一个 字典
def top_counts(count_dict, n=10):
    value_key_pairs = [(count, tz) for tz, count in count_dict.items()]
    value_key_pairs.sort()      #从小到大排（没有验证）
    return value_key_pairs[-n:]
    
#version2    
from collections import Counter
counts = Counter(time_zones)  # time_zones 是一个序列
counts.most_common(10)    
    


http://www.grouplens.org/node/73    

