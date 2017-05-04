#!/bin/bash

local_path="$1"  # /home/hadoop/tony_test
file_name="$2"	 # 000000_0

split -a 6 -d -l 3 ${local_path}/${file_name} ${local_path}/${file_name}-
for file in ${local_path}/${file_name}*; do 
  n=1
  seqnum=`ls ${file} | awk -F "-" -v n=$n '{printf("%06d",$NF+n)}'`
  mv ${file} ${local_path}/${file_name}-${seqnum}.dat
done