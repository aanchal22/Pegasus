#!/usr/bin/env bash
module load python/gcc/3.7.9
set -o xtrace
set -e
pip install -r requirements.txt

usersize=1000000
citysize=2000
bouncedatasize=50000
noise=10

export usersize
export citysize
export bouncedatasize
export noise

rm -rf bounced_data_noise/*
rm -rf transacted_data_noise/*
#mkdir transacted_data_noise
#mkdir bounced_data_noise
python bounced_data_noise.py $bouncedatasize $noise && python transaction_data_noise.py $noise

hdfs dfs -put transacted_data_noise/* /user/ak8257/Pegasus/transacted_data_noise/
hdfs dfs -put bounced_data_noise/* /user/ak8257/Pegasus/bounced_data_noise/
