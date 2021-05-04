#!/usr/bin/env bash
module load python/gcc/3.7.9
set -o xtrace
set -e
pip install -r requirements.txt

export usersize=1000000
export citysize=2000
export bouncedatasize=50000

rm bounced_data/*
rm transaction_data/*
#python user_master.py $usersize && python city_master.py $citysize && python city_hotel_mapping.py $citysize &&python bounced_data.py $bouncedatasize && python transaction_data.py

#python city_master.py $citysize && python city_hotel_mapping.py $citysize && python bounced_data.py $bouncedatasize && python transaction_data.py

python bounced_data.py $bouncedatasize && python transaction_data.py


hdfs dfs -put transaction_data/* /user/ak8257/Pegasus/transaction_data/
hdfs dfs -put bounced_data/* /user/ak8257/Pegasus/bounced_data/
hdfs dfs -put other_data/* /user/ak8257/Pegasus/other_data/


