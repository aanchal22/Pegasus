#!/usr/bin/env bash
module load python/gcc/3.7.9
set -o xtrace
set -e
pip install -r requirements.txt

export usersize=1000000
export citysize=2000
export bouncedatasize=2000

rm bounced_data/*
rm transaction_data/*
#python user_master.py $usersize && python city_master.py $citysize && python city_hotel_mapping.py $citysize &&python bounced_data.py $bouncedatasize && python transaction_data.py

#python city_master.py $citysize && python city_hotel_mapping.py $citysize && python bounced_data.py $bouncedatasize && python transaction_data.py

python bounced_data_jugaad.py $bouncedatasize && python bounced_data_jugaad.py $bouncedatasize && python transaction_data.py


hdfs dfs -put transaction_data/* /user/ak8257/Pegasus/transaction_data_test/
hdfs dfs -put bounced_data/* /user/ak8257/Pegasus/bounced_data_test/
#hdfs dfs -put other_data/* /user/ak8257/Pegasus/other_data/


hdfs dfs -setfacl -R -m user:ij2056:rwx /user/ak8257/Pegasus/transaction_data/*
hdfs dfs -setfacl -R -m user:ij2056:rwx /user/ak8257/Pegasus/bounced_data/*
hdfs dfs -setfacl -m user:ij2056:--x /user/ak8257

hdfs dfs -setfacl -R -m user:ru2025:rwx /user/ak8257/Pegasus/transaction_data/*
hdfs dfs -setfacl -R -m user:ru2025:rwx /user/ak8257/Pegasus/bounced_data/*
hdfs dfs -setfacl -m user:ru2025:--x /user/ak8257

hdfs dfs -setfacl -R -m user:ss14396:rwx /user/ak8257/Pegasus/transaction_data/*
hdfs dfs -setfacl -R -m user:ru2025:rwx /user/ak8257/Pegasus/bounced_data/*
hdfs dfs -setfacl -m user:ss14396:--x /user/ak8257

hdfs dfs -setfacl -R -m user:ak8257:rwx /user/ak8257/Pegasus/transaction_data/*
hdfs dfs -setfacl -R -m user:ak8257:rwx /user/ak8257/Pegasus/bounced_data/*
hdfs dfs -setfacl -m user:ak8257:--x /user/ak8257