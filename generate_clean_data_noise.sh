#! /usr/bin/env bash
# module load python/gcc/3.7.9
set -o xtrace
set -e
pip3 install -r requirements.txt

usersize=1000000
citysize=2000
bouncedatasize=50000
noise=10

export usersize
export citysize
export bouncedatasize
export noise

# mkdir bounced_data_n/
# mkdir other_data_n/
# mkdir transaction_data_n/

# rm bounced_data_n/*
# rm other_data_n/*
# rm transaction_data_n/*
# python3 user_master_noise.py $usersize $noise && python3 city_master_noise.py $citysize $noise && python3 city_hotel_mapping_noise.py $citysize $noise && 
python3 bounced_data_noise.py $bouncedatasize $noise && python3 transaction_data_noise.py $noise

#python city_master.py $citysize && python city_hotel_mapping.py $citysize && python bounced_data.py $bouncedatasize && python transaction_data.py

#python bounced_data.py $bouncedatasize && python transaction_data.py