#coding:utf-8

import pymongo
import datetime
import numpy as np
from drama import get_tv_station_ranks_from_db,find_init_rank, get_trans
from rank_aggr_annealing import annealing

def get_tv_station_rank(today, websites, tv_station_list,tv_coll, varieties_coll, station_rank_coll):
    ranks = get_tv_station_ranks_from_db(websites, tv_station_list, tv_coll, varieties_coll)
    init_rank = find_init_rank(ranks)
    print "init station rank:", init_rank
    new_rank, new_tau = annealing(ranks=ranks, cur_rank=init_rank, temperature_begin=300, temperature_end=0.1,
                                  cooling_factor=.95, nb_iterations=200)
    print "aggregated station rank:", new_rank, new_tau

    # 将排名存入数据库
    get_trans(new_rank, tv_station_list, station_rank_coll, today)
    print "#################每周电视台排名已完成存储###############"

