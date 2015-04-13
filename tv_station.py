#coding:utf-8

import pymongo
import datetime
import numpy as np
from tv import get_tv_station_ranks_from_db,find_init_rank, get_trans
from rank_aggr_annealing_v3 import annealing

def main():
    tv_station_list = [u'湖南卫视',u'东方卫视',u'安徽卫视',u'浙江卫视',u'北京卫视',
                        u'山东卫视',u'江苏卫视',u'江西卫视',u'河南卫视',u'重庆卫视',
                        u'东南卫视',u'广西卫视',u'四川卫视',u'广东卫视',u'吉林卫视',
                        u'山西卫视',u'云南卫视',u'天津卫视',u'辽宁卫视',u'湖北卫视',
                        u'陕西卫视',u'贵州卫视',u'河北卫视',u'黑龙江卫视',u'宁夏卫视',
                        u'青海卫视',u'甘肃卫视',u'内蒙古卫视',u'新疆卫视',u'深圳卫视']
    db = pymongo.mongo_client.MongoClient(host='127.0.0.1')['tv_media']
    tv_coll = db.tv
    varieties_coll = db.varieties
    station_rank_coll = db.station_rank
    today = datetime.datetime.now()
    ranks = get_tv_station_ranks_from_db(websites, tv_station_list, tv_coll, varieties_coll)
    init_rank = find_init_rank(ranks)
    print "init station rank:", init_rank
    new_rank, new_tau = annealing(ranks=ranks, cur_rank=init_rank, temperature_begin=300, temperature_end=0.1,
                                  cooling_factor=.95, nb_iterations=200)
    print "aggregated station rank:", new_rank, new_tau

    # 将排名存入数据库
    get_trans(new_rank, tv_station_list, station_rank_coll, today)
    print "#################每周电视台排名已完成存储###############"

if __name__ == '__main__':
    main()