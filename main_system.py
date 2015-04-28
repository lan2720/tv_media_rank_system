#coding:utf-8

import os
import pymongo
import datetime
from drama import get_drama_rank,get_a_day_tv_list,get_a_week_drama_variety
from variety import get_variety_rank
from tv_station import get_tv_station_rank

BASE_DIR = os.path.dirname(__file__)

def main():
    # websites = [u'土豆', u'搜狐视频', u'华数TV', u'芒果TV', u'优酷', u'爱奇艺', u'腾讯视频', u'乐视网', u'迅雷看看', u'风行网']
    websites = [ u'爱奇艺', u'优酷', u'腾讯视频',u'土豆', u'搜狐视频', u'华数TV', u'芒果TV', u'乐视网', u'迅雷看看', u'风行网']
    tv_station_list = [u'湖南卫视',u'东方卫视',u'安徽卫视',u'浙江卫视',u'北京卫视',
                        u'山东卫视',u'江苏卫视',u'江西卫视',u'河南卫视',u'重庆卫视',
                        u'东南卫视',u'广西卫视',u'四川卫视',u'广东卫视',u'吉林卫视',
                        u'山西卫视',u'云南卫视',u'天津卫视',u'辽宁卫视',u'湖北卫视',
                        u'陕西卫视',u'贵州卫视',u'河北卫视',u'黑龙江卫视',u'宁夏卫视',
                        u'青海卫视',u'甘肃卫视',u'内蒙古卫视',u'新疆卫视',u'深圳卫视']
    db = pymongo.mongo_client.MongoClient(host='202.120.38.146')['tv_media']
    tv_coll = db.tv
    today_drama_coll = db.today_drama
    today_variety_coll = db.today_variety
    dramas_coll = db.dramas
    drama_rank_coll = db.drama_rank
    varieties_coll = db.varieties
    variety_rank_coll = db.variety_rank
    station_rank_coll = db.station_rank
    today = datetime.datetime.now()
    day_of_week = today.strftime('%w')
    if day_of_week == '0':
        day_of_week = '7'
    get_a_day_tv_list(day_of_week,tv_coll,today_drama_coll,today_variety_coll)
    print "###################get today tv lists finished#################"
    if day_of_week == '1':
    	get_a_week_drama_variety()
        print "------------------get a week drama and variety list-----------------"
    drama_to_crawl = open(os.path.join(BASE_DIR,'a_week_drama.txt'),'r').read().decode('utf-8').split(' ')

    #################### 电视剧 ####################
    get_drama_rank(today, websites, drama_to_crawl, dramas_coll, today_drama_coll, drama_rank_coll)

    #################### 综艺 #####################
    get_variety_rank(today,websites, varieties_coll,today_variety_coll,variety_rank_coll)

    #################### 电视台 ####################
    get_tv_station_rank(today, websites, tv_station_list,tv_coll, varieties_coll, station_rank_coll)

if __name__ == '__main__':
    main()
