# coding=UTF-8

import sys
import requests
import socket
import json
import re
import os
import time
import datetime
import pymongo
import random
import numpy as np
from rank_aggr_annealing import annealing
from xml.etree import ElementTree
from pyquery import PyQuery as pq

accept = '*/*'
encoding = ''
language = 'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4'
connection = 'keep-alive'
chrome = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.115 Safari/537.36'
tvs = ['cctv1', 'cctv8', 'anhui', 'btv1', 'chongqing', 'dongfang', 'dongnan', 'guangdong',
       'guangxi', 'gansu', 'guizhou', 'hebei', 'henan', 'heilongjiang', 'hubei', 'hunan', 'jilin', 'jiangsu',
       'jiangxi', 'liaoning', 'neimenggu', 'ningxia', 'qinghai', 'shandong', 'shenzhen',
       'shan3xi', 'shan1xi', 'sichuan', 'tianjin', 'xiamen', 'xinjiang', 'yunnan', 'zhejiang']

repl = ' 0123456789'
pat = re.compile(r'\(.*\)')  # 去除 (重播)

BASE_DIR = os.path.dirname(__file__)

def get_a_day_tv_list(day_of_week, tv_coll, today_drama_coll, today_variety_coll):
    posts = []
    all_drama_today = {}
    all_variety_today = {}
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    with open(os.path.join(BASE_DIR,'qq_hrefs.txt'), 'r') as infile:
        for line in infile:
            dramas = {}  # 每个电视台每天都有这个
            varieties = {}
            url = line.strip().split('\t')[0].format(day_of_week)
            tvname = line.strip().split('\t')[1]
            while True:
                try:
                    ws = requests.get(url, headers={'User-Agent': chrome})
                except (socket.timeout, requests.exceptions.Timeout):  # socket.timeout
                    print "timeout", url
                except requests.exceptions.ConnectionError:
                    print "connection error", url
                else:
                    break
            ws.encoding = 'utf-8'
            root = ElementTree.fromstring(ws.text.encode('utf-8'))
            for node in root.findall('.//P'):
                ctime = node.find('PT').text
                if int(ctime.split(' ')[-1][0:5].replace(':', '')) in range(1930, 2230):
                    name = node.find('PN').text
                    # if u'东南剧苑' in name: # 河南卫视星光剧场播电影
                    # 	continue
                    if u'剧场' in name or u'剧苑' in name:
                        if u"：" not in name or u'精编' in name:
                            continue
                        else:
                            name = name.split(u"：")[-1].strip(repl) # 已经是utf-8
                            name = name.split(' ')[0] # 有的以空格分隔 
                            #name = name#.decode('utf-8')
                            name = re.sub("[\s+\.·\!\/_,$%^*(+\"\'0123456789]+|[+——！，。？、~@#￥%……&*（）]+".decode("utf-8"), "".decode("utf-8"),name) 
                            if name == u'嗨，老头！':
                                name = u'嘿老头'
                            dramas.setdefault(name, None)
                            all_drama_today.setdefault(name, None)
                            print name, 'drama'
                    elif node.find('Fd2').text == u'0':  # 说明不是电视剧 动画片 电影
                        if u'本集' in name or u'花絮' in name or u'新闻' in name or u'焦点' in name \
                                or u'天气' in name or u'抢先看' in name or u'收视' in name or u'中超' in name \
                                or u'报道' in name or u'要闻' in name or u'今日' in name or u'提要' in name \
                                or u'典礼' in name or u'直播' in name or u'东方眼' in name or u'新干线' in name \
                                or u'纪录片' in name or u'气象' in name or u'娱乐' in name or u'看点' in name \
                                or u'歌曲' in name or u'电影' in name or u'影院' in name or u'预告' in name \
                                or u'旅游' in name or u'南粤' in name or u'内蒙古' in name or u'盛典' in name \
                                or u'海峡' in name or u'金穗双联' in name or u'年度人物' in name or u'开幕式' in name \
                                or u'颁奖' in name or u'节目' in name:
                            continue
                        else:
                            name = re.sub(pat, "", name.split(u'：')[0])  # 去除"重播"
                            name = name.split(' ')[0] # 有的以空格分隔 
                            name = re.sub("[\s+\.·\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+".decode("utf-8"), "".decode("utf-8"),name)
                            # name = name.replace(u'·', '')  # 大王·小王
                            varieties.setdefault(name, None)
                            all_variety_today.setdefault(name, None)
                            print name, 'variety'
                    else:
                        pass
            posts.append({'date': today, 'name': tvname, 'drama': dramas.keys(), 'variety': varieties.keys()})
        tv_coll.insert(posts)
        today_drama_coll.update({'date': today}, {'$set': {'dramas': all_drama_today.keys()}}, upsert=True)
        today_variety_coll.update({'date': today}, {'$set': {'varieties': all_variety_today.keys()}}, upsert=True)


def get_a_week_drama_variety():
    dramas = {}
    varieties = {}
    with open(os.path.join(BASE_DIR,'qq_hrefs.txt'), 'r') as infile, open(os.path.join(BASE_DIR,'a_week_drama.txt'), 'w+') as outfile1,open(os.path.join(BASE_DIR,'a_week_variety.txt'),'w+') as outfile2:
        for line in infile:
            for i in range(1, 8):
                url = line.strip().split('\t')[0].format(str(i))
                print url
                tvname = line.strip().split('\t')[1]
                while True:
                    try:
                        ws = requests.get(url, headers={'User-Agent': chrome})
                    except (socket.timeout, requests.exceptions.Timeout):  # socket.timeout
                        print "timeout", url
                    except requests.exceptions.ConnectionError:
                        print "connection error", url
                    else:
                        break 
                ws.encoding = 'utf-8'
                root = ElementTree.fromstring(ws.text.encode('utf-8'))
                for node in root.findall('.//P'):
                    ctime = node.find('PT').text
                    if int(ctime.split(' ')[-1][0:5].replace(':', '')) in range(1930, 2230):
                        name = node.find('PN').text
                        if u'星光剧场' in name or u'东南剧苑' in name:  # 河南卫视星光剧场播电影
                            continue
                        if u'剧场' in name:
                            if u"：" not in name or u'精编' in name:
                                continue
                            else:
                                name = name.split(u"：")[-1].strip(repl)
                                name = name.split(' ')[0] # 有的以空格分隔
                                name = re.sub("[\s+\.·\!\/_,$%^*(+\"\'0123456789]+|[+——！，。？、~@#￥%……&*（）]+".decode("utf-8"), "".decode("utf-8"),name)
                                if name == u'嗨，老头！':
                                    name = u'嘿老头'
                                dramas.setdefault(name, None)
                                print name, "drama"
                        elif node.find('Fd2').text == u'0':  # 说明不是电视剧 动画片 电影
                            if u'本集' in name or u'花絮' in name or u'新闻' in name or u'焦点' in name \
                                    or u'天气' in name or u'抢先看' in name or u'收视' in name or u'中超' in name \
                                    or u'报道' in name or u'要闻' in name or u'今日' in name or u'提要' in name \
                                    or u'典礼' in name or u'直播' in name or u'东方眼' in name or u'新干线' in name \
                                    or u'纪录片' in name or u'气象' in name or u'娱乐' in name or u'看点' in name \
                                    or u'歌曲' in name or u'电影' in name or u'影院' in name or u'预告' in name \
                                    or u'旅游' in name or u'南粤' in name or u'内蒙古' in name or u'盛典' in name \
                                    or u'海峡' in name or u'金穗双联' in name or u'年度' in name or u'开幕式' in name \
                                    or u'颁奖' in name or u'节目' in name:
                                continue
                            else:
                                name = re.sub(pat, "", name.split(u'：')[0])  # 去除"重播"
                                name = name.split(' ')[0] # 有的以空格分隔 
                                # name = name.replace(u'·', '')  # 大王·小王
                                name = re.sub("[\s+\.·\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+".decode("utf-8"), "".decode("utf-8"),name)
                                varieties.setdefault(name, None)
                                print name, "variety"
                        else:
                            pass
        outfile1.write((' '.join(dramas.keys())).encode('utf-8'))
        outfile2.write((' '.join(varieties.keys())).encode('utf-8'))


def search_in_all(drama_name):
    soku_url = 'http://www.soku.com/v'
    data = {'keyword': drama_name}

    result_dict = {}
    for i in range(5):  # soku每次刷新之后链接出来的资源不一样
        while True:
            try:
                soku_search_result = requests.get(soku_url, params=data).text  #.encode('utf-8')
            except (socket.timeout, requests.exceptions.Timeout):
                print "timeout", soku_url
            except requests.exceptions.ConnectionError:
                print "connection error", soku_url
            else:
                break
        for item in pq(soku_search_result)('div.item'):
            if pq(item).children('div').attr('class') != 'tv':
                continue
            year = pq(item)('li.base_pub').text().strip('()')
            v_title = pq(item)('li.base_name')('a').attr('_log_title') + '_' + year  # only have to get once
            result_dict.setdefault(v_title, {})
            for site in pq(item)('div.source')('span'):
                site_name = pq(site).attr('name')  #.encode('utf-8') #网站名字: 优酷 土豆 搜狐视频等
                addr = pq(site)('a').attr('href')
                if site_name not in result_dict[v_title]:
                    result_dict[v_title][site_name] = addr

    iqiyi_url = 'http://so.iqiyi.com/so/q_{}?source=hot&refersource=lib'.format(drama_name.encode('utf-8'))
    while True:
        try:
            iqiyi_search_result = requests.get(iqiyi_url, headers={'Host': 'so.iqiyi.com'}, timeout=2,
                                               allow_redirects=False).text  #.encode('utf-8')
        except (socket.timeout, requests.exceptions.Timeout):
            print "timeout", iqiyi_url
        except requests.exceptions.ConnectionError:
            print "connection error", iqiyi_url
        else:
            break
    for item in pq(iqiyi_search_result)('li.list_item').filter(
            lambda i, this: pq(this).attr('data-widget-searchlist-catageory') == u'电视剧'):
        year = pq(item)('em.fs12').text().strip()
        v_title = pq(item).attr('data-widget-searchlist-tvname') + '_' + year
        for node in pq(item)('em.vm-inline').filter(lambda i, this: pq(this).attr('data-site') == 'iqiyi'):
            site_name = u'爱奇艺'  # 网站名字 中文
            data_doc_id = pq(node).attr('data-doc-id')

            data_url = 'http://so.iqiyi.com/multiEpisode?key={}&platform=web'.format(data_doc_id)
            while True:
                try:
                    link_page = requests.get(data_url, headers={'Host': 'so.iqiyi.com', 'Referer': iqiyi_url},
                                             timeout=2, allow_redirects=False).text  #.encode('utf-8')
                except (socket.timeout, requests.exceptions.Timeout):
                    print "timeout", data_url
                except requests.exceptions.ConnectionError:
                    print "connection error", data_url
                else:
                    break
            link_pat = re.compile(r'(?<=href=\\\"http://).*?(?=\\\")', re.M)
            try:
                video_id = re.finditer(link_pat, link_page).next().group()
            except StopIteration:
                print "link error, it isn't a drama link."
            else:
                src_link = 'http://{}'.format(video_id)
                result_dict.setdefault(v_title, {})
                result_dict[v_title][site_name] = src_link
    return result_dict

def tudou_parser(url):  # 没问题
    lid_pat = re.compile(r'(?<=,lid: )\d+', re.M)
    while True:
        try:
            tudou_page = requests.get(url, headers={'Host': 'www.tudou.com', 'User-Agent': chrome}, timeout=2,
                                      allow_redirects=False).text
            lid = re.search(lid_pat, tudou_page).group()
        except (socket.timeout, requests.exceptions.Timeout):
            print "timeout", url
        except requests.exceptions.ConnectionError:
            print "connection error", url
        except AttributeError:
            print "tudou main page error"
        else:
            break
    # 请求iid_list的页面url需要lid号
    iids_url = 'http://www.tudou.com/tvp/alist.action?jsoncallback=page_play_model_aListModelList__findAll&areaCode=310000&a={}&app=4'.format(lid)
    print iids_url
    while True:
        try:
            iids_page = requests.get(iids_url, headers={'Referer': url, 'User-Agent': chrome}, timeout=2).text
        except (socket.timeout, requests.exceptions.Timeout):
            print "timeout", iids_url
        except requests.exceptions.ConnectionError:
            print "connection error", iids_url
        else:
            break

    # 找到每一集的iid
    iid_pat = re.compile(r'(?<="iid":)\d+(?=,)', re.M)
    count_pat = re.compile(r'(?<="playNum":)\d+(?=,)', re.M)
    playcount = 0
    for iid in re.finditer(iid_pat, iids_page):
        count_url = 'http://www.tudou.com/crp/itemSum.action?jsoncallback=page_play_model_itemSumModel__find&app=4&showArea=true&iabcdefg={}&uabcdefg=331728306&juabcdefg=019el62f2qcbk'.format(
            iid.group())
        while True:
            try:
                count_page = requests.get(count_url,
                                          headers={'Host': 'www.tudou.com', 'User-Agent': chrome, 'Referer': url},
                                          timeout=2).text
            except (socket.timeout, requests.exceptions.Timeout):
                print "timeout", count_url
            except requests.exceptions.ConnectionError:
                print "connection error", count_url
            else:
                break
        playcount += int(re.search(count_pat, count_page).group())

    return playcount


def sohu_parser(url):  # 没问题
    vid_pat = re.compile(r'(?<=vid=")\d+(?=")', re.M)
    plid_pat = re.compile(r'(?<=playlistId=")\d+(?=")', re.M)
    oplid_pat = re.compile(r'(?<=o_playlistId=")\d+(?=")', re.M)
    json_pat = re.compile(r'(?<=plids:).*?(?=,vids)', re.M)

    try_time = 0
    while True:
        try:
            page = requests.get(url, headers={'Host': 'tv.sohu.com', 'User-Agent': chrome}, timeout=2,
                                allow_redirects=False)
            page.encoding = 'utf-8'
            vid = re.search(vid_pat, page.text).group()
            plid = re.search(plid_pat, page.text).group()
            print vid,plid
        except (socket.timeout, requests.exceptions.Timeout):
            print "timeout", url
        except requests.exceptions.ConnectionError:
            print "connection error", url
        except AttributeError:
            try_time += 1
            if try_time == 5:
                return 0
            else:
                print "page error, no vid, plid."
                print url
        else:
            break

    try:
        oplid = re.search(oplid_pat, page.text).group()
    except AttributeError:
        count_url = 'http://count.vrs.sohu.com/count/queryext.action?vids={}&plids={}&callback=playCountVrs'.format(vid,plid)
    else:
        print oplid
        count_url = 'http://count.vrs.sohu.com/count/queryext.action?vids={}&plids={},{}&callback=playCountVrs'.format(vid, plid, oplid)
    try_time = 0
    print count_url
    while True:
        try:
            amount = requests.get(count_url, headers={'Host': 'count.vrs.sohu.com', 'Referer': url}, timeout=2,
                                  allow_redirects=False).text
            playcount = re.search(json_pat, amount).group()
        except (socket.timeout, requests.exceptions.Timeout):
            print "timeout", count_url
        except requests.exceptions.ConnectionError:
            print "connection error", count_url
        except AttributeError:
            try_time += 1
            if try_time == 5:
                return 0
            else:
                print "sohu page response error. try again."
                print count_url
        else:
            break

    d = json.loads(playcount)
    count_sum = 0
    for v in d.itervalues():
        count_sum += v['total']
    return count_sum


def iqiyi_parser(url):  # 没问题
    while True:
        try:
            page = requests.get(url, headers={'Host': 'www.iqiyi.com'}, timeout=2, allow_redirects=False)  # ios-8859-1
        except (socket.timeout, requests.exceptions.Timeout):
            print "timeout", url
        except requests.exceptions.ConnectionError:
            print "connection error", url
        else:
            break
    page.encoding = 'utf-8'

    album_id = pq(page.text)('#videoArea').children('div').attr('data-player-albumid')
    count_url = 'http://cache.video.qiyi.com/jp/pc/{}/'.format(album_id)
    count_pat = re.compile(r'(?<=:)\d+(?=})', re.M)
    try_time = 0
    while True:
        try:
            playcount_page = requests.get(count_url, headers={'Host': 'cache.video.qiyi.com', 'Referer': url},
                                          timeout=2, allow_redirects=False).text
            playcount = re.search(count_pat, playcount_page).group()
        except (socket.timeout, requests.exceptions.Timeout):
            print "timeout", count_url
        except requests.exceptions.ConnectionError:
            print "connection error", count_url
        except AttributeError:
            try_time += 1
            if try_time == 5:
                return 0
            else:
                print "page response error. try again."
                print count_url
        else:
            break

    return int(playcount)


# def hunan_parser(url):  # 没问题
#     while True:
#         try:
#             base_page = requests.get(url, headers={'Host': 'www.hunantv.com', 'User-Agent': chrome}, timeout=2,
#                                      allow_redirects=False)  #.text
#         except (socket.timeout, requests.exceptions.Timeout):
#             print "timeout", url
#         except requests.exceptions.ConnectionError:
#             print "connection error", url
#         else:
#             break
#     base_page.encoding = 'utf-8'

#     click_pat = re.compile(r'(?<="click":").*(?=",)')
#     playcount = 0
#     for each in pq(base_page.text)('#tvplay-box')('li'):
#         vid = pq(each).attr('id').split('-')[-1]
#         print vid
#         data_url = 'http://click.hunantv.com/get.php?callback=jQuery18208550447486341_1426752637357&aid={}&type=videos&_=1426752637423'.format(vid)
#         while True:
#             try:
#                 data_page = requests.get(data_url,
#                                          headers={'Host': 'click.hunantv.com', 'Referer': url, 'User-Agent': chrome},
#                                          timeout=2, allow_redirects=False).text
#             except (socket.timeout, requests.exceptions.Timeout):
#                 print "timeout", data_url
#             except requests.exceptions.ConnectionError:
#                 print "connection error", data_url
#             else:
#                 break
#         count = re.search(click_pat, data_page).group()
#         if count[-1] == u'万':
#             count = int(count[:-1]) * 10000
#         else:
#             count = int(count.replace(',', ''))
#         playcount += count
#     return playcount
def hunan_parser(url): # 改版
    vid  = url.split('/')[-1].split('.')[0]
    print vid
    count_url = 'http://videocenter-2039197532.cn-north-1.elb.amazonaws.com.cn/dynamicinfo?callback=jQuery18206827334458939731_1433925943689&vid={}&_=1433925944085'.format(vid)
    count_pat = re.compile(r'(?<="allVV":)\d+(?=,")', re.M)
    try_time = 0
    while True:
        try:
            data_page = requests.get(count_url,
                                     headers={'Host': 'videocenter-2039197532.cn-north-1.elb.amazonaws.com.cn', 'Referer': url, 'User-Agent': chrome},
                                     timeout=2, allow_redirects=False).text
            # print data_page
            playcount = re.search(count_pat, data_page).group()
        except (socket.timeout, requests.exceptions.Timeout):
            print "timeout", count_url
        except requests.exceptions.ConnectionError:
            print "connection error", count_url
        except AttributeError:
            try_time += 1
            if try_time == 5:
                return 0
            else:
                print "response page error. no playcount found. try again."
                print count_url
        else:
            break
    return int(playcount)

def letv_parser(url):  # 没问题
    pid_pattern = re.compile(r'(?<=pid:)\d+', re.M)
    try_time = 0
    while True:
        try:
            page = requests.get(url, headers={'Host': 'www.letv.com', 'User-Agent': chrome,
                                              'Accept':accept,
                                              'Accept-Encoding':'',
                                              'Accept-Language':language,
                                              'Connection':connection}, timeout=4,
                                allow_redirects=False)  #'ISO-8859-1'
            page.encoding = 'utf-8'
            pid = re.search(pid_pattern, page.text).group()
            print pid
        except (socket.timeout, requests.exceptions.Timeout):
            print "timeout", url
        except requests.exceptions.ConnectionError:
            print "connection error", url
        except AttributeError:
            try_time += 1
            if try_time == 5:
                return 0
            else:
                print "response page error. no pid found. try again"
                print url
        else:
            break

    playcount_pattern = re.compile(r'(?<="plist_play_count":)\d+', re.M)
    json_url = 'http://stat.letv.com/vplay/queryMmsTotalPCount?callback=jQuery171004010079498402774_1426657047954&pid={}'.format(pid)
    try_time = 0
    while True:
        try:
            playcount_data = requests.get(json_url, cookies = page.cookies,
                                         headers={'Host': 'stat.letv.com',
                                                'Referer': url,
                                                'Accept':accept,
                                                'Accept-Encoding':'',
                                                'Accept-Language':language,
                                                'Connection':connection}, timeout=1,
                                        allow_redirects=False).text
            playcount = re.search(playcount_pattern, playcount_data).group()
        except (socket.timeout, requests.exceptions.Timeout):
            print "timeout", json_url
        except requests.exceptions.ConnectionError:
            print "connection error", json_url
        except AttributeError:
            try_time += 1
            if try_time == 5:
                return 0
            else:
                print "response page error. no playcount found. try again"
        else:
            break
    return int(playcount)


def qq_parser(url):  # 没问题
    vid = url.split('/')[-2]
    playmount_url = 'http://sns.video.qq.com/tvideo/fcgi-bin/batchgetplaymount?callback=jQuery1910049597709672525525_1426567859380&low_login=1&id={}&otype=json&_=1426567859381'.format(vid)
    count_pattern = re.compile(r'(?<="node":\[).+(?=])')
    try_time = 0
    while True:
        try:
            qq_json = requests.get(playmount_url, headers={'Host': 'sns.video.qq.com', 'User-Agent': chrome}, timeout=2,
                                   allow_redirects=False)  # raw page is ('ISO-8859-1')
            qq_json.encoding = 'utf-8'  # change IOS-8859-1  to utf-8(unicode)
            amount = re.search(count_pattern, qq_json.text).group()
        except (socket.timeout, requests.exceptions.Timeout):
            print "timeout", playmount_url
        except requests.exceptions.ConnectionError:
            print "connection error", playcount_url
        except AttributeError:
            try_time += 1
            if try_time == 5:
                return 0
            else:
                print "response page error. no acount found. try again"
        else:
            break

    amount = json.loads(amount, encoding='utf-8')
    return int(amount['all'])  #, amount['td_m']


def youku_parser(url):  # something to promotion 没问题
    pattern = re.compile(r'(?<=id_).*?(?=\.html)')
    try:
        vid = re.search(pattern, url).group()
    except AttributeError:
        print "youku vid get error."
        return 0
    else:
        url_get_data = 'http://v.youku.com/v_vpactionInfo/id/{}/pm/3?&__ro=info_stat'.format(vid)
        while True:
            try:
                page = requests.get(url_get_data, headers={'Host': 'v.youku.com', 'Referer': url, 'User-Agent': chrome},
                                    timeout=2, allow_redirects=False).text  #.encode('utf-8')
            except (socket.timeout, requests.exceptions.Timeout):
                print "timeout", url_get_data
            except requests.exceptions.ConnectionError:
                print "connection error", url_get_data
            else:
                break
        try:
            amount = int(pq(page)('div.common').eq(2)('span.num').eq(0).text().replace(',', ''))
        except ValueError:
            print "no correct amount. pass."
            return 0
        else:
            return amount


def fun_parser(url):  # 没问题
    try_time = 0
    while True:
        try:
            first_page = requests.get(url, headers={'Host': 'www.fun.tv', 'User-Agent': chrome}, timeout=2,
                                      allow_redirects=True).text
            playcount = int(pq(first_page)('div.exp-num').text().replace(',', ''))
        except (socket.timeout, requests.exceptions.Timeout):
            print "timeout", url
        except requests.exceptions.ConnectionError:
            print "connection error", url
        except ValueError:
            try_time += 1
            if try_time == 5:
                return 0
            else:
                print "fun video try again.", url  # no such video now
        else:
            break
    for li in pq(first_page)('ul.torrent')('li.torr-list-normal'):
        episode_link = 'http://www.fun.tv' + pq(li)('a').attr('href')
        while True:
            try:
                page = requests.get(episode_link, headers={'Host': 'www.fun.tv', 'Referer': url, 'User-Agent': chrome},
                                    timeout=2, allow_redirects=False).text
            except (socket.timeout, requests.exceptions.Timeout):
                print "timeout", episode_link
            except requests.exceptions.ConnectionError:
                print "connection error", episode_link
            else:
                break
        playcount += int(pq(page)('div.exp-num').text().replace(',', ''))
    return playcount


def xunlei_parser(url):  # 播放数与页面上显示的不符
    vid_pattern = re.compile(r'(?<=/v/)(\d+/\d+)')
    try:
        vid = re.search(vid_pattern, url).group()
    # print vid
    except AttributeError:
        print 'error url, no vid received.'
        return 0
    else:
        js_url = 'http://api.movie.kankan.com/vodjs/moviedata/{}.js'.format(vid)
        vv_pattern = re.compile(r'(?<=totle_vv=\').*(?=\';)', re.M)
        while True:
            try:
                js_data = requests.get(url=js_url, headers={'Referer': url, 'User-Agent': chrome}, timeout=2,
                                       allow_redirects=False).text
                totle_vv = re.search(vv_pattern, js_data).group()
            except (socket.timeout, requests.exceptions.Timeout):
                print "timeout", js_url
            except requests.exceptions.ConnectionError:
                print "connection error", js_url
            except AttributeError:
                print 'page response error. try again.'
                print js_url
            else:
                break
        totle_vv = totle_vv.replace(',', '')
        if totle_vv == "":
            return 0
        else:
            return int(totle_vv)


def wasu_parser(url):  # 没问题
    try_time = 0
    while True:
        try:
            base_page = requests.get(url, headers={'Accept': '*/*', 'Accept-Encoding': '',
                                                   'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4',
                                                   'Connection': 'keep-alive', 'Host': 'www.wasu.cn',
                                                   'User-Agent': chrome}, timeout=4, allow_redirects=False).text
            dramaId_pat = re.compile(r'(?<="/dramaId/"\+)\d+(?=\+)')
            drama_id = re.search(dramaId_pat, base_page).group()
            # print("drama id: {}".format(drama_id))
        except (socket.timeout, requests.exceptions.Timeout):
            print "timeout", url
        except requests.exceptions.ConnectionError:
            print "connection error", url
        except AttributeError:
            try_time += 1
            if try_time == 5:
                return 0
            else:
                print "page response error, no drama id has detected."
                print url
        else:
            break

    playcount = 0
    for each_show in pq(base_page)('#play_tipbox1')('a'):
        show_id = pq(each_show).attr('href').split('/')[-1]
        data_url = 'http://uc.wasu.cn/Ajax/updateViewHit/id/{}/pid/11/dramaId/{}'.format(show_id, drama_id)
        print data_url
        while True:
            try:
                cnt = requests.get(data_url, headers={'Host': 'uc.wasu.cn',
                                                      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                                                      'Accept-Encoding': '',
                                                      'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4',
                                                      'Cache-Control': 'max-age=0',
                                                      'Connection': 'keep-alive'}, timeout=10).text  #.replace(',','')
                if len(cnt.split('\n')) > 1:
                    cnt = cnt.split('\n')[1]
                cnt = int(cnt.replace(',', ''))
            except (socket.timeout, requests.exceptions.Timeout):
                print "timeout", data_url
            except (requests.exceptions.ConnectionError, requests.exceptions.ContentDecodingError):
                print "connection error", data_url
            except ValueError:
                if u"Gateway Time-out" in cnt:
                    print "gateway error"
            else:
                break
        playcount += cnt
    return playcount


def get_drama_ranks_from_db(websites, today_drama_coll, dramas_coll, today):
    today_str = today.strftime("%Y-%m-%d")
    yest_str = (today + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
    # 今日爬取的 且有资源的 都在dramas_coll里面
    today_exist = [i['name'] for i in dramas_coll.find({'date': today_str}, {'name': 1, '_id': 0})]
    # 昨日爬取的 且有资源的
    yest_crawl_exist = [i['name'] for i in dramas_coll.find({'date': yest_str}, {'name': 1, '_id': 0})]
    data_exist = [i for i in today_exist if i in yest_crawl_exist]
    # 今日的电台会放的电视剧
    today_tv_list = today_drama_coll.find_one({'date': today_str}, {'dramas': 1, '_id': 0}, timeout=False)['dramas']  #.extend(drama_to_crawl_coll.find_one({'date:'}))
    # 今日电视台要放 且有历史数据的 可用于排行
    today_rank_list = [i for i in today_tv_list if i in data_exist]

    rows = dict(zip(websites, range(10)))  # 10个视频网站
    cols = dict(zip(today_rank_list, range(len(today_rank_list))))  # 今日播出的电视剧且有资源的

    for drama, seq in cols.iteritems():
        print drama, seq
    print "########################################"

    ranks = np.zeros((10, len(today_rank_list)), dtype=np.int64)  #.fill(-1)
    ranks.fill(-1)

    for drama in today_rank_list:
        try:
            srcs = dramas_coll.find_one({'name': drama, 'date': today_str}, {'srcs': 1, '_id': 0})['srcs']
        except TypeError:
            print ""
            continue  # 这部戏没有资源那么就跳过 ranks里面这个位置就是-1
        else:
            for website, today_count in srcs.iteritems():
                # print website, today_count,drama
                try:
                    yesterday_count = \
                        dramas_coll.find_one({'name': drama, 'date': yest_str}, {'srcs': 1, '_id': 0})['srcs'][website]
                except KeyError:
                    continue
                else:
                    # print yesterday_count
                    ranks[rows[website], cols[drama]] = today_count - yesterday_count

    for a in ranks: # 根据ranks里面的数值,得到每个网站的排序ranks
        pos_dic = dict(zip(a, range(len(a))))
        filter_a = np.array([i for i in a if i != -1])
        rank = dict(zip(sorted(filter_a, reverse=True), range(len(filter_a))))

        for val, idx in rank.iteritems():
            a[pos_dic[val]] = idx + 1

    return ranks, today_rank_list  # 用,分隔的

def get_each_drama_playcount(dramas_collection, drama_to_crawl):
    parsers = {
        u'土豆': tudou_parser,
        u'搜狐视频': sohu_parser,
        u'华数TV': wasu_parser,
        u'芒果TV': hunan_parser,
        u'优酷': youku_parser,
        u'爱奇艺': iqiyi_parser,  # 爱情公寓在soku中能搜到爱奇艺的源,但是避免重复计算播放量还是使用iqiyi链接
        u'腾讯视频': qq_parser,
        u'乐视网': letv_parser,
        u'迅雷看看': xunlei_parser,
        u'风行网': fun_parser  ###### pptv没有播放数
    }
    date = datetime.datetime.now().strftime('%Y-%m-%d')
    for drama in drama_to_crawl:
        srcs = {}
        for item in search_in_all(drama).itervalues():
            for website, link in item.iteritems():
                if website not in parsers or not link:
                    continue
                print link
                srcs.setdefault(website, 0)
                srcs[website] += parsers[website](link)
        srcs = dict(filter(lambda x: x[1] != 0, srcs.items())) # 去除播放数的0的网站数据
        if srcs != {}:  # 如果电视剧没有资源, 就不存这部电视剧,等到它有播放量了为止
            dramas_collection.update({'name': drama, 'date': date}, {'$set': {'srcs': srcs}}, upsert=True)


def find_init_rank(ranks):
    site_min = [0, np.inf]
    positions = {}
    row, col = np.shape(ranks)
    for i in range(row):
        no_srcs_list = []
        for j in range(col):
            if ranks[i][j] == -1:
                no_srcs_list.append(j)
        if site_min[1] > len(no_srcs_list):
            site_min[0], site_min[1] = i, len(no_srcs_list)
        positions[i] = no_srcs_list
    init_rank = ranks[site_min[0]]
    exist_now = col - site_min[1]
    l = positions[site_min[0]]
    for i in range(site_min[1]):
        init_rank[l[i]] = exist_now + i + 1
    return init_rank

def get_trans(rank, today_rank_list, drama_rank_coll, today):
    l = rank.tolist()
    final_rank = [today_rank_list[l.index(i)] for i in range(1,len(l)+1)]
    drama_rank_coll.update({'date':today.strftime('%Y-%m-%d')}, {'$set':{'rank':final_rank}}, upsert = True)
    return final_rank

def get_tv_station_ranks_from_db(websites, tv_station_list, tv_coll, varieties_coll):
    rows = dict(zip(websites, range(10)))  # 10个视频网站
    cols = dict(zip(tv_station_list, range(len(tv_station_list)))) # 30个电视台
    ranks = np.zeros((10, len(tv_station_list)), dtype=np.int64)  #.fill(-1)
    ranks.fill(-1)

    for station in tv_station_list:
        station_result = {}
        show_list = []
        for everyday in tv_coll.find({'name':station}, {'variety':1, '_id':0}).sort('date',pymongo.DESCENDING).limit(7): # 按时间大至小降序排列 然后返回最近的7天
            show_list.extend(everyday['variety'])
        show_list = set(show_list) # 去掉重复的show
        for show in show_list:
            try:
                showinfo = list(varieties_coll.find({'name':show}, {'srcs':1, '_id':0}).sort('date',pymongo.DESCENDING).limit(1))[0]
            except IndexError: # 没有这个综艺节目的近七天数据
                print show
                continue
            else:
                for website, playcount in showinfo['srcs'].iteritems(): # 找到当前所要的节目最近一次爬取的数据
                    station_result.setdefault(website, 0)
                    station_result[website] += playcount
        for website, sumvalue in station_result.iteritems():
            ranks[rows[website], cols[station]] = sumvalue

    for a in ranks: # 根据ranks里面的数值,得到每个网站的排序ranks
        pos_dic = dict(zip(a, range(len(a))))
        filter_a = np.array([i for i in a if i != -1])
        rank = dict(zip(sorted(filter_a, reverse=True), range(len(filter_a))))

        for val, idx in rank.iteritems():
            a[pos_dic[val]] = idx + 1

    return ranks

def get_drama_rank(today, websites, drama_to_crawl, dramas_coll, today_drama_coll, drama_rank_coll):
    print "###################drama list for crawling ready###################"
    get_each_drama_playcount(dramas_coll,drama_to_crawl)
    print "################## drama playcount crawl finish ###################"
    ranks,today_rank_list = get_drama_ranks_from_db(websites, today_drama_coll, dramas_coll, today)
    init_rank = find_init_rank(ranks)
    print "init drama rank:",init_rank
    new_rank, new_tau = annealing(ranks=ranks, cur_rank=init_rank, temperature_begin=300, temperature_end=0.1,
                                  cooling_factor=.95, nb_iterations=200)
    print "aggregated drama rank:", new_rank, new_tau
    
    # 将排名存入数据库
    get_trans(new_rank, today_rank_list, drama_rank_coll, today)
    print "#############每日 电视剧 排名已完成存储############"

if __name__ == '__main__':
    # print youku_parser('http://v.youku.com/v_show/id_XOTU3NDA3MDky.html?from=s1.8-3-3.1')
    # print tudou_parser('http://www.tudou.com/albumplay/NZRgBwCFJUg/zKNfPorXDM8.html')
    # print wasu_parser('http://www.wasu.cn/Play/show/id/5981659')
    print hunan_parser('http://www.hunantv.com/v/2/108430/f/1128323.html')
    # print letv_parser('http://www.letv.com/ptv/vplay/22673216.html#vid=22673216')