#coding:utf-8

# 每日爬取当日的综艺节目即可 不需要从一周综艺中爬

import requests
import sys
import time
import json
import re
import socket
import datetime
import pymongo
import numpy as np
from drama import find_init_rank, get_trans
from rank_aggr_annealing import annealing
from pyquery import PyQuery as pq

accept = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
encode = 'gzip, deflate'
connection = 'keep-alive'
chrome = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.115 Safari/537.36'
iphone = 'VideoiPhone'
chinese = 'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4'
no_cache = 'max-age=0'

def search_in_all(keyword): 
	last_fri = (datetime.datetime.now() + datetime.timedelta(days=-7)).strftime("%Y%m%d")
	today = datetime.datetime.now().strftime("%Y%m%d")
	# print last_fri, today
	srcs = {}
	srcs.update(search_in_baidu(keyword, last_fri, today))
	srcs.update(search_in_kankan(keyword, last_fri, today))
	srcs.update(search_in_soku(keyword))
	return srcs # website:[links]
	# print json.dumps(srcs, indent = 4, ensure_ascii =False)

def search_in_baidu(keyword, last_fri, today):
	sites_list = {u'sohu':u'搜狐视频',u'wasu':u'华数TV',u'hunantv':u'芒果TV',u'iqiyi':u'爱奇艺',u'letv':u'乐视网',u'fun':u'风行网'}
	query_url = 'http://app.video.baidu.com/app?ie=utf-8&cuid=4ae93cc8602f3278825826136a249e39ee843a9d&ct=905969664&time={:.6f}&version=6.2.2&word={}&md=iphone&s=1'.format(time.time(),keyword)
	# print query_url
	while True:
		try:
			query_page = requests.get(query_url, headers = {'Host':'app.video.baidu.com',
									   						'Accept-Encoding':encode,
									   						'Connection':connection,
									   						'User-Agent':iphone}, timeout = 3)
			d = json.loads(query_page.text)
			result_json = d['sResult']['aldjson']
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", query_url
		except requests.exceptions.ConnectionError:
			print "connection error", query_url
		except KeyError:
			print "baidu search page error"
		else:
			break

	if len(result_json) == 0: # 如果搜不到结果pass
		return {}

	found = False
	for result in result_json:
		if u'actor' in result or u'director' in result:
			continue
		else:	# 找到第一个综艺节目就退出
			vid = result['id']
			found = True
			break
			
	if not found: # 如果所有的结果都没有综艺节目 就返回{}
		return {}

	sites = {}
	for site in result['sites']:
		name = site['site_url'].split('.')[0]
		if name in sites_list:
			chinese_name = sites_list[name]
			sites[chinese_name] = site['site_url']

	if not sites: # 搜到了结果但是没有满足的播放网站 pass
		return {}

	srcs = {} # 存结果的
	for chinese_name, site in sites.iteritems(): # key is url, value is site name
		findSrc_url = 'http://app.video.baidu.com/xqsingle/?cuid=4ae93cc8602f3278825826136a249e39ee843a9d&id={}&worktype=iphnativetvshow&year=2015&time={:.6f}&version=6.2.2&site={}'.format(vid, time.time(),site)
		# print findSrc_url
		while True:
			try:
				srcs_page = requests.get(findSrc_url, headers = {'Host':'app.video.baidu.com',
												   				 'Accept-Encoding':encode,
												   				 'Connection':connection,
												   				 'User-Agent':iphone}, timeout = 2)
				d = json.loads(srcs_page.text)
				videos_list = d['videos']
			except (socket.timeout, requests.exceptions.Timeout):
				print "timeout", findSrc_url
			except requests.exceptions.ConnectionError:
				print "connection error", findSrc_url
			except KeyError:
				print "baidu search apage error"
			else:
				break
		
		if videos_list: # 每个网站上这个星期之内播放的节目链接
			for video in videos_list:
				if video['episode'] >= last_fri and video['episode'] < today:
					srcs.setdefault(chinese_name,[])
					srcs[chinese_name].append(video['url'])
	return srcs
	# print json.dumps(srcs,indent = 4, ensure_ascii = False)

def search_in_kankan(keyword, last_fri, today): # 迅雷看看爬取的是节目的总播放量 因为网站上没有分集播放量
	vid_pat = re.compile(r'(?<=/v/)(\d+/\d+)')
	url = 'http://search.kankan.com/search.php?keyword={}'.format(keyword)
	while True:
		try:
			result_page = requests.get(url, headers = {}, timeout = 3, allow_redirects = False)
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", url
		except requests.exceptions.ConnectionError:
			print "connection error", url
		else:
			break
	result_page.encoding = 'utf-8'
	ep_list = {}
	for ep in pq(result_page.text)('div.diversity')('a'):
		try:
			pubdate = pq(ep).attr('title')[:10].replace('-','')
		except TypeError:
			continue # 不是综艺节目的link 跳过
		else:	
			if pubdate >= last_fri and pubdate < today:
				url = pq(ep).attr('href')
				vid = re.search(vid_pat, url).group()
				if vid in ep_list:
					continue
				else:
					ep_list.setdefault(vid,url)
	if ep_list:
		return {u'迅雷看看':ep_list.values()}
	else:
		return {}

def search_in_soku(keyword):
	last_fri = (datetime.datetime.now() + datetime.timedelta(days=-7)).strftime("%m%d")
	today = datetime.datetime.now().strftime("%m%d")
	url = 'http://www.soku.com/v?keyword={}'.format(keyword)
	while True:
		try:
			main_page = requests.get(url, headers = {'Accept':accept,
											 'Accept-Encoding':encode,
											 'Accept-Language':chinese,
											 'Connection':connection,
											 'Host':'www.soku.com',
											 'User-Agent':chrome}, timeout = 2, allow_redirects = False).text
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", url
		except requests.exceptions.ConnectionError:
			print "connection error", url
		else:
			break
	select_list = [u'腾讯视频', u'优酷', u'土豆']
	srcs = {} # 存储结果
	for each in pq(main_page)('div.DIR')('div.item'):
		if pq(each).children('div').hasClass('tv') or pq(each).children('div').hasClass('movie'):
			continue # 是电影电视剧就pass
		else:
			for site in pq(each)('div.detail')('div.T')('div.playarea')('div.pgm-source')('div.source')('span').filter(lambda i, this: pq(this).attr('name') in select_list):
				# url = pq(site)('a').attr('href') # 优酷 土豆 腾讯都是只招一个入口链接:优酷爬节目总播放数 土豆爬最近期的 腾讯
				name = pq(site).attr('name')
				link = []
				if name == u'腾讯视频':
					for i in pq(each)('div.zy')('div.detail')('div.T')('div.accordion.accordion_zy.site27')('div.accordion-heading').filter(lambda i, this: 
						pq(this)('span.date').text().replace('-','') >= last_fri and pq(this)('span.date').text().replace('-','') < today):
						link.append(pq(i)('a').attr('href'))
				elif name == u'优酷':
					for i in pq(each)('div.zy')('div.detail')('div.T')('div.linkpanels.linkpanels_zy.site14')('div.items')('ul.v')('li.v_title').filter(lambda i, this: 
						pq(this)('span.phases').text().replace('-','') >= last_fri and pq(this)('span.date').text().replace('-','') < today):
						link.append(pq(i)('a').attr('href'))
				elif name == u'土豆':
					for i in pq(each)('div.zy')('div.detail')('div.T')('div.linkpanels.linkpanels_zy.site1')('div.items')('ul.v')('li.v_title').filter(lambda i, this: 
						pq(this)('span.phases').text().replace('-','') >= last_fri and pq(this)('span.date').text().replace('-','') < today):
						link.append(pq(i)('a').attr('href'))
				else:
					pass
				if link: # 没有最近的节目就不添加
					srcs[name] = link
			break # 只要找到一个综艺节目 后面的就不找了
	return srcs

def iqiyi_variety_parser(url):
	vid_pat = re.compile(r'(?<=albumId:).*?(?=,)', re.M)
	count_pat = re.compile(r'(?<=":).*?(?=}])')
	try_time = 0
	while True:
		try:
			main_page = requests.get(url, headers = {'Host':'www.iqiyi.com',
												 	 'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
													 'Accept-Encoding':'gzip, deflate, sdch',
													 'Accept-Language':chinese,
													 'Cache-Control':no_cache,
													 'Connection':connection,
													 'User-Agent':chrome
													 }, timeout = 2, allow_redirects = False).text
			vid = re.search(vid_pat, main_page).group().strip()
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", url
		except requests.exceptions.ConnectionError:
			print "connection error", url
		except AttributeError:
			try_time += 1
			if try_time == 5:
				return 0
			else:
				print "iqiyi vid no found"
		else:
			break

	count_url = 'http://cache.video.qiyi.com/jp/pc/{}/'.format(vid)
	
	try_time = 0
	while True:
		try:
			count_page = requests.get(count_url, headers = {'Accept':'*/*',
															'Accept-Encoding':'gzip, deflate, sdch',
															'Accept-Language':chinese,
															'Connection':connection,
															'Host':'cache.video.qiyi.com',
															'Referer':url,
															'User-Agent':chrome}, timeout = 2, allow_redirects = False).text
			play_count = int(re.search(count_pat, count_page).group().strip())
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", count_url
		except requests.exceptions.ConnectionError:
			print "connection error", count_url
		except AttributeError:
			try_time += 1
			if try_time == 5:
				return 0
			else:
				print "iqiyi count page error"
		except ValueError:
			print "iqiyi page error"
		else:
			break
	return play_count


def wasu_variety_parser(url):
	vid = url.split('?')[0].split('/')[-1]
	count_url = 'http://uc.wasu.cn/Ajax/updateViewHit/id/{}/pid/37/dramaId/{}'.format(vid, vid)
	try_time = 0
	while True:
		try:
			count_page = requests.get(count_url, headers = {'Host':'uc.wasu.cn',
															# 'Referer':'http://uc.wasu.cn/Public/iframe.html',
															'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
															'Accept-Encoding': '',#'gzip, deflate, sdch',
															'Accept-Language':'zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4',
															'Cache-Control': no_cache,
											   				'Connection':connection,
											   				# 'Referer':'http://uc.wasu.cn/Public/iframe.html',
															# 'X-Requested-With':'XMLHttpRequest',
															'User-Agent':chrome}, timeout = 10).text
			if len(count_page.split('\n')) > 1:
				play_count = count_page.split('\n')[1]
				play_count = int(play_count.replace(',', ''))
			else:
				play_count = int(count_page.replace(',', ''))
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", count_url
		except requests.exceptions.ConnectionError:
			print "connection error", count_url
		except ValueError:
			if u"Gateway Time-out" in count_page:
				print "gateway error"
		except AttributeError:
			try_time += 1
			if try_time == 5:
				return 0
			else:
				print "letv page error: {}".format(count_url)
		else:
			break
	return play_count

def letv_variety_parser(url):
	count_pat = re.compile(r'(?<="media_play_count":).*?(?=,")')
	vid = url.split('/')[-1].split('.')[0]
	count_url = 'http://stat.letv.com/vplay/queryMmsTotalPCount?callback=jQuery17106965199981350452_1428409686854&vid={}'.format(vid)
	
	try_time = 0
	while True:
		try:
			count_page = requests.get(count_url, headers = {'Accept':'*/*',
									   						'Accept-Encoding':encode,
									   						'Connection':connection,
									   						'Host':'stat.letv.com',
									   						'Referer':url,
									   						'User-Agent':chrome}, timeout = 2)
			play_count = int(re.search(count_pat, count_page.text).group())
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", count_url
		except requests.exceptions.ConnectionError:
			print "connection error", count_url
		except AttributeError:
			try_time += 1
			if try_time == 5:
				return 0
			else:
				print "letv page error: {}".format(count_url)
		except ValueError:
			print "letv page error"
		else:
			break
	# print count_page.encoding
	return play_count

def hunan_variety_parser(url):
	vid = url.split('/')[-1].split('.')[0]
	count_url = 'http://click.hunantv.com/get.php?aid={}&type=videos'.format(vid)
	count_pat = re.compile(r'(?<="click":").*?(?=",)')
	try_time = 0
	while True:
		try:
			count_page = requests.get(count_url, headers = {'Accept':'*/*',
									   						'Accept-Encoding':encode,
									   						'Connection':connection,
									   						'Host':'click.hunantv.com',
									   						'Referer':url,
									   						'User-Agent':chrome}, timeout = 2).text
			play_count = re.search(count_pat, count_page).group()
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", count_url
		except requests.exceptions.ConnectionError:
			print "connection error", count_url
		except AttributeError:
			try_time += 1
			if try_time == 5:
				return 0
			else:
				print "hunan page error: {}".format(count_url)
		else:
			break
	if play_count[-1] == u'万':
		play_count = int(play_count[:-1]) * 10000
	else:
		play_count = int(play_count.replace(',', ''))

	return play_count

def fun_variety_parser(url):
	try_time = 0
	while True:
		try:
			count_page = requests.get(url, headers = {'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
															'Accept-Encoding':'gzip, deflate, sdch',
															'Accept-Language':chinese,
															'Cache-Control':no_cache,
															'Connection':connection,
															'Host':'www.fun.tv',
									   						'User-Agent':chrome}, timeout = 2).text
			# play_count = re.search(count_pat, count_page).group()

			play_count = int(pq(count_page)('#_digglist')('div.exponent')('div.exp-num').text().replace(',',''))
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", url
		except requests.exceptions.ConnectionError:
			print "connection error", url
		except AttributeError:
			try_time += 1
			if try_time == 5:
				return 0
			else:
				print "fun page error: {}".format(url)
		except ValueError:
			print "fun page error"
		else:
			break
	return play_count

def sohu_variety_parser(url):
	vid_pat = re.compile(r'(?<=vid=").*?(?=";)',re.M)
	count_pat = re.compile(r'(?<="total":).*?(?=,")')
	try_time = 0
	while True:
		try:
			main_page = requests.get(url, headers = {'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
													 'Accept-Encoding':'gzip, deflate, sdch',
													 'Accept-Language':chinese,
													 'Cache-Control':no_cache,
													 'Connection':connection,
													 'Host':'tv.sohu.com',
									   				 'User-Agent':chrome}, timeout = 2).text
			vid = re.search(vid_pat, main_page).group()
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", url
		except requests.exceptions.ConnectionError:
			print "connection error", url
		except AttributeError:
			try_time += 1
			if try_time == 5:
				return 0
			else:
				print "sohu page error, no vid found"
		else:
			break
	# print vid
	count_url = 'http://count.vrs.sohu.com/count/queryext.action?vids={}'.format(vid)
	try_time = 0
	while True:
		try:
			count_page = requests.get(count_url , headers = {'Accept':'*/*',
													 'Accept-Encoding':'gzip, deflate, sdch',
													 'Accept-Language':chinese,
													 'Connection':connection,
													 'Host':'count.vrs.sohu.com',
													 'Referer':url,
													 'User-Agent':chrome}, timeout = 2, allow_redirects = False).text
			play_count = int(re.search(count_pat, count_page).group())
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", url
		except requests.exceptions.ConnectionError:
			print "connection error", url
		except AttributeError:
			try_time += 1
			if try_time == 5:
				return 0
			else:
				print "sohu page error, no vid found"
		except ValueError:
			print "sohu page error"
		else:
			break
	return play_count



def xunlei_variety_parser(url): # 总播放数
	vid_pat = re.compile(r'(?<=/v/)(\d+/\d+)')
	count_pat = re.compile(r'(?<=totle_vv=\').*?(?=\';)')
	vid = re.search(vid_pat, url).group()
	count_url = 'http://api.movie.kankan.com/vodjs/moviedata/{}.js'.format(vid)
	try_time = 0
	while True:
		try:
			count_page = requests.get(count_url, headers = {'Accept':'*/*',
											 				'Accept-Encoding':encode,
															'Accept-Language':chinese,
															'Connection':connection,
															'Host':'api.movie.kankan.com',
															'Referer':url,
															'User-Agent':chrome}, timeout = 2, allow_redirects = False).text
			playcount = re.search(count_pat, count_page).group()
			# print playcount
			if playcount:
				playcount = int(playcount.replace(',',''))
			else:
				playcount = 0
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", count_url
		except requests.exceptions.ConnectionError:
			print "connection error", count_url
		except AttributeError:
			try_time += 1
			if try_time > 5:
				return 0
			else:
				print "kankan count page error"
		except ValueError:
			print "kankan count is not a str to int"
		else:
			break
	return playcount

def tudou_variety_parser(url): #, today, last_fri):
	last_fri = (datetime.datetime.now() + datetime.timedelta(days=-7)).strftime("%Y%m%d")
	today = datetime.datetime.now().strftime("%Y%m%d")

	iid_pat = re.compile(r'(?<=,iid: )(\d+)', re.M)
	lcode_pat = re.compile(r'(?<=,lcode: \').*?(?=\')', re.M)
	json_pat = re.compile(r'(?<=page_play_model_aListModelList__getTvp\().*(?=\))', re.M)
	while True:
		try:
			main_page = requests.get(url, headers = {'Accept':accept,
													 'Accept-Encoding':encode,
													 'Accept-Language':chinese,
													 'Cache-Control':no_cache,
													 'Connection':connection,
													 'Host':'www.tudou.com',
													 'User-Agent':chrome}, timeout = 4, allow_redirects = False).text
			# iid = re.search(iid_pat, main_page).group()
			lcode = re.search(lcode_pat, main_page).group()
			# print lcode
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", url
		except requests.exceptions.ConnectionError:
			print "connection error", url
		except AttributeError:
			print "tudou no lcode found"
		else:
			break
	ep_url = 'http://www.tudou.com/tvp/getMultiTvcCodeByAreaCode.action?jsoncallback=page_play_model_aListModelList__getTvp&type=3&codes={}&app=7&areaCode=310000'.format(lcode)
	while True:
		try:
			ep_page = requests.get(ep_url, headers = {'Accept':accept,
														'Referer':url,
														'User-Agent':chrome,
														'X-Requested-With':'XMLHttpRequest'},
									timeout = 2, allow_redirects = False).text
			s = re.search(json_pat, ep_page).group()
			jsonstr = json.loads(s)['message']
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", ep_url
		except requests.exceptions.ConnectionError:
			print "connection error", ep_url
		except AttributeError:
			print "tudou no iid found"
		else:
			break

	iids_list = []
	for ep in jsonstr:
		pubdate = '20'+ep['title'].split(' ')[-1]
		if pubdate >= last_fri and pubdate < today:
			iids_list.append(ep['iid'])

	play_count = 0
	for iid in iids_list:
		count_url = 'http://www.tudou.com/crp/itemSum.action?iabcdefg={}&juabcdefg=019ichnk5m10cm'.format(iid)
		while True:
			try:
				count_page = requests.get(count_url, headers = {'Accept':accept,
																 'Accept-Encoding':encode,
																 'Accept-Language':chinese,
																 'Connection':connection,
																 'Host':'www.tudou.com',
																 'Referer':url,
																 'User-Agent':chrome,
																 'X-Requested-With':'XMLHttpRequest'
												 				}, timeout = 2, allow_redirects = False).text
				play_count += int(json.loads(count_page)['playNum'])
			except (socket.timeout, requests.exceptions.Timeout):
				print "timeout", url
			except requests.exceptions.ConnectionError:
				print "connection error", url
			except ValueError:
				print "tudou count page error"
			else:
				break
	return play_count

def youku_variety_parser(url):
	vid_pat = re.compile(r'(?<=videoId = \').*?(?=\';)',re.M)
	while True:
		try:
			main_page = requests.get(url, headers = {'Accept':accept,
											 'Accept-Encoding':encode,
											 'Accept-Language':chinese,
											 'Cache-Control':no_cache,
											 'Connection':connection,
											 'Host':'v.youku.com',
											 'User-Agent':chrome},timeout = 2, allow_redirects = False).text
			vid = re.search(vid_pat, main_page).group()
			# play_count = int(pq(main_page)('div#item_'+iid)('a')('div.statplay').text().replace(',',''))
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", url
		except requests.exceptions.ConnectionError:
			print "connection error", url
		except AttributeError:
			print "youku vid no found"
		else:
			break
	count_url = 'http://v.youku.com/v_vpactionInfo/id/{}/pm/3?__rt=1&__ro=info_stat'.format(vid)
	try_time = 0
	while True:
		try:
			count_page = requests.get(count_url, headers = {'Accept':accept,
															'Accept-Encoding':encode,
															'Accept-Language':chinese,
															'Connection':connection,
															'Host':'v.youku.com',
															'Referer':url,
															'User-Agent':chrome}, timeout = 2, allow_redirects = False).text
			play_count = int(pq(count_page)('div.common').eq(0)('ul.half').eq(0)('span.num').text().replace(',', ''))
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", count_url
		except requests.exceptions.ConnectionError:
			print "connection error", count_url
		except AttributeError:
			print "youku count no found"
		except ValueError:
			try_time += 1
			if try_time > 5:
				print "youku no playcount, pass"
				return 0
		else:
			break
	return play_count

def qq_variety_parser(url):
	count_pat = re.compile(r'(?<="all":).*?(?=,")', re.M)
	vid = url.split('/')[-1].split('.')[0]
	count_url = 'http://sns.video.qq.com/tvideo/fcgi-bin/batchgetplaymount?callback=jQuery1910056143816793337464_1428574983123&low_login=1&id={}&otype=json&_=1428574983124'.format(vid)
	while True:
		try:
			count_page = requests.get(count_url, headers = {}, timeout = 2, allow_redirects = False).text
			play_count = int(re.search(count_pat, count_page).group())
		except (socket.timeout, requests.exceptions.Timeout):
			print "timeout", count_url
		except requests.exceptions.ConnectionError:
			print "connection error", count_url
		except ValueError:
			print "qq count page error"
		else:
			break
	return play_count

def get_varieties_playcount_and_store(keyword, varieties_coll, date): # a list
	parsers = {
 		u'土豆': tudou_variety_parser,
        u'搜狐视频': sohu_variety_parser,
        u'华数TV': wasu_variety_parser,
        u'芒果TV': hunan_variety_parser,
        u'优酷': youku_variety_parser,
        u'爱奇艺': iqiyi_variety_parser,  
        u'腾讯视频': qq_variety_parser,
        u'乐视网': letv_variety_parser,
        u'迅雷看看': xunlei_variety_parser,
        u'风行网': fun_variety_parser
	}
	count_results = {}
	for site, links in search_in_all(keyword.encode('utf-8')).iteritems():
		playcount = 0
		for link in links:
			print link
			playcount += parsers[site](link)
		count_results[site] = playcount
	count_results = dict(filter(lambda x: x[1] != 0, count_results.items())) # 去除播放数的0的网站数据
	if count_results:
		varieties_coll.update({'name': keyword, 'date': date}, {'$set': {'srcs': count_results}}, upsert=True)

def get_variety_ranks_from_db(varieties_coll,websites, today): # today is datetime.datetime
	today_str = today.strftime("%Y-%m-%d")
	# 今日爬取的 且有资源的 都在varieties_coll里面 可用于排名
	today_rank_list = [i['name'] for i in varieties_coll.find({'date': today_str}, {'name': 1, '_id': 0})]

	rows = dict(zip(websites, range(10)))  # 10个视频网站
	cols = dict(zip(today_rank_list, range(len(today_rank_list))))  # 今日播出的且有资源的
	for variety, seq in cols.iteritems():
		print variety, seq
	print "########################################"

	ranks = np.zeros((10, len(today_rank_list)), dtype=np.int64)
	ranks.fill(-1)

	for variety in today_rank_list:
		try:
			srcs = varieties_coll.find_one({'name': variety, 'date': today_str}, {'srcs': 1, '_id': 0})['srcs']
		except TypeError:
			print "" # 实际上不会出现这种error 因为today_rank_list中的节目都是从varieties_coll拿出来的不会找不到
			continue  # 没有资源那么就跳过 ranks里面这个位置就是-1
		else:
			for website, today_count in srcs.iteritems():
				ranks[rows[website], cols[variety]] = today_count

	for a in ranks:
		pos_dic = dict(zip(a, range(len(a))))
		filter_a = np.array([i for i in a if i != -1])
		rank = dict(zip(sorted(filter_a, reverse=True), range(len(filter_a))))

		for val, idx in rank.iteritems():
			a[pos_dic[val]] = idx + 1

	return ranks, today_rank_list  # 用,分隔的

def get_variety_rank(today, websites, varieties_coll, today_variety_coll, variety_rank_coll):
	today_variety_list = today_variety_coll.find_one({'date': today.strftime("%Y-%m-%d")}, {'varieties': 1, '_id': 0}, timeout=False)['varieties']
	for variety in today_variety_list:
		get_varieties_playcount_and_store(variety, varieties_coll, today.strftime("%Y-%m-%d"))
	ranks, today_rank_list = get_variety_ranks_from_db(varieties_coll,websites, today)
	for rank in ranks:
		print rank
	print "#############全网 综艺 排名已完成##############"
	init_rank = find_init_rank(ranks)
	print "init variety rank:", init_rank
	new_rank, new_tau = annealing(ranks=ranks, cur_rank=init_rank, temperature_begin=300, temperature_end=0.1,
                                  cooling_factor=.95, nb_iterations=200)
	print "aggregated rank:", new_rank, new_tau
	print "#############排名融合已完成#############"
    # 将排名存入数据库
	get_trans(init_rank, today_rank_list, variety_rank_coll, today)
	print "#############每日 综艺 排名已完成存储############"

