#coding:utf-8

import requests
from pyquery import PyQuery as pq

def get_weibo_count(keyword):
    search_data = {'keyword':keyword, 'smblog':u'搜微博'}
    page = requests.post('http://weibo.cn/search/?pos=search', data = search_data)
    print page.encoding
#    print page.text
    with open('main.html', 'w+') as outfile:
        outfile.write(page.text.encode('utf-8'))

def login():
    s = requests.Session()
    #login_page = s.get('http://login.weibo.cn/login/').text
    req = requests.Request('POST', 'http://login.weibo.cn/login/',
                            data  = )
    password_name = pq(login_page)('input').filter(lambda i, this: pq(this).attr('type') == 'password').attr('name')
    r = s.post('http://login.weibo.cn/login/', data = {'mobile':'wangjianan527@gmail.com'})
    
    print r.text

def main():
    keyword = u'快乐大本营'
    get_weibo_count(keyword)

if __name__ == '__main__':
    #main()
    login()
