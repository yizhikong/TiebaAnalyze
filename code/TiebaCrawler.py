# -*- coding: cp936 -*-
import urllib2
import urllib
import re
import json
from datetime import datetime
from bs4 import BeautifulSoup
import thread, threading
import traceback

MAX_TIME = 30

class TiebaCrawler(object):
        
    def __init__(self):
        pass

    # crawl tiezi links from tieba
    def getTieziLinks(self, keyword, page_num=5):
        url = 'http://tieba.baidu.com/f?'
        query_dict = {'kw':keyword.encode('gb2312'), 'pn':0, 'ie':'utf-8'}
        _pat = re.compile('<a href="(.*?)" title="(.*?)" target="_blank" class="j_th_tit ">')
        links = []
        # get the links one page by one page
        for i in range(page_num):
            query_dict['pn'] = i * 50
            page_url = url + urllib.urlencode(query_dict)
            html = urllib2.urlopen(page_url, timeout=MAX_TIME).read()
            links += _pat.findall(html)
        # delete duplicate
        link_dict = {}
        for link in links:
            url, title = 'http://tieba.baidu.com' + link[0], link[1]
            link_dict[url] = title
        
        return link_dict

    # crawl the detail of tiezi, include tiezi information and comments
    def getTieziDetails(self, link):
        print link
        html = urllib2.urlopen(link, timeout=MAX_TIME).read()
        # get basic information, include fid, tid, relpy_num, page_num
        information = self.getTieziInformation(link = link, html = html)

        # begin to get reply and lzl comments
        comments = {}

        for p in range(1, information['page_num'] + 1):
            # get reply(floors) of this page
            page_query_dict = {'pn' : p}
            page_url = link + '?' + urllib.urlencode(page_query_dict)
            # html = urllib2.urlopen(pageUrl, timeout=MAX_TIME).read()
            page_cmt = self.getTieziComment(link=page_url,
                fid=information['fid'], tid=information['tid'])
            comments = dict(comments, **page_cmt)
        return comments, information

    '''get relpy_num, page_num, fid and tid of a tiezi'''
    def getTieziInformation(self, link, html=None):
        # get html content if just call by link
        if html is None:
            html = urllib2.urlopen(link, timeout=MAX_TIME).read()

        information = {'reply_num':-1, 'page_num':-1, 'fid':'', 'tid':'', 'lz':'', 'title':''}

        # get reply number, and total page number
        info_pat = re.compile('<li class="l_reply_num".*?><span class="red" ' + 
            'style=.*?>(.*?)</span>.*?<span class="red">(.*?)</span>.*?</li>')
        result = info_pat.search(html)
        if result:
            information['reply_num'] = int(result.group(1))
            information['page_num'] = int(result.group(2))

        if information['page_num'] > 50:
            raise Exception("page num > 50")
            
        # get fid and tid
        id_pat = re.compile("fid:'(\d+)'.*?tid:'(\d+)'")
        result = id_pat.search(html)
        if result:
            information['fid'] = result.group(1)
            information['tid'] = result.group(2)

        # get uname of lz
        lz_pat = re.compile('<div class="louzhubiaoshi.*?author="(.*)">')
        result = lz_pat.search(html)
        if result:
            information['lz'] = result.group(1)

        # get title
        title_pat = re.compile('<title>(.*?)</title>')
        result = title_pat.search(html)
        if result:
            information['title'] = result.group(1)
        return information

    '''get comments of a special tiezi page, including reply comments and lzl comments'''
    def getTieziComment(self, link, html=None, fid=None, tid=None):
        print link
        # get html content if just call by link
        if html is None:
            html = urllib2.urlopen(link, timeout=MAX_TIME).read()

        # get pid and fid if they are None
        if fid is None or fid is None:
            information = self.getTieziInformation(link, html)
            fid, tid = information['fid'], information['tid']

        '''
        tiezi_comment[pid]['info']
                              |---['uid'] # 1175721558
                              |---['uname'] # \u7409\u7483\u5c0f\u4ead
                              |---['content'] # 1L\u796d\u5fc3\u810f<img src=\"http:\/\/imgs8.jpg\" width=\"252\>
                              |---['time'] # u'2015-09-03 09:58'
                              |---['floor_idx']
                           ['lzl']
                              |---[0]
                                   |---['spid']
                                   |---['uname']
                                   |---['content']
                                   |---['time']
                              |---[1]
                              |---[2]
        '''

        tiezi_comment = {}
        soup = BeautifulSoup(html)
        floors = soup.findAll('div', attrs={'class':'l_post l_post_bright j_l_post clearfix  '})
        for floor in floors:
            floor_info = {}
            data_field = json.loads(floor['data-field'])
            pid = data_field['content']['post_id']
            comment_num = data_field['content']['comment_num']
            try:
                floor_info['uid'] = data_field['author']['user_id']
            except:
                continue
            floor_info['uname'] = data_field['author']['user_name'] # \u7409\u7483\u5c0f\u4ead
            #content = floor.find('div', attrs={'class':'d_post_content j_d_post_content  clearfix'})
            try:
                floor_info['content'] =  data_field['content']['content'].strip()
            except:
                continue
            #floor_info['floor_idx'] = data_field['content']['post_index'] + 1
            # get post time
            tail_info = floor.findAll('span', attrs={'class':'tail-info'})
            floor_info['floor_idx'] = tail_info[-2].text[:-1]
            floor_info['time'] = tail_info[-1].text # u'2015-09-03 09:58'

            tiezi_comment[pid] = {}
            tiezi_comment[pid]['info'] = floor_info
            tiezi_comment[pid]['lzl'] = self.getFloorLzlComment(tid = tid, pid = pid, 
                                                        total_page = (comment_num + 9) / 10)
        #print 'get ' + str(len(tiezi_comment)) + ' comments'
        return tiezi_comment

    '''get lzl comments of a special floor'''
    def getFloorLzlComment(self, tid, pid, total_page=1):
        # http://tieba.baidu.com/p/comment?tid=4017583745&pid=75105119617&pn=2
        # this link can get one floor's all lzl comments. (floor is identified by pid)
        url = 'http://tieba.baidu.com/p/comment?'
        query_dict = {'tid':tid, 'pid':pid, 'pn':1}
        
        # get comments and put into a list
        lzl_comments = []
        for p in range(1, total_page + 1):
            query_dict['pn'] = p
            try:
                target = url + urllib.urlencode(query_dict)
                html = urllib2.urlopen(target, timeout=MAX_TIME).read()
            except:
                print 'in getFloorLzlComment ' + target
            soup = BeautifulSoup(html)
            comments = soup.findAll('li')
            # for comments of this page
            for cmt in comments:
                lzl = {}
                data = json.loads(cmt['data-field'], strict=False)
                if 'spid' not in data:
                    continue
                lzl['spid'] = data['spid']
                lzl['uname'] = cmt.find('a', attrs={'class':'at j_user_card '})['username']
                lzl['content'] = cmt.find('span', attrs={'class':'lzl_content_main'}).text
                lzl['content'] = lzl['content'].strip()
                lzl['time'] = cmt.find('span', attrs={'class':'lzl_time'}).text  # 2016-2-15 20:29
                lzl_comments.append(lzl)

        #print 'get ' + str(len(lzl_comments)) + ' lzl comments'
        return lzl_comments

    # the who does the user follow and the fans of the user
    # return ([follow_name1, follow_name2,...], [fans_name1, fans_name2,...])
    def getFollowsAndFans(self, uname):
        #http://tieba.baidu.com/home/main/?un=%E7%BF%BC%E4%B9%8B%E7%A9%BA%E7%A9%BA&ie=utf-8&fr=frs
        prefix = 'http://tieba.baidu.com'
        url = 'http://tieba.baidu.com/home/main/?'
        query_dict = {'un' : uname.encode('utf8'), 'ie' : 'utf-8', 'fr' : 'frs'}
        url = url + urllib.urlencode(query_dict)
        html = urllib2.urlopen(url, timeout=MAX_TIME).read()
        next_page_pat = re.compile('<a href="(.*?)" class="next">')

        # get follows
        follows = []
        follow_pat = re.compile('<span class="concern_num">\(<a href="(/home/concern.id.*?)" target="_blank">')
        result = follow_pat.search(html)
        if result:
            follow_name_pat = re.compile('name_show="(.*?)" href="#" >')
            follow_url = prefix + result.group(1)
            # print 'follow url : ' + follow_url
            while follow_url is not None:
                request = urllib2.Request(follow_url)
                request.add_header('User-Agent','Mozilla/5.0 (Windows NT 6.2; rv:16.0) Gecko/20100101 Firefox/16.0')
                follow_html = urllib2.urlopen(request, timeout=MAX_TIME).read()
                follows += follow_name_pat.findall(follow_html)
                sub_result = next_page_pat.search(follow_html)
                if sub_result:
                    next_url = prefix + sub_result.group(1)
                    if next_url != follow_url:
                        follow_url = next_url
                    else:
                        break
                else:
                    break

        # get fans
        fans = []
        # <span class="concern_num">(<a href="/home/fans?id=5ab9e7bfbce4b98be7a9bae7a9baa909?t=1424670733&fr=home" target="_blank">
        fan_pat = re.compile('<span class="concern_num">\(<a href="(/home/fans.id.*?)" target="_blank">')
        result = fan_pat.search(html)
        if result:
            fan_name_pat = re.compile('name_show="(.*?)" href="#" >')
            fan_url = prefix + result.group(1)
            # print 'fan url : ' + fan_url
            while fan_url is not None:
                request = urllib2.Request(fan_url)
                request.add_header('User-Agent','Mozilla/5.0 (Windows NT 6.2; rv:16.0) Gecko/20100101 Firefox/16.0')
                fan_html = urllib2.urlopen(request, timeout=MAX_TIME).read()
                fans += fan_name_pat.findall(fan_html)
                sub_result = next_page_pat.search(fan_html)
                if sub_result:
                    next_url = prefix + sub_result.group(1)
                    if next_url != fan_url:
                        fan_url = next_url
                    else:
                        break
                else:
                    break
                
        return follows, fans

    # get what tieba does the user concern and get the relative level
    # return [(tieba_name, level), (tieba_name level), ....]
    def getConcernTieba(self, uname):
        #http://tieba.baidu.com/home/main/?un=%E7%BF%BC%E4%B9%8B%E7%A9%BA%E7%A9%BA&ie=utf-8&fr=frs
        url = 'http://tieba.baidu.com/home/main/?'
        query_dict = {'un' : uname.encode('utf8'), 'ie' : 'utf-8', 'fr' : 'frs'}
        url = url + urllib.urlencode(query_dict)
        html = urllib2.urlopen(url, timeout=MAX_TIME).read()

        concerns = []
        #class="u-f-item unsign"><span>(.*?)</span><span class="forum_level lv(\d+)"> is ok
        #but will contain bazhu message
        concern_pat = re.compile('class="u-f-item unsign"><span>(.*?)</span>.*?"forum_level lv(\d+)">')
        return concern_pat.findall(html)

    # get members of a tieba
    # return [member_name1, member_name2, ...]
    def getTiebaMembers(self, tname):
        url = 'http://tieba.baidu.com/bawu2/platform/listMemberInfo?'
        query_dict = {'word' : tname.encode('utf8'), 'pn' : 1}
        member_url = url + urllib.urlencode(query_dict)
        html = urllib2.urlopen(member_url, timeout=MAX_TIME).read()
        soup = BeautifulSoup(html, 'lxml')
        total_page = int(soup.find('span', attrs={'class':'tbui_total_page'}).text[1:-1])
        total_page_pat = re.compile('<span class="tbui_total_page">.*?（\d+）.*?</span>')
        #total_page = int(total_page_pat.search(html).group(1))

        name_pat = re.compile('class="user_name" title="(.*?)"')
        members = name_pat.findall(html)
        for i in range(2, total_page + 1):
            query_dict['pn'] = i
            member_url = url + urllib.urlencode(query_dict)
            html = urllib2.urlopen(member_url, timeout=MAX_TIME).read()
            members += name_pat.findall(html)
        return members

    def getUserDetail(self, uname):
        url = 'https://www.baidu.com/p/' + uname.encode('utf8') + '/detail'
        html = urllib2.urlopen(url, timeout=MAX_TIME).read()
        # get attrs like sexual, birthday
        pat_str = '<dd><span class=profile-attr>(.*?)</span>\s*<span.*?>(.*?)</span>'
        attr_pat = re.compile(pat_str)
        # return <type 'str'>, print directly can see chinese
        # need to .decode('utf8')
        return attr_pat.findall(html)

    def getUidByName(self, uname):
        url = 'http://tieba.baidu.com/home/main?'
        query_dict = {'un' : uname.encode('utf8'), 'ie' : 'utf-8', 'fr' : 'pb'}
        url = url + urllib.urlencode(query_dict)
        # print url
        request = urllib2.Request(url)
        request.add_header('User-Agent','Mozilla/5.0 (Windows NT 6.2; rv:16.0) Gecko/20100101 Firefox/16.0')
        html = urllib2.urlopen(request, timeout=MAX_TIME).read()
        f = open('test.txt', 'w')
        f.write(html)
        f.close()
        pat = re.compile('href="/im/pcmsg\?from=(\d+)" target="sixin"')
        return pat.search(html).group(1)

    # return unicode
    def getTiebaInformation(self, tname):
        url = 'http://tieba.baidu.com/bawu2/platform/detailsInfo?'
        query_dict = {'word' : tname.encode('utf8'), 'ie' : 'utf-8'}
        url = url + urllib.urlencode(query_dict)
        html = urllib2.urlopen(url, timeout=MAX_TIME).read()
        soup = BeautifulSoup(html)
        # get slogan
        information = {}
        information['slogan'] = soup.find('p', attrs = {'class':'card_slogan'}).text
        information['menNum'] = soup.find('span', attrs = {'class':'card_menNum'}).text
        information['infoNum'] = soup.find('span', attrs = {'class':'card_infoNum'}).text
        drl_names = soup.findAll('a', attrs = {'class' : 'drl_item_name_top'}) +\
                    soup.findAll('a', attrs = {'class' : 'drl_item_name_nor'})
        information['drl'] = map(lambda x : x.text, drl_names)
        return information
        
SAVE_PATH = 'tiezi/'

def save(crawler, link, title):
    try:
        comments, information = crawler.getTieziDetails(link)
        str2Time = lambda x : datetime.strptime(x, '%Y-%m-%d %H:%M')
        #floors = sorted(comments.iteritems(),
        #                key=lambda x:str2Time(x[1]['info']['time']))
        floors = sorted(comments.iteritems(),
                        key=lambda x:int(x[1]['info']['floor_idx'][:-1]))
    except:
        with open('failed.txt', 'a+') as f:
            f.write(link + '\n')
        return
    
    with open(SAVE_PATH + information['tid'] + '.txt', 'w') as f:
        f.write(title)
        f.write('\n')
        for pid, floor in floors:
            f.write('[')
            f.write(floor['info']['floor_idx'].encode('utf8', 'ignore'))
            f.write(']')
            f.write(floor['info']['content'].encode('utf8', 'ignore'))
            f.write('\n')
            for lzl in floor['lzl']:
                f.write('\t[')
                f.write(lzl['uname'].encode('utf8', 'ignore'))
                f.write(']:')
                f.write(lzl['content'].encode('utf8', 'ignore'))
                f.write('\n')
            f.write('\n')

if __name__ == '__main__':
    crawler = TiebaCrawler()
    
    '''
    MAX_THREAD = 10
    name = 0
    for i in range(0, len(links), MAX_THREAD):
        batch = links[i:i+MAX_THREAD]
        threads = []
        for link in batch:
            t = threading.Thread(target = save,
                                 args = (crawler,
                                         link,
                                         titleDict[link]))
            t.start()
            threads.append(t)
            name += 1
        for t in threads:
            t.join(60)
    print 'Finish'
    '''
    
