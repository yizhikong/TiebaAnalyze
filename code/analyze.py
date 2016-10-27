# -*- coding: utf-8 -*-
import networkx as nx
import matplotlib.pyplot as plt
from matplotlib import font_manager
import matplotlib
import sqlite3
import random
import TiebaCrawler
import crawlUserAndTieBa as UserCrawler
import Queue
import re
import numpy as np
import jieba
import jieba.posseg as pseg
import crawlTiezi

PREFIX = '../data/'
RESULT_PREFIX = '../result/'
DICTIONARY_PATH = PREFIX + 'dictionary.txt'
DB_PATH = PREFIX + 'dragonball_tieba.db'
DB2_PATH = PREFIX + 'tieba.db'
HTML_TEMPLATE = PREFIX + 'html.txt'

matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['axes.unicode_minus'] = False

jieba.load_userdict(DICTIONARY_PATH)

# the "content" is something like "回复 yzkk : hello world"
# get "yzkk" here
def replyToWho(content):
    pat = re.compile(u"回复\s*(.*?)\s*(\:|：)")
    result = pat.search(content)
    if result:
        return result.group(1)
    else:
        return ""

# return a dictionary to show the reply information
# key is username(who is replied), value is reply count
def getChatDict(conn, uname):
    sql = "SELECT * FROM REPLY WHERE UNAME = '%s'" % uname
    replyList = conn.execute(sql).fetchall()
    sql = "SELECT * FROM REPLY WHERE REPLYTO = '%s'" % uname
    repliedList = conn.execute(sql).fetchall()
    chatDict = {}
    for uname, replyto, count in replyList:
        chatDict[replyto] = count
    for whoreply, uname, count in repliedList:
        try:
            chatDict[whoreply] += count
        except:
            chatDict[whoreply] = count
    return chatDict

# get ones fans and follows 
def getFansAndFollows(conn, uname):
    crawler = TiebaCrawler.TiebaCrawler()
    if not UserCrawler.hasUser(conn, uname):
        UserCrawler.updateUserInfo(crawler, [uname])
    cursor = conn.execute("SELECT FANSNAME FROM FANS WHERE UNAME = '%s'" % uname)
    fans = map(lambda x:x[0], cursor.fetchall())
    cursor = conn.execute("SELECT FOLLOWNAME FROM FOLLOWS WHERE UNAME = '%s'" % uname)
    follows = map(lambda x:x[0], cursor.fetchall())
    return fans, follows

# select something by sql and render the result using html
# the html will show user name, content, link and floor
def selectAndShow(conn, sql, save_name, filt=True):
    results = conn.execute(sql).fetchall()
    html = ''.join(open(HTML_TEMPLATE).readlines())
    template = u"<div class='one'><p>%s:</p><p class='content'>%s<a href='%s'>[查看]</a>[%s楼]</p></div>"''
    rendens = []
    pat = re.compile('<.*?>')
    for result in results:
        name, content, tid, floor = result
        if filt and len(re.sub(pat, '', content)) < 3:
            continue
        link = 'http://tieba.baidu.com/p/' + str(tid)
        rendens.append(template % (name, content, link, str(floor)))
    html = html % (sql, u''.join(rendens))
    f = open(RESULT_PREFIX + save_name + '.html', 'w')
    f.write(html.encode('utf8', 'ignore'))
    f.close()

# select something by sql and render the result using html
# the html only shows the content
def contentSelectAndShow(conn, sql, save_name, filt=True):
    results = conn.execute(sql).fetchall()
    html = ''.join(open(HTML_TEMPLATE).readlines())
    template = u"<div class='one'><p class='content'>%s</p></div>"''
    rendens = []
    pat = re.compile('<.*?>')
    for result in results:
        content = result[0]
        if filt and len(re.sub(pat, '', content)) < 3:
            continue
        rendens.append(template % content)
    html = html % (sql, u''.join(rendens))
    f = open(RESULT_PREFIX + save_name + '.html', 'w')
    f.write(html.encode('utf8', 'ignore'))
    f.close()

# analyze a tiezi according tid, show the reply net
def analyzeTiezi(conn, tid):
    sql = 'SELECT LZ, TITLE FROM TIEZI WHERE TID = %s' % tid
    result = conn.execute(sql).fetchall()
    # if this tiezi is not in database, crawl it
    if len(result) == 0:
        print 'Crawl new one'
        crawlTiezi.crawlOne(conn, 'http://tieba.baidu.com/p/' + str(tid))
        result = conn.execute(sql).fetchall()
    lz, title = result[0]
    sql = 'SELECT UNAME, CONTENT, FLOOR, PID FROM COMMENT WHERE TID = %s' % tid
    edges = []
    floors = conn.execute(sql).fetchall()
    for uname, content, floor, pid in floors:
        prefix = unicode(floor) + u'L_'
        uname =  prefix + uname
        edges.append((uname, lz, 1))
        sql = 'SELECT UNAME, CONTENT FROM CCOMMENT WHERE PID = %d' % pid
        lzls = conn.execute(sql).fetchall()
        for lzl in lzls:
            replyTo = replyToWho(lzl[1])
            if len(replyTo) > 0:
                edges.append((prefix + lzl[0], prefix + replyTo, 3))
            else:
                edges.append((prefix + lzl[0], uname, 2))
    # use networkx to draw the net
    FG = nx.DiGraph()
    FG.add_weighted_edges_from(edges)
    pos = nx.spring_layout(FG)
    colors, sizes, lz_idx, count = [], [], 0, 0
    for u, d in FG.nodes(data=True):
        if u == lz:
            lz_idx = count
        count += 1
        depth = 1
        try:
            depth = len(nx.shortest_path(FG, source = u, target = lz))
        except:
            print 'delete one'
        colors.append(depth)
        sizes.append(depth)
    # the the max depth and average depth of the net
    max_depth, aver_depth = max(sizes), sum(sizes)/len(sizes)
    step = 700.0 / max_depth
    sizes = map(lambda x:int(700-(x-1)*step), sizes)
    sizes[lz_idx] = 1800
    nx.draw_networkx_nodes(FG, pos, node_color=colors, node_size=sizes, cmap=plt.cm.Blues)
    #nx.draw_networkx_nodes(FG, pos, node_color='w', node_size=700)
    nx.draw_networkx_labels(FG,pos,font_size=8)
    
    for u, v, d in FG.edges(data=True):
        nx.draw_networkx_edges(FG, pos, edgelist=[(u, v)], alpha=1.0/d['weight'])
        
    #plt.xlabel('Max depth : ' + str(max_depth))
    plt.axis('off')
    plt.text(-0.2, -0.2, 'Max depth : ' + str(max_depth))
    plt.text(0.1, -0.2, 'Average depth : ' + str(aver_depth))
    plt.title(title)
    plt.show()

# analyze the topic by tiezi title(topic = most frequent words)
def analyzeTopic(conn):
    sql = "SELECT TITLE FROM TIEZI"
    titles = map(lambda x : x[0], conn.execute(sql).fetchall())
    words = {}
    result = pseg.cut(u"。".join(titles))
    for r, s in result:
        if s != u"nr" or len(r) < 2:
            continue
        if r in words:
            words[r] += 1
        else:
            words[r] = 1
    rank = sorted(words.iteritems(), key = lambda x:x[1], reverse = True)
    names = map(lambda x:x[0], rank[:15])
    counts = map(lambda x:x[1], rank[:15])
    position = np.arange(len(names))
    ax = plt.axes()
    ax.set_xticks(position + 0.4)
    font = font_manager.FontProperties(fname = r'c:\windows\fonts\simsun.ttc', size = 10)
    ax.set_xticklabels(names, fontproperties = font)
    plt.title(u"话题人物")
    plt.bar(position, counts)
    plt.show()

def replyNet(conn, uname):
    fansDict, followsDict = {}, {}
    fans, follows = getFansAndFollows(conn, uname)
    fansSet, followsSet = set(fans), set(follows)
    friendSet = fansSet & followsSet
    chatDict = getChatDict(conn, uname)
    friendsChatDict = {}
    for friend in friendSet:
        friendsChatDict[friend] = getChatDict(conn, friend)

    edges, hasAdd = [], {}
    for key in chatDict:
        edges.append((uname, key, chatDict[key]))
        hasAdd[uname+key] = 1
        hasAdd[key+uname] = 1
    for friend in friendsChatDict:
        fDict = friendsChatDict[friend]
        for key in friendSet:
            if key in fDict and key+friend not in hasAdd:
                edges.append((friend, key, fDict[key]))
                hasAdd[key+friend] = 1
                hasAdd[friend+key] = 1
                
    for edge in edges:
        print edge[0] + u'-----' + edge[1]
                
    FG = nx.Graph()
    FG.add_weighted_edges_from(edges)
    nx.draw(FG, with_labels=True)
    plt.show()

# show the friends net using bfs
def friendNet(conn, uname, max_degree = 3):
    fansDict, followsDict = {}, {}
    fans, follows = getFansAndFollows(conn, uname)
    fansSet, followsSet = set(fans), set(follows)
    friendSet = fansSet & followsSet
    rootFriendSet = friendSet

    edges, hasVisit = [], {uname:1}
    # initial the queue
    queue = Queue.Queue()
    for friend in friendSet:
        queue.put((friend, uname, 1))
        hasVisit[uname] = 1

    while queue.empty() is False:
        # print queue.qsize()
        name, fromWho, degree = queue.get()
        edges.append((name, fromWho, degree))
        if degree >= max_degree or name in hasVisit:
            continue
        fans, follows = getFansAndFollows(conn, name)
        fansSet, followsSet = set(fans), set(follows)
        friendSet = fansSet & followsSet
        for friend in friendSet:
            if friend not in rootFriendSet:
                continue
            if friend in hasVisit:
                continue
            queue.put((friend, name, degree + 1))
            hasVisit[friend] = 1
                
    #for edge in edges:
    #    print edge[0] + u'-----' + edge[1]
    
    FG = nx.Graph()
    FG.add_weighted_edges_from(edges)
    pos = nx.spring_layout(FG)
    nx.draw_networkx_labels(FG,pos,font_size=8)
    colors = []
    for u, d in FG.nodes(data=True):
        if u in rootFriendSet:
            colors.append(1)
        else:
            colors.append(2)
    nx.draw_networkx_nodes(FG, pos, node_color=colors, node_size=700, cmap=plt.cm.Blues)
    #nx.draw_networkx_nodes(FG, pos, node_color='w', node_size=700)
    for u, v, d in FG.edges(data=True):
        nx.draw_networkx_edges(FG, pos, edgelist=[(u, v)], alpha=1.0/d['weight'])
        
    plt.axis('off')
    plt.show()

# get the rank of tiebas according the member number
def tiebaRank(conn, num = 10):
    sql = 'SELECT TNAME, COUNT(*) FROM MEMBER ' +\
          'WHERE UID IN ' +\
          '(SELECT UID FROM COMMENT GROUP BY UID HAVING COUNT(*) > 30) ' +\
          'GROUP BY TNAME ' +\
          'ORDER BY COUNT(*) DESC ' +\
          'LIMIT 0, %s'
    result = conn.execute(sql % num).fetchall()
    tnames = map(lambda x:x[0], result)
    counts = map(lambda x:x[1], result)
    position = np.arange(len(tnames))
    ax = plt.axes()
    ax.set_xticks(position + 0.4)
    font = font_manager.FontProperties(fname = r'c:\windows\fonts\simsun.ttc', size = 10)
    ax.set_xticklabels(tnames, fontproperties = font)
    plt.title(u"回帖数大于30的用户关注的贴吧排行")
    plt.bar(position, counts)
    plt.show()

# something wrong?
def analyze(conn, uname):
    fansDict, followsDict = {}, {}
    fans, follows = getFansAndFollows(conn, uname)
    fansSet, followsSet = set(fans), set(follows)
    friendSet = fansSet & followsSet
    neiborSet = fansSet | followsSet
    if len(friendSet) == 0:
        return len(fansSet), len(friendSet)
    
    # get fans' fans, follows' follows
    for friend in friendSet:
        _fans, _follows = getFansAndFollows(conn, friend)
        fansDict[friend], followsDict[friend] = set(_fans), set(_follows)
        
    # count
    commonFollow = 0.0
    for friend in friendSet:
        commonFollow += len(followsDict[friend] & followsSet)
    commonFollow /= (len(friendSet) * (len(followsSet) - len(friendSet)))

    commonFan = 0.0
    for friend in friendSet:
        commonFan += len(fansDict[friend] & fansSet)
    commonFan /= (len(friendSet) * (len(fansSet) - len(friendSet)))

    return commonFollow, commonFan
    

if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)
    selectAndShow(conn, u"SELECT UNAME, CONTENT, TID, FLOOR FROM COMMENT WHERE CONTENT LIKE '%点<img%>看%'", 'av', False)
    selectAndShow(conn, u"SELECT UNAME, CONTENT, TID, FLOOR FROM COMMENT WHERE CONTENT IN " +\
        "(SELECT CONTENT FROM COMMENT WHERE LENGTH(CONTENT) >= 15 GROUP BY CONTENT HAVING COUNT(*) > 10)", 'water', True)
    #selectAndShow(conn, u"SELECT UNAME, CONTENT, TID, FLOOR FROM COMMENT WHERE CONTENT LIKE '%点<%>看%'", False)
    analyzeTopic(conn)
    analyzeTiezi(conn, 4819705573)
    #tid = conn.execute('SELECT TID FROM TIEZI WHERE REPLYNUM = 30').fetchall()[5]
    #analyzeTiezi(conn, tid)
    tiebaRank(conn, 9)
    friendNet(conn, u"yzkk", 2)
    #names = conn.execute("SELECT UNAME FROM USER WHERE UID IN (SELECT UID FROM MEMBER WHERE LEVEL > 6 AND TNAME = '七龙珠')").fetchall()[:5]
    #result = [analyze(conn, name[0]) for name in names]
    #for i in range(len(names)):
    #    print names[i]
    #    print result[i]
    #print result
    conn.close()
    conn = sqlite3.connect(DB2_PATH)
    #analyzeTopic(conn)
    tiebaRank(conn, 9)
    selectAndShow(conn, u"SELECT UNAME, CONTENT, TID, FLOOR FROM COMMENT WHERE CONTENT LIKE '%点<img%>看%'", 'conan_av', False)
    selectAndShow(conn, u"SELECT UNAME, CONTENT, TID, FLOOR FROM COMMENT WHERE CONTENT IN " +\
        "(SELECT CONTENT FROM COMMENT WHERE LENGTH(CONTENT) >= 15 GROUP BY CONTENT HAVING COUNT(*) > 10)", 'conan_water', True)
    contentSelectAndShow(conn, u"SELECT DISTINCT(CONTENT) FROM COMMENT WHERE CONTENT IN " +\
        "(SELECT CONTENT FROM COMMENT WHERE LENGTH(CONTENT) >= 15 GROUP BY CONTENT HAVING COUNT(*) > 10)", 'conan_distinct_water', True)
    conn.close()