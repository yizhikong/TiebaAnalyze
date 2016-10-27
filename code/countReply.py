# -*- coding: cp936 -*-
import TiebaCrawler
import sqlite3
import time
import re

DB_PATH = '../data/tieba.db'

def replyToWho(content):
    pat = re.compile(u"回复\s*(.*?)\s*(\:|：)")
    result = pat.search(content)
    if result:
        return result.group(1)
    else:
        return ""

def incReply(replyDict, whoReply, toWho):
    try:
        uDict = replyDict[whoReply]
    except:
        replyDict[whoReply] = {}
        uDict = replyDict[whoReply]
    try:
        uDict[toWho] += 1
    except:
        uDict[toWho] = 1

def getReplyListByData(conn, cmts, tiezi_info):
    replyDict = {}
    lz = tiezi_info['lz']
    for pid in cmts.keys():
        uname = cmts[pid]['info']['uname']
        incReply(replyDict, uname, lz)
        for lzl in cmts[pid]['lzl']:
            toWho = replyToWho(lzl['content'])
            if len(toWho) > 0:
                incReply(replyDict, lzl['uname'], toWho)
            else:
                incReply(replyDict, lzl['uname'], lz)
    replyList = []
    for uname in replyDict:
        littleDict = replyDict[uname]
        for k in littleDict:
            replyList.append((uname, k, littleDict[k]))
    return replyList

def getReplyListByDB(conn):
    replyDict = {}
    # add lzl communicate
    sql = u"SELECT UNAME, CONTENT FROM CCOMMENT WHERE CONTENT LIKE '回复%';"
    cursor = conn.execute(sql)
    ccomments = cursor.fetchall()
    for i in range(len(ccomments)):
        uname, content = ccomments[i]
        toWho = replyToWho(content)
        if len(toWho) > 0:
            incReply(replyDict, uname, toWho)
        else:
            print content
        if i % 100 == 0:
            print 'ccoment communicate' + str(i) + '/' + str(len(ccomments))
            
    cursor = conn.execute("SELECT PID, UNAME FROM COMMENT")
    comments = cursor.fetchall()
    for i in range(len(comments)):
        pid, uname = comments[i]
        # get the lzl comments by the pid of floor
        sql = "SELECT UNAME, CONTENT FROM CCOMMENT WHERE PID = " + str(pid)
        ccmts = cursor.execute(sql).fetchall()
        for ccmt in ccmts:
            whoReply, content = ccmt
            if u'回复' != content[:2]:
                incReply(replyDict, whoReply, uname)
        if i % 100 == 0:
            print 'ccoment' + str(i) + '/' + str(len(comments))

    cursor = conn.execute("SELECT TID, LZ FROM TIEZI")
    tiezis = cursor.fetchall()
    for i in range(len(tiezis)):
        tid, uname = tiezis[i]
        sql = "SELECT UNAME FROM COMMENT WHERE TID = " + str(tid)
        whoReplies = cursor.execute(sql).fetchall()
        for whoReply in whoReplies:
            incReply(replyDict, whoReply[0], uname)
        if i % 100 == 0:
            print 'coment' + str(i) + '/' + str(len(tiezis))

    replyList = []
    for uname in replyDict:
        littleDict = replyDict[uname]
        for k in littleDict:
            replyList.append((uname, k, littleDict[k]))

    return replyList
    
def initialReplyTable(conn):
    replyList = getReplyListByDB(conn)
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO REPLY VALUES (?, ?, ?)', replyList)
    conn.commit()

def updateReplyTable(conn, cmts, tiezi_info):
    replyList = getReplyListByData(conn, cmts, tiezi_info)
    for whoReply, toWho, inc in replyList:
        sql = u"SELECT COUNT FROM REPLY WHERE UNAME = '%s' AND REPLYTO = '%s'"
        result = conn.execute(sql%(whoReply, toWho)).fetchall()
        if len(result) > 1:
            print 'ERROR!!!'
        if len(result) == 0:
            sql = u"INSERT INTO REPLY(UNAME, REPLYTO, COUNT) " +\
                  u"VALUES ('%s', '%s', %s)" % (whoReply, toWho, inc)
            conn.execute(sql)
        else:
            count = result[0][0] + inc
            sql = u"UPDATE REPLY SET COUNT = %s " +\
                  u"WHERE UNAME = '%s' AND REPLYTO = '%s'"
            conn.execute(sql % (count, whoReply, toWho))
    conn.commit()

if __name__ == '__main__':
    crawler = TiebaCrawler.TiebaCrawler()
    conn = sqlite3.connect(DB_PATH)
    initialReplyTable(conn)
    conn.commit()
