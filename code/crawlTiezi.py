# -*- coding: cp936 -*-
import TiebaCrawler
import sqlite3
import time
import threading
import countReply
import traceback
import os
import Queue

LINK_PATH = '../data/links.txt'

DB_PATH = '../data/tieba.db'

INSERT_TIEZI = "INSERT INTO TIEZI(TID, TITLE, PAGENUM, REPLYNUM, FID, LZ) " +\
               "VALUES(%s, '%s', %s, %s, %s, '%s');"

SELECT_TIEZI = "SELECT TITLE, PAGENUM, REPLYNUM, LZ FROM TIEZI " +\
               "WHERE TID = %s;"

INSERT_COMMENT = u"INSERT INTO COMMENT(PID, UID, UNAME, TIME, FLOOR, CONTENT, TID) " +\
               "VALUES(%s, %s, '%s', '%s', %s, '%s', %s);"

SELECT_COMMENT = "SELECT UNAME, TIME, FLOOR, CONTENT FROM COMMENT " +\
               "WHERE PID = %s;"

INSERT_CCOMMENT = "INSERT INTO CCOMMENT(SPID, UID, UNAME, TIME, CONTENT, PID) " +\
               "VALUES(%s, %s, '%s', '%s', '%s', %s);"

SELECT_CCOMMENT = "SELECT UNAME, TIME, CONTENT FROM CCOMMENT " +\
               "WHERE SPID = %s;"

def hasTiezi(conn, TID):
    sql = SELECT_TIEZI % TID
    cursor = conn.execute(sql)
    for row in cursor:
        return True
    return False

def sqlInsertTiezi(conn, TID, TITLE, PAGENUM, REPLYNUM, FID, LZ):
    sql = SELECT_TIEZI % TID
    cursor = conn.execute(sql)
    for row in cursor:
        print 'same TID'
        return
    sql = INSERT_TIEZI % (int(TID), TITLE.replace("'", "''"), int(PAGENUM),
                          int(REPLYNUM), int(FID), LZ.replace("'", "''"))
    conn.execute(sql)

def sqlInsertComment(conn, PID, UID, UNAME, TIME, FLOOR, CONTENT, TID):
    sql = SELECT_COMMENT % PID
    cursor = conn.execute(sql)
    for row in cursor:
        print 'same PID'
        return
    sql = INSERT_COMMENT % (int(PID), int(UID), UNAME.replace("'", "''"),
                            TIME, int(FLOOR), CONTENT.replace("'", "''"),
                            int(TID))
    conn.execute(sql)

def sqlInsertCComment(conn, SPID, UID, UNAME, TIME, CONTENT, PID):
    sql = SELECT_CCOMMENT % SPID
    cursor = conn.execute(sql)
    for row in cursor:
        print 'same SPID'
        return
    if UID == '':
        UID = 0
    sql = INSERT_CCOMMENT % (int(SPID), int(UID), UNAME.replace("'", "''"),
                             TIME, CONTENT.replace("'", "''"), int(PID))
    conn.execute(sql)

def str2MkTime(time_str):
    time_tuple = time.strptime(time_str, '%Y-%m-%d %H:%M')
    mkTime = str(time.mktime(time_tuple)).split('.')[0]
    return mkTime

def storeTiezi(cmts, tiezi_info):
    # store tiezi info
    sqlInsertTiezi(conn, tiezi_info['tid'], tiezi_info['title'],
                   tiezi_info['page_num'], tiezi_info['reply_num'],
                   tiezi_info['fid'], tiezi_info['lz'])
    print 'stored tiezi infomation'
    # store comment info
    for pid in cmts.keys():
        detail = cmts[pid]
        floor_info = detail['info']
        sqlInsertComment(conn, pid, floor_info['uid'], floor_info['uname'],
                         str2MkTime(floor_info['time']), floor_info['floor_idx'],
                         floor_info['content'], tiezi_info['tid'])
        for lzl in detail['lzl']:
            sqlInsertCComment(conn, lzl['spid'], '', lzl['uname'],
                              lzl['time'], lzl['content'], pid)
    conn.commit()

def storeInTime(link, l):
    comments, information = crawler.getTieziDetails(link.strip())
    conn = sqlite3.connect(DB_PATH)
    storeTiezi(conn, comments, information)
    conn.commit()
    conn.close()
    l.append(1)

def saveLinks(links):
    f = open('links.txt', 'w')
    for link in links:
        f.write(link + '\n')
    f.close()

def crawlOne(conn, link):
    link = link.strip()
    if hasTiezi(conn, link.split('/')[-1]):
        print 'Has existed'
        #return
    crawler = TiebaCrawler.TiebaCrawler()
    comments, information = crawler.getTieziDetails(link.strip())
    storeTiezi(conn, comments, information)
    try:
        countReply.updateReplyTable(conn, comments, information)
    except:
        print 'update reply error, roll back'
        print traceback.print_exc()
        conn.rollback()

def saveLinks(links):
    with open(LINK_PATH, 'w') as f:
        for link in links:
            f.write(link + '\n')
        f.close()

def loadLinks():
    with open(LINK_PATH) as f:
        links = f.readlines()
        links = map(lambda x:x.strip(), links)
        return links

# multiple threading, just for big IO task
class Producer(object):
    
    def __init__(self, max_size = 10):
        self.max_size = max_size
        self.processed = 0

    # threading target function
    def learnSkill(self, func):
        self.func = func
        
    # threading args
    def feedResourses(self, resourses):
        self.resourses = resourses

    def produce(self):
        free_num = self.max_size - threading.active_count()
        while free_num:
            t = threading.Thread(target = self.func,
                                 args = self.resourses[self.processed])
            t.setDaemon(True)
            t.start()
            self.processed += 1
            free_num -= 1
            if self.processed == len(self.resourses):
                print 'Finish!'

class Threaden(object):

    # use a queue to receive the output of the function
    @staticmethod
    def threadenFunc(func):
        # args must be a tuple after threadArgs
        def threadFunc(work_queue, args):
            work_queue.put(func(args))
        return threadFunc

    @staticmethod
    # make the args as a tuple for threading.Thread
    def threadenArgs(q, args):
        return (q, args)

# single consumer having multiple producer
class Consumer(Queue.Queue):

    def learnSkill(self, func):
        self.func = func
        self.fail_count = 0

    def consume(self):
        print 'Begin wait'
        result = self.get(block=True, timeout=30)
        # unpack and execute
        print 'Begin to consume'
        try:
            self.func(*result)
        except:
            self.fail_count += 1
            print 'fail : ' + str(self.fail_count)
        print 'Finish consume'
            

if __name__ == '__main__':
    # initial
    crawler = TiebaCrawler.TiebaCrawler()
    conn = sqlite3.connect(DB_PATH)
    links = None
    if os.path.exists(LINK_PATH):
        links = loadLinks()
    else:
        titleDict = crawler.getTieziLinks(u'柯南', 500)
        links = titleDict.keys()
        print 'Get links!'
        saveLinks(links)

    # pass exists tiezi
    count = 0
    while hasTiezi(conn, links[count].split('/')[-1]):
        count += 1

    consumer = Consumer(10)
    consumer.learnSkill(storeTiezi)
    targetLinks = filter(lambda x :not hasTiezi(conn, x.split('/')[-1]),
                         links[count:])
    threadLinks = [Threaden.threadenArgs(consumer, link)
                   for link in targetLinks]
    
    producer = Producer(max_size = 10)
    producer.learnSkill(Threaden.threadenFunc(crawler.getTieziDetails))
    producer.feedResourses(threadLinks[20:])

    count = 0
    while count < len(targetLinks):
        producer.produce()
        try:
            consumer.consume()
            count += 1
            print 'current : ' + str(count) + '/' + str(len(targetLinks))
            print 'queue size : ' + str(consumer.qsize())
            print 'thread num : ' + str(threading.active_count())
        except Queue.Empty:
            print 'Time out'
            print 'thread num : ' + str(threading.active_count())
